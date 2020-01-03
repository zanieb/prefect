import copy
import datetime
import _thread
import time
import warnings
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

import pendulum

import prefect
from prefect.client import Client
from prefect.core import Edge, Task
from prefect.utilities.executors import tail_recursive
from prefect.engine.cloud.utilities import prepare_state_for_cloud
from prefect.engine.result import NoResult, Result
from prefect.engine.result_handlers import ResultHandler
from prefect.engine.runner import ENDRUN
from prefect.engine.task_run import TaskRun
from prefect.engine.state import (
    Cached,
    ClientFailed,
    Failed,
    Mapped,
    Retrying,
    Queued,
    State,
)
from prefect.engine.task_runner import TaskRunner, TaskRunnerInitializeResult
from prefect.utilities.graphql import with_args


class CloudTaskRun(TaskRun):
    def __init__(
        self,
        task: Task,
        state: Optional[State],
        context: Dict[str, Any],
        id: str = None,
    ):
        """
        Initializes all state necessary to run a Flow.

        If the provided state is a Submitted state, the state it wraps is extracted.

        Args:
            - state (Optional[State]): the initial state of the run
            - task_states (Dict[Task, State]): a dictionary of any initial task states
            - context (Dict[str, Any], optional): prefect.Context to use for execution
                to use for each Task run
            - task_contexts (Dict[Task, Dict[str, Any]], optional): contexts that will be provided to each task
            - parameters(dict): the parameter values for the run

        Returns:
            - NamedTuple: a tuple of initialized objects:
                `(state, task_states, context, task_contexts)`
        """

        self.client = Client()

        # if the map_index is not None, this is a dynamic task and we need to load
        # task run info for it
        map_index = context.get("map_index")
        if map_index not in [-1, None]:
            try:
                task_run_info = self.client.get_task_run_info(
                    flow_run_id=context.get("flow_run_id", ""),
                    task_id=context.get("task_id", ""),
                    map_index=map_index,
                )

                # if state was provided, keep it; otherwise use the one from db
                state = state or task_run_info.state  # type: ignore
                context.update(
                    task_run_id=task_run_info.id,  # type: ignore
                    task_run_version=task_run_info.version,  # type: ignore
                )
            except Exception as exc:
                self.logger.exception(
                    "Failed to retrieve task state with error: {}".format(repr(exc))
                )
                if state is None:
                    state = Failed(
                        message="Could not retrieve state from Prefect Cloud",
                        result=exc,
                    )
                raise ENDRUN(state=state)

        # we assign this so it can be shared with heartbeat thread
        id = context.get("task_run_id") or id  # type: ignore
        context.update(checkpointing=True)

        super().__init__(task, state=state, context=context, id=id)

    def handlers(self):
        return [self.cloud_task_state_handler]

    def cloud_task_state_handler(self, old_state: State, new_state: State) -> State:
        """
        A special state handler that the TaskRunner uses to call its flow's state handlers.
        This method is called as part of the base Runner's `handle_state_change()` method.

        Args:
            - old_state (State): the old (previous) state
            - new_state (State): the new (current) state

        Returns:
            - State: the new state
        """
        raise_on_exception = prefect.context.get("raise_on_exception", False)

        try:
            new_state = super().task_state_handler(
                old_state=old_state, new_state=new_state
            )
        except Exception as exc:
            msg = "Exception raised while calling state handlers: {}".format(repr(exc))
            self.logger.exception(msg)
            if raise_on_exception:
                raise exc
            new_state = Failed(msg, result=exc)

        task_run_id = prefect.context.get("task_run_id")
        version = prefect.context.get("task_run_version")

        try:
            cloud_state = prepare_state_for_cloud(new_state)
            self.state = self.client.set_task_run_state(
                task_run_id=task_run_id,
                version=version,
                state=cloud_state,
                cache_for=self.task.cache_for,
            )
        except Exception as exc:
            self.logger.exception(
                "Failed to set task state with error: {}".format(repr(exc))
            )
            raise ENDRUN(state=ClientFailed(state=new_state))

        if self.state.is_queued():
            self.state.state = old_state  # type: ignore
            raise ENDRUN(state=self.state)

        if version is not None:
            prefect.context.update(task_run_version=version + 1)  # type: ignore

        return new_state

    def __repr__(self) -> str:
        # TODO: opportunity to add more useful info here
        return '<"TaskRun">'
