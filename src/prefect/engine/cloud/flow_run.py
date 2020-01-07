from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Union,
)
import collections

import pendulum

import prefect
from prefect.client import Client
from prefect.engine.cloud import CloudTaskRunner
from prefect.core import Edge, Flow, Task
from prefect.engine import signals
from prefect.engine.result import Result
from prefect.engine.result_handlers import ConstantResultHandler
from prefect.engine.runner import ENDRUN, Runner
from prefect.engine.run import Run
from prefect.engine.state import (
    Cancelled,
    Failed,
    Mapped,
    Pending,
    Retrying,
    Running,
    Scheduled,
    State,
    Success,
)
from prefect.engine.task_runner import TaskRunner
from prefect.utilities.collections import flatten_seq
from prefect.utilities.executors import run_with_heartbeat
from prefect.utilities import logging
from prefect.engine.flow_run import FlowRun
from prefect.engine.cloud.utilities import prepare_state_for_cloud


class CloudFlowRun(FlowRun):
    """
    Initializes all state necessary to run a CloudFlow.

    If the provided state is a Submitted state, the state it wraps is extracted.

    Args:
        - flow...
        - id...
        - state (Optional[State]): the initial state of the run
        - task_states (Dict[Task, State]): a dictionary of any initial task states
        - context (Dict[str, Any], optional): prefect.Context to use for execution
            to use for each Task run
        - task_contexts (Dict[Task, Dict[str, Any]], optional): contexts that will be provided to each task
        - parameters(dict): the parameter values for the run
        - task_runner_cls (TaskRunner, optional): The class used for running
            individual Tasks. Defaults to [TaskRunner](task_runner.html)
        - executor_cls (Executor, optional): The class used for submitting task work functions.
    """

    def __init__(
        self,
        flow: "Flow",
        state: Optional[State] = None,
        task_states: Dict[Task, State] = None,
        context: Dict[str, Any] = None,
        task_contexts: Dict[Task, Dict[str, Any]] = None,
        parameters: Dict[str, Any] = None,
        task_runner_cls: type = CloudTaskRunner,
        id: Optional[str] = None,
        **kwargs: Any,
    ):

        # TODO: bad: both the runner and run object have a client
        self.client = Client()

        # load id from context
        id = id or prefect.context.get("flow_run_id")

        try:
            flow_run_info = self.client.get_flow_run_info(id)
        except Exception as exc:
            self.logger.debug(
                "Failed to retrieve flow state with error: {}".format(repr(exc))
            )
            if state is None:
                state = Failed(
                    message="Could not retrieve state from Prefect Cloud", result=exc
                )
            raise ENDRUN(state=state)

        updated_context = context or {}
        updated_context.update(flow_run_info.context or {})
        updated_context.update(
            flow_id=flow_run_info.flow_id,
            flow_run_id=flow_run_info.id,
            flow_run_version=flow_run_info.version,
            flow_run_name=flow_run_info.name,
            scheduled_start_time=flow_run_info.scheduled_start_time,
        )

        updated_task_contexts = task_contexts or {}
        updated_task_states = task_states or {}
        tasks = {t.slug: t for t in self.flow.tasks}
        # update task states and contexts
        for task_run in flow_run_info.task_runs:
            task = tasks[task_run.task_slug]
            updated_task_states.setdefault(task, task_run.state)
            updated_task_contexts.setdefault(task, {}).update(
                task_id=task_run.task_id,
                task_run_id=task_run.id,
                task_run_version=task_run.version,
            )

        # if state is set, keep it; otherwise load from Cloud
        state = state or flow_run_info.state  # type: ignore

        # update parameters, prioritizing kwarg-provided params
        updated_parameters = flow_run_info.parameters or {}  # type: ignore
        updated_parameters.update(parameters or {})

        super().__init__(
            flow=flow,
            state=state,
            task_states=updated_task_states,
            context=updated_context,
            task_contexts=updated_task_contexts,
            parameters=updated_parameters,
            id=id,
            **kwargs,
        )

    def flow_state_handlers(
        self, flow: Flow, old_state: State, new_state: State
    ) -> State:
        """
        Call all flow state handlers.

        Args:
            - old_state (State): the old (previous) state
            - new_state (State): the new (current) state

        Returns:
            - State: the new state
        """
        raise_on_exception = prefect.context.get("raise_on_exception", False)

        try:
            new_state = super().flow_state_handlers(
                flow=flow, old_state=old_state, new_state=new_state
            )
        except Exception as exc:
            msg = "Exception raised while calling state handlers: {}".format(repr(exc))
            self.logger.debug(msg)
            if raise_on_exception:
                raise exc
            new_state = Failed(msg, result=exc)

        version = prefect.context.get("flow_run_version")

        try:
            cloud_state = prepare_state_for_cloud(new_state)
            self.client.set_flow_run_state(
                flow_run_id=self.id, version=version, state=cloud_state
            )
        except Exception as exc:
            self.logger.debug(
                "Failed to set flow state with error: {}".format(repr(exc))
            )
            raise ENDRUN(state=new_state)

        prefect.context.update(flow_run_version=version + 1)

        return new_state
