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
from prefect import config
from prefect.core import Edge, Flow, Task
from prefect.engine import signals, Run
from prefect.engine.result import Result
from prefect.engine.result_handlers import ConstantResultHandler
from prefect.engine.runner import ENDRUN, Runner, call_state_handlers
from prefect.engine.state import (
    Cached,
    Cancelled,
    Failed,
    Looped,
    Mapped,
    Paused,
    Pending,
    Resume,
    Retrying,
    Running,
    Scheduled,
    Skipped,
    State,
    Submitted,
    Success,
    TimedOut,
    TriggerFailed,
)
from prefect.engine.task_runner import TaskRunner
from prefect.utilities.collections import flatten_seq
from prefect.utilities.executors import run_with_heartbeat
from prefect.utilities import logging


class TaskRun(Run):
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
        super().__init__(state=state, context=context)

        self.task = task
        self.id = id

        if isinstance(state, Retrying):
            self.state.run_count = self.state.run_count + 1
        else:
            self.state.run_count = self.state.context.get("task_run_count", 1)

        if isinstance(state, Resume):
            context.update(resume=True)

        # TODO: remove this and replace with chris's new code
        if hasattr(self.state, "cached_inputs"):
            if "_loop_count" in (self.state.cached_inputs or {}):  # type: ignore
                loop_context = {
                    "task_loop_count": self.state.cached_inputs.pop(  # type: ignore
                        "_loop_count"
                    )  # type: ignore
                    .to_result()
                    .value,
                    "task_loop_result": self.state.cached_inputs.pop(  # type: ignore
                        "_loop_result"
                    )  # type: ignore
                    .to_result()
                    .value,
                }
                self.context.update(loop_context)

        self.context.update(
            task_run_count=self.state.run_count,
            task_name=self.task.name,
            task_tags=self.task.tags,
            task_slug=self.task.slug,
        )
        self.context.setdefault("checkpointing", config.flows.checkpointing)
        self.context.update(logger=self.task.logger)

    def handlers(self):
        return [self.task_state_handler]

    def task_state_handler(self, old_state: State, new_state: State) -> State:
        """
        A special state handler that the TaskRunner uses to call its flow's state handlers.
        This method is called as part of the base Runner's `handle_state_change()` method.

        Args:
            - old_state (State): the old (previous) state
            - new_state (State): the new (current) state

        Returns:
            - State: the new state
        """
        self.logger.debug(
            "Task '{name}': Handling state change from {old} to {new}".format(
                name=prefect.context.get("task_full_name", self.task.name),
                old=type(old_state).__name__,
                new=type(new_state).__name__,
            )
        )
        for handler in self.task.state_handlers:
            new_state = handler(self.task, old_state, new_state) or new_state

        return new_state

    def __repr__(self) -> str:
        # TODO: opportunity to add more useful info here
        return '<"TaskRun">'
