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
from prefect.core import Edge, Flow, Task
from prefect.engine import signals, Run
from prefect.engine.result import Result
from prefect.engine.result_handlers import ConstantResultHandler
from prefect.engine.runner import ENDRUN, Runner, call_state_handlers
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


class FlowRun(Run):
    def __init__(
        self,
        flow: "Flow",
        state: Optional[State],
        task_states: Dict[Task, State],
        context: Dict[str, Any],
        task_contexts: Dict[Task, Dict[str, Any]],
        parameters: Dict[str, Any],
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

        self.flow = flow
        self.id = id

        # make copies to avoid modifying user inputs
        self.task_states = dict(task_states or {})
        self.task_contexts = dict(task_contexts or {})
        self.parameters = dict(parameters or {})

        # overwrite context parameters one-by-one
        if self.parameters:
            context_params = self.context.setdefault("parameters", {})
            for param, value in self.parameters.items():
                context_params[param] = value

        self.context.update(flow_name=self.flow.name)
        self.context.setdefault("scheduled_start_time", pendulum.now("utc"))

        # add various formatted dates to context
        now = pendulum.now("utc")
        dates = {
            "date": now,
            "today": now.strftime("%Y-%m-%d"),
            "yesterday": now.add(days=-1).strftime("%Y-%m-%d"),
            "tomorrow": now.add(days=1).strftime("%Y-%m-%d"),
            "today_nodash": now.strftime("%Y%m%d"),
            "yesterday_nodash": now.add(days=-1).strftime("%Y%m%d"),
            "tomorrow_nodash": now.add(days=1).strftime("%Y%m%d"),
        }
        for key, val in dates.items():
            self.context.setdefault(key, val)

        for task in self.flow.tasks:
            self.task_contexts.setdefault(task, {}).update(
                task_name=task.name, task_slug=task.slug
            )

    # TODO: allow this object to be a facade for nested attributes
    # ... this is a bad example, but you get the idea
    @property
    def result(self):
        return self.state.result

    def handlers(self):
        return [self.flow_state_handler]

    def flow_state_handler(self, old_state: State, new_state: State) -> State:
        """
        A special state handler that the FlowRunner uses to call its flow's state handlers.
        This method is called as part of the base Runner's `handle_state_change()` method.

        Args:
            - old_state (State): the old (previous) state
            - new_state (State): the new (current) state

        Returns:
            - State: the new state
        """
        self.logger.debug(
            "Flow '{name}': Handling state change from {old} to {new}".format(
                name=self.flow.name,
                old=type(old_state).__name__,
                new=type(new_state).__name__,
            )
        )
        for handler in self.flow.state_handlers:
            new_state = handler(self.flow, old_state, new_state) or new_state

        return new_state

    def __repr__(self) -> str:
        # TODO: opportunity to add more useful info here
        return '<"FlowRun">'
