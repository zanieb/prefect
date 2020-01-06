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


class FlowRun(Run):
    """
    FlowRuns store all state necessary to run a Flow.
    If the provided state is a Submitted state, the state it wraps is extracted.
    Args:
        - flow...
        - id...
        - state (State, optional): starting state for the Flow. Defaults to
            `Pending`
        - task_states (dict, optional): dictionary of task states to begin
            computation with, with keys being Tasks and values their corresponding state
        - parameters (dict, optional): dictionary of any needed Parameter
            values, with keys being strings representing Parameter names and values being
            their corresponding values
        - context (Dict[str, Any], optional): prefect.Context to use for execution
            to use for each Task run
        - task_contexts (Dict[Task, Dict[str, Any]], optional): contexts that will be provided to each task
        - task_runner_cls (TaskRunner, optional): The class used for running
            individual Tasks. Defaults to [TaskRunner](task_runner.html)
        - executor_cls (Executor, optional): The class used for submitting task work functions.
        - state_handlers (Iterable[Callable], optional): A list of state change handlers
            that will be called whenever the flow changes state, providing an
            opportunity to inspect or modify the new state. The handler
            will be passed the flow runner instance, the old (prior) state, and the new
            (current) state, with the following signature:
                `state_handler(fr: FlowRunner, old_state: State, new_state: State) -> Optional[State]`
            If multiple functions are passed, then the `new_state` argument will be the
            result of the previous handler.
        - return_tasks ([Task], optional): list of Tasks to include in the
            final returned Flow state. Defaults to `None`
        - task_runner_state_handlers (Iterable[Callable]): A list of state change
            handlers that will be provided to the task_runner, and called whenever a task changes
            state.
    """

    def __init__(
        self,
        flow: Flow,
        state: Optional[State] = None,
        task_states: Dict[Task, State] = None,
        context: Dict[str, Any] = None,
        task_contexts: Dict[Task, Dict[str, Any]] = None,
        parameters: Dict[str, Any] = None,
        runner_cls: Optional[type] = None,
        task_runner_cls: Optional[type] = None,
        executor_cls: Optional[type] = None,
        state_handlers: Iterable[Callable] = None,
        task_state_handlers: Iterable[Callable] = None,
        return_tasks: Set[Task] = None,
        id: Optional[str] = None,
    ):
        if runner_cls is None:
            runner_cls = prefect.engine.get_default_flow_runner_class()

        super().__init__(
            id=id,
            state=state,
            context=context,
            state_handlers=state_handlers,
            runner_cls=runner_cls,
        )

        self.flow = flow

        # make copies to avoid modifying user inputs
        self.task_states = dict(task_states or {})
        self.task_contexts = dict(task_contexts or {})
        self.parameters = dict(parameters or {})
        self.task_state_handlers = task_state_handlers or []

        if return_tasks is None:
            self.return_tasks = self.flow.tasks

        if task_runner_cls is None:
            task_runner_cls = prefect.engine.get_default_task_runner_class()
        self.task_runner_cls = task_runner_cls

        if executor_cls is None:
            executor_cls = prefect.engine.get_default_executor_class()
        self.executor_cls = executor_cls

        # overwrite context parameters one-by-one
        if self.parameters:
            context_params = self.context.setdefault("parameters", {})
            for param, value in self.parameters.items():
                context_params[param] = value

        self.context.update(flow_name=self.flow.name)
        # TODO: technically throwing scheduled time here is arbitrary and can be argued that it is mixing an agnostic store and business logic
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

        # invoke flow handlers first before the old runner handlers
        self.state_handlers = [self.flow_state_handlers] + self.state_handlers

    @staticmethod
    def flow_state_handlers(flow, old_state: State, new_state: State) -> State:
        """
        Call all flow state handlers.

        Args:
            - flow (Flow): the flow changing state
            - old_state (State): the old (previous) state
            - new_state (State): the new (current) state

        Returns:
            - State: the new state
        """
        for handler in flow.state_handlers:
            new_state = handler(flow, old_state, new_state) or new_state

        return new_state

    # TODO: allow this object to be a facade for nested attributes
    # ... this is a bad example, but you get the idea
    @property
    def result(self):
        return self.state.result

    def __repr__(self) -> str:
        # TODO: opportunity to add more useful info here
        return '<"FlowRun">'
