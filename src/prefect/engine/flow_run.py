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
from prefect.engine.runner import ENDRUN, Runner
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
    """

    def __init__(
        self,
        flow: "Flow",
        state: Optional[State],
        task_states: Dict[Task, State],
        context: Dict[str, Any],
        task_contexts: Dict[Task, Dict[str, Any]],
        parameters: Dict[str, Any],
        task_runner_cls: type = None,
        executor_cls: type = None,
        id: str = None,
    ):
        super().__init__(state=state, context=context)

        self.flow = flow
        self.id = id

        # make copies to avoid modifying user inputs
        self.task_states = dict(task_states or {})
        self.task_contexts = dict(task_contexts or {})
        self.parameters = dict(parameters or {})

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

    def __repr__(self) -> str:
        # TODO: opportunity to add more useful info here
        return '<"FlowRun">'
