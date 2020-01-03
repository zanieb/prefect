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

import pendulum

import prefect
from prefect.core import Edge, Flow, Task
from prefect.engine import signals
from prefect.engine.flow_run import FlowRun
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


class FlowRunner(Runner):
    """
    FlowRunners handle the execution of Flows and determine the State of a Flow
    before, during and after the Flow is run.

    In particular, through the FlowRunner you can specify which tasks should be
    the first tasks to run, which tasks should be returned after the Flow is finished,
    and what states each task should be initialized with.

    Args:
        - flow (Flow): the `Flow` to be run
        - task_runner_cls (TaskRunner, optional): The class used for running
            individual Tasks. Defaults to [TaskRunner](task_runner.html)
        - state_handlers (Iterable[Callable], optional): A list of state change handlers
            that will be called whenever the flow changes state, providing an
            opportunity to inspect or modify the new state. The handler
            will be passed the flow runner instance, the old (prior) state, and the new
            (current) state, with the following signature:
                `state_handler(fr: FlowRunner, old_state: State, new_state: State) -> Optional[State]`
            If multiple functions are passed, then the `new_state` argument will be the
            result of the previous handler.

    Note: new FlowRunners are initialized within the call to `Flow.run()` and in general,
    this is the endpoint through which FlowRunners will be interacted with most frequently.

    Example:
    ```python
    @task
    def say_hello():
        print('hello')

    with Flow("My Flow") as f:
        say_hello()

    fr = FlowRunner(run=FlowRun(flow=f))
    run = fr.run()

    run.state
    run.result
    ```
    """

    def __init__(
        self,
        run: FlowRun,
        task_runner_cls: type = None,
        state_handlers: Iterable[Callable] = None,
    ):
        self.context = prefect.context.to_dict()
        self.run_store = run
        if task_runner_cls is None:
            task_runner_cls = prefect.engine.get_default_task_runner_class()
        self.task_runner_cls = task_runner_cls

        super().__init__(state_handlers=state_handlers)

        for handler in self.run_store.handlers():
            self.register_handler(handler)

    def __repr__(self) -> str:
        return "<{}: {}>".format(type(self).__name__, self.run_store.flow.name)

    # TODO: MISSING HEARTBEAT! REQUIRED FOR CLOUD

    def run(
        self,
        return_tasks: Iterable[Task] = None,
        task_runner_state_handlers: Iterable[Callable] = None,
        executor: "prefect.engine.executors.Executor" = None,
    ) -> State:
        """
        The main endpoint for FlowRunners.  Calling this method will perform all
        computations contained within the Flow and return the final state of the Flow.

        Args:
            - return_tasks ([Task], optional): list of Tasks to include in the
                final returned Flow state. Defaults to `None`
            - task_runner_state_handlers (Iterable[Callable], optional): A list of state change
                handlers that will be provided to the task_runner, and called whenever a task changes
                state.
            - executor (Executor, optional): executor to use when performing
                computation; defaults to the executor specified in your prefect configuration

        Returns:
            - State: `State` representing the final post-run state of the `Flow`.

        """

        self.logger.info("Beginning Flow run for '{}'".format(self.run_store.flow.name))

        if executor is None:
            executor = prefect.engine.get_default_executor_class()()

        try:

            with prefect.context(self.run_store.context):
                self.run_store.state = self.check_flow_is_pending_or_running(
                    self.run_store.state
                )
                self.run_store.state = self.check_flow_reached_start_time(
                    self.run_store.state
                )
                self.run_store.state = self.set_flow_to_running(self.run_store.state)
                self.run_store.state = self.get_flow_run_state(
                    return_tasks=return_tasks,
                    task_runner_state_handlers=task_runner_state_handlers,
                    executor=executor,
                )

        except ENDRUN as exc:
            state = exc.state

        except KeyboardInterrupt:
            self.logger.debug("Interrupt signal raised, cancelling Flow run.")
            state = Cancelled(message="Interrupt signal raised, cancelling flow run.")

        # All other exceptions are trapped and turned into Failed states
        except Exception as exc:
            self.logger.exception(
                "Unexpected error while running flow: {}".format(repr(exc))
            )
            if prefect.context.get("raise_on_exception"):
                raise exc
            new_state = Failed(
                message="Unexpected error while running flow: {}".format(repr(exc)),
                result=exc,
            )
            state = self.handle_state_change(state or Pending(), new_state)

        return state

    @Runner.call_state_handlers
    def check_flow_reached_start_time(self, state: State) -> State:
        """
        Checks if the Flow is in a Scheduled state and, if it is, ensures that the scheduled
        time has been reached.

        Args:
            - state (State): the current state of this Flow

        Returns:
            - State: the state of the flow after performing the check

        Raises:
            - ENDRUN: if the flow is Scheduled with a future scheduled time
        """
        if isinstance(state, Scheduled):
            if state.start_time and state.start_time > pendulum.now("utc"):
                self.logger.debug(
                    "Flow '{name}': start_time has not been reached; ending run.".format(
                        name=self.run_store.flow.name
                    )
                )
                raise ENDRUN(state)
        return state

    @Runner.call_state_handlers
    def check_flow_is_pending_or_running(self, state: State) -> State:
        """
        Checks if the flow is in either a Pending state or Running state. Either are valid
        starting points (because we allow simultaneous runs of the same flow run).

        Args:
            - state (State): the current state of this flow

        Returns:
            - State: the state of the flow after running the check

        Raises:
            - ENDRUN: if the flow is not pending or running
        """

        # the flow run is already finished
        if state.is_finished() is True:
            self.logger.info("Flow run has already finished.")
            raise ENDRUN(state)

        # the flow run must be either pending or running (possibly redundant with above)
        elif not (state.is_pending() or state.is_running()):
            self.logger.info("Flow is not ready to run.")
            raise ENDRUN(state)

        return state

    @Runner.call_state_handlers
    def set_flow_to_running(self, state: State) -> State:
        """
        Puts Pending flows in a Running state; leaves Running flows Running.

        Args:
            - state (State): the current state of this flow

        Returns:
            - State: the state of the flow after running the check

        Raises:
            - ENDRUN: if the flow is not pending or running
        """
        if state.is_pending():
            self.logger.info("Starting flow run.")
            return Running(message="Running flow.")
        elif state.is_running():
            return state
        else:
            raise ENDRUN(state)

    @run_with_heartbeat
    @Runner.call_state_handlers
    def get_flow_run_state(
        self,
        return_tasks: Set[Task],
        task_runner_state_handlers: Iterable[Callable],
        executor: "prefect.engine.executors.base.Executor",
    ) -> State:
        """
        Runs the flow.

        Args:
            - state (State): starting state for the Flow. Defaults to
                `Pending`
            - task_states (dict): dictionary of task states to begin
                computation with, with keys being Tasks and values their corresponding state
            - task_contexts (Dict[Task, Dict[str, Any]]): contexts that will be provided to each task
            - return_tasks ([Task], optional): list of Tasks to include in the
                final returned Flow state. Defaults to `None`
            - task_runner_state_handlers (Iterable[Callable]): A list of state change
                handlers that will be provided to the task_runner, and called whenever a task changes
                state.
            - executor (Executor): executor to use when performing
                computation; defaults to the executor provided in your prefect configuration

        Returns:
            - State: `State` representing the final post-run state of the `Flow`.

        """

        if not self.run_store.state.is_running():
            self.logger.info("Flow is not in a Running state.")
            raise ENDRUN(self.run_store.state)

        if return_tasks is None:
            return_tasks = set()
        if set(return_tasks).difference(self.run_store.flow.tasks):
            raise ValueError("Some tasks in return_tasks were not found in the flow.")

        # -- process each task in order

        with executor.start():

            for task in self.run_store.flow.sorted_tasks():

                task_state = self.run_store.task_states.get(task)
                if task_state is None and isinstance(
                    task, prefect.tasks.core.constants.Constant
                ):
                    self.run_store.task_states[task] = task_state = Success(
                        result=task.value
                    )

                # if the state is finished, don't run the task, just use the provided state
                if (
                    isinstance(task_state, State)
                    and task_state.is_finished()
                    and not task_state.is_cached()
                    and not task_state.is_mapped()
                ):
                    continue

                upstream_states = {}  # type: Dict[Edge, Union[State, Iterable]]

                # -- process each edge to the task
                for edge in self.run_store.flow.edges_to(task):
                    upstream_states[edge] = self.run_store.task_states.get(
                        edge.upstream_task, Pending(message="Task state not available.")
                    )

                # augment edges with upstream constants
                for key, val in self.run_store.flow.constants[task].items():
                    edge = Edge(
                        upstream_task=prefect.tasks.core.constants.Constant(val),
                        downstream_task=task,
                        key=key,
                    )
                    upstream_states[edge] = Success(
                        "Auto-generated constant value",
                        result=Result(val, result_handler=ConstantResultHandler(val)),
                    )

                # -- run the task

                with prefect.context(task_full_name=task.name, task_tags=task.tags):
                    self.run_store.task_states[task] = executor.submit(
                        self.run_task,
                        task=task,
                        state=task_state,
                        upstream_states=upstream_states,
                        context=dict(
                            prefect.context,
                            **self.run_store.task_contexts.get(task, {})
                        ),
                        task_runner_state_handlers=task_runner_state_handlers,
                        executor=executor,
                    )

            # ---------------------------------------------
            # Collect results
            # ---------------------------------------------

            # terminal tasks determine if the flow is finished
            terminal_tasks = self.run_store.flow.terminal_tasks()

            # reference tasks determine flow state
            reference_tasks = self.run_store.flow.reference_tasks()

            # wait until all terminal tasks are finished
            final_tasks = terminal_tasks.union(reference_tasks).union(return_tasks)
            final_states = executor.wait(
                {
                    t: self.run_store.task_states.get(
                        t, Pending("Task not evaluated by FlowRunner.")
                    )
                    for t in final_tasks
                }
            )

            # also wait for any children of Mapped tasks to finish, and add them
            # to the dictionary to determine flow state
            all_final_states = final_states.copy()
            for t, s in list(final_states.items()):
                if s.is_mapped():
                    s.map_states = executor.wait(s.map_states)
                    s.result = [ms.result for ms in s.map_states]
                    all_final_states[t] = s.map_states

            assert isinstance(final_states, dict)

        key_states = set(flatten_seq([all_final_states[t] for t in reference_tasks]))
        terminal_states = set(
            flatten_seq([all_final_states[t] for t in terminal_tasks])
        )
        return_states = {t: final_states[t] for t in return_tasks}

        self.run_store.state = self.determine_final_state(
            state=self.run_store.state,
            key_states=key_states,
            return_states=return_states,
            terminal_states=terminal_states,
        )

        return self.run_store.state

    def determine_final_state(
        self,
        state: State,
        key_states: Set[State],
        return_states: Dict[Task, State],
        terminal_states: Set[State],
    ) -> State:
        """
        Implements the logic for determining the final state of the flow run.

        Args:
            - state (State): the current state of the Flow
            - key_states (Set[State]): the states which will determine the success / failure of the flow run
            - return_states (Dict[Task, State]): states to return as results
            - terminal_states (Set[State]): the states of the terminal tasks for this flow

        Returns:
            - State: the final state of the flow run
        """
        # check that the flow is finished
        if not all(s.is_finished() for s in terminal_states):
            self.logger.info("Flow run RUNNING: terminal tasks are incomplete.")
            state.result = return_states

        # check if any key task failed
        elif any(s.is_failed() for s in key_states):
            self.logger.info("Flow run FAILED: some reference tasks failed.")
            state = Failed(message="Some reference tasks failed.", result=return_states)

        # check if all reference tasks succeeded
        elif all(s.is_successful() for s in key_states):
            self.logger.info("Flow run SUCCESS: all reference tasks succeeded")
            state = Success(
                message="All reference tasks succeeded.", result=return_states
            )

        # check for any unanticipated state that is finished but neither success nor failed
        else:
            self.logger.info("Flow run SUCCESS: no reference tasks failed")
            state = Success(message="No reference tasks failed.", result=return_states)

        return state

    def run_task(
        self,
        task: Task,
        state: State,
        upstream_states: Dict[Edge, State],
        context: Dict[str, Any],
        task_runner_state_handlers: Iterable[Callable],
        executor: "prefect.engine.executors.Executor",
    ) -> State:
        """

        Runs a specific task. This method is intended to be called by submitting it to
        an executor.

        Args:
            - task (Task): the task to run
            - state (State): starting state for the Flow. Defaults to
                `Pending`
            - upstream_states (Dict[Edge, State]): dictionary of upstream states
            - context (Dict[str, Any]): a context dictionary for the task run
            - task_runner_state_handlers (Iterable[Callable]): A list of state change
                handlers that will be provided to the task_runner, and called whenever a task changes
                state.
            - executor (Executor): executor to use when performing
                computation; defaults to the executor provided in your prefect configuration

        Returns:
            - State: `State` representing the final post-run state of the `Flow`.

        """
        with prefect.context(self.context):
            default_handler = task.result_handler or self.run_store.flow.result_handler
            task_runner = self.task_runner_cls(
                task=task,
                state_handlers=task_runner_state_handlers,
                result_handler=default_handler,
            )

            # if this task reduces over a mapped state, make sure its children have finished
            for edge, upstream_state in upstream_states.items():

                # if the upstream state is Mapped, wait until its results are all available
                if not edge.mapped and upstream_state.is_mapped():
                    assert isinstance(upstream_state, Mapped)  # mypy assert
                    upstream_state.map_states = executor.wait(upstream_state.map_states)
                    upstream_state.result = [
                        s.result for s in upstream_state.map_states
                    ]

            return task_runner.run(
                state=state,
                upstream_states=upstream_states,
                context=context,
                executor=executor,
            )
