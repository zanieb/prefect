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
    TYPE_CHECKING,
)

import pendulum

import prefect
from prefect.core import Edge, Flow, Task
from prefect.engine import signals
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

if TYPE_CHECKING:
    from prefect.engine import FlowRun


class FlowRunner(Runner):
    """
    FlowRunners handle the execution of Flows and determine the State of a Flow
    before, during and after the Flow is run.

    In particular, through the FlowRunner you can specify which tasks should be
    the first tasks to run, which tasks should be returned after the Flow is finished,
    and what states each task should be initialized with.

    Args:
        - run (FlowRun): a `FlowRun` configuration object that has the flow definition and any existing state

    Note: new FlowRunners are initialized within the call to `Flow.run()` and in general,
    this is the endpoint through which FlowRunners will be interacted with most frequently.

    Example:
    ```python
    @task
    def say_hello():
        print('hello')

    with Flow("My Flow") as f:
        say_hello()

    run = FlowRun(flow=f)
    runner = FlowRunner(run=run)
    
    flow_state = runner.run()
    
    run.state
    run.result
    ```
    """

    def __init__(self, run: "FlowRun"):
        super().__init__()
        self.context = prefect.context.to_dict()
        self.run_state = run

    def __repr__(self) -> str:
        return "<{}: {}>".format(type(self).__name__, self.run_state.flow.name)

    # TODO: delete this when task runner is not using this in the base class
    def call_runner_target_handlers(self, old_state: State, new_state: State) -> State:
        return new_state

    # TODO: this will get moved to the base class when the task runner can support these changes...
    def handle_state_change(self, old_state: State, new_state: State) -> State:
        """
        Calls any handlers associated with the Runner

        This method will only be called when the state changes (`old_state is not new_state`)

        Args:
            - old_state (State): the old (previous) state of the task
            - new_state (State): the new (current) state of the task

        Returns:
            State: the updated state of the task

        Raises:
            - PAUSE: if raised by a handler
            - ENDRUN: if raised by a handler
            - ENDRUN(Failed()): if any of the handlers fail unexpectedly

        """
        self.logger.debug(
            "Handling state change from {old} to {new}".format(
                old=type(old_state).__name__, new=type(new_state).__name__,
            )
        )

        raise_on_exception = prefect.context.get("raise_on_exception", False)

        try:
            # call runner's own handlers
            for handler in self.run_state.state_handlers:
                new_state = (
                    handler(self.run_state.flow, old_state, new_state) or new_state
                )

        # raise pauses and ENDRUNs
        except (signals.PAUSE, ENDRUN):
            raise

        # trap signals
        except signals.PrefectStateSignal as exc:
            if raise_on_exception:
                raise
            return exc.state

        # abort on errors
        except Exception as exc:
            if raise_on_exception:
                raise
            msg = "Unexpected error while calling state handlers: {}".format(repr(exc))
            self.logger.exception(msg)
            raise ENDRUN(Failed(msg, result=exc))
        return new_state

    def run(self, executor: "prefect.engine.executors.Executor" = None,) -> State:
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

        self.logger.info("Beginning Flow run for '{}'".format(self.run_state.flow.name))

        if executor is None:
            executor = self.run_state.executor_cls()

        try:
            state = self.run_state.state

            with prefect.context(self.run_state.context):
                state = self.check_flow_is_pending_or_running(state)
                state = self.check_flow_reached_start_time(state)
                state = self.set_flow_to_running(state)
                state = self.get_flow_run_state(state, executor=executor,)

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
        finally:
            self.run_state.state = state

        return self.run_state.state

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
                        name=self.run_state.flow.name
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
        self, state: State, executor: "prefect.engine.executors.base.Executor",
    ) -> State:
        """
        Runs the flow.

        Args:
            - state (State): starting state for the Flow. Defaults to
                `Pending`
            - executor (Executor): executor to use when performing
                computation; defaults to the executor provided in your prefect configuration

        Returns:
            - State: `State` representing the final post-run state of the `Flow`.

        """

        if not state.is_running():
            self.logger.info("Flow is not in a Running state.")
            raise ENDRUN(state)

        if set(self.run_state.return_tasks).difference(self.run_state.flow.tasks):
            raise ValueError("Some tasks in return_tasks were not found in the flow.")

        # -- process each task in order

        with executor.start():

            for task in self.run_state.flow.sorted_tasks():

                task_state = self.run_state.task_states.get(task)
                if task_state is None and isinstance(
                    task, prefect.tasks.core.constants.Constant
                ):
                    self.run_state.task_states[task] = task_state = Success(
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
                for edge in self.run_state.flow.edges_to(task):
                    upstream_states[edge] = self.run_state.task_states.get(
                        edge.upstream_task, Pending(message="Task state not available.")
                    )

                # augment edges with upstream constants
                for key, val in self.run_state.flow.constants[task].items():
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
                    self.run_state.task_states[task] = executor.submit(
                        self.run_task,
                        task=task,
                        state=task_state,
                        upstream_states=upstream_states,
                        context=dict(
                            prefect.context,
                            **self.run_state.task_contexts.get(task, {})
                        ),
                        task_runner_state_handlers=self.run_state.task_state_handlers,
                        executor=executor,
                    )

            # ---------------------------------------------
            # Collect results
            # ---------------------------------------------

            # terminal tasks determine if the flow is finished
            terminal_tasks = self.run_state.flow.terminal_tasks()

            # reference tasks determine flow state
            reference_tasks = self.run_state.flow.reference_tasks()

            # wait until all terminal tasks are finished
            final_tasks = terminal_tasks.union(reference_tasks).union(
                set(self.run_state.return_tasks)
            )
            final_states = executor.wait(
                {
                    t: self.run_state.task_states.get(
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
        return_states = {t: final_states[t] for t in self.run_state.return_tasks}

        state = self.determine_final_state(
            state=state,
            key_states=key_states,
            return_states=return_states,
            terminal_states=terminal_states,
        )

        return state

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
            default_handler = task.result_handler or self.run_state.flow.result_handler
            task_runner = self.run_state.task_runner_cls(
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
