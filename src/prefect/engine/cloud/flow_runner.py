import queue
import threading
import collections
import warnings
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import prefect
from prefect.client import Client
from prefect.engine.cloud import CloudFlowRun
from prefect.core import Flow, Task
from prefect.engine.cloud import CloudTaskRunner
from prefect.engine.cloud.utilities import prepare_state_for_cloud
from prefect.engine.flow_runner import FlowRunner, FlowRunnerInitializeResult
from prefect.engine.runner import ENDRUN
from prefect.engine.state import Failed, State, Cancelled
from prefect.utilities.executors import PeriodicMonitoredCall
from prefect.utilities.graphql import with_args

QueueItem = collections.namedtuple("QueueItem", "event payload")


class CloudFlowRunner(FlowRunner):
    """
    Write up that this is the same as the FlowRunner, but monitored...
    """

    def __init__(
        self, run: CloudFlowRun, state_handlers: Iterable[Callable] = None
    ) -> None:
        # TODO: bad: both the runner and run object have a client
        self.client = Client()
        self.state_thread = None
        self.worker_thread = None
        self.queue = queue.Queue()
        self.executor = None
        super().__init__(run=run, state_handlers=state_handlers)

    def _heartbeat(self) -> bool:
        self.client.update_flow_run_heartbeat(self.run_state.id)

    # TODO: delete this when task runner is not using this in the base class
    def call_runner_target_handlers(self, old_state: State, new_state: State) -> State:
        return new_state

    def _request_exit(self):
        self.queue.put(QueueItem(event="exit", payload=None))

    def _on_exit(self):
        was_running = self.state_thread.cancel()
        if not was_running:
            self.logger.warning("State thread already stopped before cancellation.")

        self.worker_thread.join()
        self.logger.debug("Exiting")

    def check_valid_initial_state(self, flow_run_id):
        state = self.fetch_current_flow_run_state(flow_run_id)
        return state != Cancelled

    # TODO: consider moving this to the base class
    def cancel(self, wait=True) -> List[Any]:
        if self.executor:
            return self.executor.shutdown(wait=wait)
        raise RuntimeError("Flow is not running, thus cannot be cancelled")

    def run(
        self,
        return_tasks: Iterable[Task] = None,
        task_runner_state_handlers: Iterable[Callable] = None,
        executor: "prefect.engine.executors.Executor" = None,
    ) -> State:
        self.logger.debug("Starting")
        if not self.check_valid_initial_state(self.run_state.id):
            raise RuntimeError("Flow run initial state is invalid. It will not be run!")

        # start a state listener thread, pulling states for this flow run id from cloud.
        # Events are reported back to the main thread (here). Why a separate thread?
        # Among other reasons, when we start doing subscriptions later, it will continue
        # to work with little modification (replacing the periodic caller with a thread)
        self.state_thread = PeriodicMonitoredCall(
            interval=10,
            function=self.stream_flow_run_state_events,
            logger=self.logger,
            flow_run_id=self.run_state.id,
        )
        self.state_thread.start(
            name_prefix="PrefectFlowRunState-{}".format(self.run_state.id)
        )

        def controlled_run():
            try:
                if executor is None:
                    executor = self.run_state.executor_cls()
                self.executor = executor

                # TODO: return state back to main thread
                state = super().run(
                    return_tasks=return_tasks,
                    task_runner_state_handlers=task_runner_state_handlers,
                    executor=executor,
                )
            except Exception:
                self.logger.exception("Error occured on run")

            self.logger.debug("Flowrunner completed")
            self._request_exit()

        # note: this creates a cloud flow runner which has a heartbeat
        self.worker_thread = threading.Thread(target=controlled_run)
        self.worker_thread.start()

        # handle all flow state events of interest as well as exit requests
        try:
            while True:
                item = self.queue.get()
                if item is None:
                    break
                elif not isinstance(item, QueueItem):
                    self.logger.warning("Bad event: {}".format(repr(item)))
                    continue

                if item.event == "state" and item.payload == Cancelled:
                    self.cancel()
                    self._request_exit()
                elif item.event == "exit":
                    break
                else:
                    self.logger.warning("Unknown event: {}".format(item))
                    continue
        except Exception:
            self.logger.exception("Unhandled exception in the event loop")

        self._on_exit()
        # TODO: return result state
        return  # ...

    def fetch_current_flow_run_state(self, flow_run_id):
        query = {
            "query": {
                with_args("flow_run_by_pk", {"id": flow_run_id}): {
                    "state": True,
                    "flow": {"settings": True},
                }
            }
        }

        flow_run = self.client.graphql(query).data.flow_run_by_pk
        return State.parse(flow_run.state)

    def stream_flow_run_state_events(self, flow_run_id):
        state = self.fetch_current_flow_run_state(flow_run_id)

        # note: currently we are polling the latest known state. In the future when subscriptions are
        # available we can stream all state transistions, since we are guarenteed to have ordering
        # without duplicates. Until then, we will apply filtering of the states we want to see before
        # it hits the queue here instead of the main thread.

        if state == Cancelled:
            self.queue.put(QueueItem(event="state", payload=state))
