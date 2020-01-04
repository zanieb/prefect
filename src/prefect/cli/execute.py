import sys
import collections
import threading
import queue

import click

import prefect
from prefect.client import Client
from prefect.utilities.graphql import EnumValue, with_args
from prefect.utilities import logging
from prefect.utilities.executors import PeriodicMonitoredCall
from prefect.engine.state import State, Cancelled


@click.group(hidden=True)
def execute():
    """
    Execute flow environments.

    \b
    Usage:
        $ prefect execute [OBJECT]

    \b
    Arguments:
        cloud-flow  Execute a cloud flow's environment (during deployment)

    \b
    Examples:
        $ prefect execute cloud-flow

    \b
        $ prefect execute local-flow ~/.prefect/flows/my_flow.prefect
    """
    pass


@execute.command(hidden=True)
def cloud_flow():
    """
    Execute a flow's environment in the context of Prefect Cloud.

    Note: this is a command that runs during Cloud execution of flows and is not meant
    for local use.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    if not flow_run_id:
        click.echo("Not currently executing a flow within a Cloud context.")
        raise Exception("Not currently executing a flow within a Cloud context.")

    runner = MonitoredCloudFlowRunner()
    success = runner.run(flow_run_id)
    if not success:
        sys.exit(1)


QueueItem = collections.namedtuple("QueueItem", "event payload")


class MonitoredCloudFlowRunner:
    def __init__(self):
        self.client = Client()
        self.logger = logging.get_logger(type(self).__name__)
        self.state_thread = None
        self.worker_thread = None
        self.queue = queue.Queue()
        self.successful = False
        self.executor = None

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

    def run(self, flow_run_id):
        self.logger.debug("Starting")
        if not self.check_valid_initial_state(flow_run_id):
            raise RuntimeError("Flow run initial state is invalid. It will not be run!")

        # start a state listener thread, pulling states for this flow run id from cloud.
        # Events are reported back to the main thread (here). Why a separate thread?
        # Among other reasons, when we start doing subscriptions later, it will continue
        # to work with little modification (replacing the periodic caller with a thread)
        self.state_thread = PeriodicMonitoredCall(
            interval=10,
            function=self.stream_flow_run_state_events,
            logger=self.logger,
            flow_run_id=flow_run_id,
        )
        self.state_thread.start(
            name_prefix="PrefectFlowRunState-{}".format(flow_run_id)
        )

        cancel_event = threading.Event()

        def controlled_run():
            try:
                self.execute_flow_run(flow_run_id, cancel_event)
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
                    # TODO: call cancel to flow executor to prevent scheduiling of future work

                    self._request_exit()
                elif item.event == "exit":
                    break
                else:
                    self.logger.warning("Unknown event: {}".format(item))
                    continue
        except Exception:
            self.logger.exception("Unhandled exception in the event loop")

        self._on_exit()
        return self.successful

    def execute_flow_run(self, flow_run_id, cancel_event) -> bool:
        query = {
            "query": {
                with_args("flow_run", {"where": {"id": {"_eq": flow_run_id}}}): {
                    "flow": {"name": True, "storage": True, "environment": True},
                    "version": True,
                }
            }
        }

        result = self.client.graphql(query)

        if not result.data.flow_run:
            raise RuntimeError("Flow run {} not found".format(flow_run_id))

        flow_run = result.data.flow_run[0]

        try:
            storage_schema = prefect.serialization.storage.StorageSchema()
            storage = storage_schema.load(flow_run.flow.storage)

            flow = storage.get_flow(storage.flows[flow_run.flow.name])
            environment = flow.environment
            environment.setup(storage=storage, cancel_event=cancel_event)
            environment.execute(
                storage=storage, flow_location=storage.flows[flow_run.flow.name]
            )

            self.successful = True
        except Exception as exc:
            msg = "Failed to load and execute Flow's environment: {}".format(repr(exc))
            state = prefect.engine.state.Failed(message=msg)
            self.client.set_flow_run_state(
                flow_run_id=flow_run_id, version=flow_run.version, state=state
            )

            raise exc

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
