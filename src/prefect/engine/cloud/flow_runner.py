import _thread
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
from prefect.engine.state import Failed, State
from prefect.utilities.graphql import with_args


class CloudFlowRunner(FlowRunner):
    """
    Write up that this is the same as the FlowRunner, but monitored...
    """

    def __init__(
        self, run: CloudFlowRun, state_handlers: Iterable[Callable] = None
    ) -> None:
        # TODO: bad: both the runner and run object have a client
        self.client = Client()
        super().__init__(run=run, state_handlers=state_handlers)

    def _heartbeat(self) -> bool:
        self.client.update_flow_run_heartbeat(self.run_state.id)

    # TODO: delete this when task runner is not using this in the base class
    def call_runner_target_handlers(self, old_state: State, new_state: State) -> State:
        return new_state
