import collections
import functools
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

import prefect
from prefect.engine import signals
from prefect.engine.state import Failed, Pending, State
from prefect.utilities import logging


class Run:
    def __init__(
        self, state: Optional[State], context: Dict[str, Any],
    ):
        self.logger = logging.get_logger(type(self).__name__)

        # extract possibly nested meta states -> for example a Submitted( Queued( Retry ) )
        while isinstance(state, State) and state.is_meta_state():
            state = state.state  # type: ignore

        self.state = state or Pending()
        self.context = dict(context or {})

    def __repr__(self) -> str:
        # TODO: opportunity to add more useful info here
        return '<"Run">'
