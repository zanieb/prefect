import collections
import functools
import uuid
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, TYPE_CHECKING

import prefect
from prefect.engine import signals
from prefect.engine.state import Failed, Pending, State
from prefect.utilities import logging

if TYPE_CHECKING:
    from prefect.engine.runner import Runner


class Run:
    def __init__(
        self,
        state: Optional[State],
        context: Optional[Dict[str, Any]],
        runner_cls: type,
        state_handlers: Iterable[Callable] = None,
        id: Optional[str] = None,
    ):
        self.logger = logging.get_logger(type(self).__name__)

        # extract possibly nested meta states -> for example a Submitted( Queued( Retry ) )
        while isinstance(state, State) and state.is_meta_state():
            state = state.state  # type: ignore

        self.state = state or Pending()
        self.context = dict(context or {})
        self.state_handlers = state_handlers or []
        self.id = id or str(uuid.uuid4())
        self.runner_cls = runner_cls

    def __repr__(self) -> str:
        state = None
        if self.state:
            state = self.state.__class__.__name__
        return '<"{}" id={} runner={} state={}>'.format(
            self.__class__.__name__, self.id, self.runner_cls.__name__, str(state)
        )
