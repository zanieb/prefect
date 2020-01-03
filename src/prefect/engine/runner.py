import collections
import functools
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

import prefect
from prefect.engine import signals
from prefect.engine.state import Failed, Pending, State
from prefect.utilities import logging

# for backwards compatibility
ENDRUN = signals.ENDRUN


class Runner:
    def __init__(self, state_handlers: Iterable[Callable] = None):
        if state_handlers is not None and not isinstance(
            state_handlers, collections.Sequence
        ):
            raise TypeError("state_handlers should be iterable.")
        self.state_handlers = state_handlers or []
        self.logger = logging.get_logger(type(self).__name__)

    def __repr__(self) -> str:
        return '<"Runner">'

    def _heartbeat(self) -> bool:
        return False

    def register_handler(self, handler: Callable):
        self.state_handlers.append(handler)

    @classmethod
    def call_state_handlers(cls, method: Callable[..., State]) -> Callable[..., State]:
        """
        Decorator that calls the Runner's `handle_state_change()` method.

        If used on a Runner method that has the signature:
            method(self, state: State, *args, **kwargs) -> State
        this decorator will inspect the provided State and the returned State and call
        the Runner's `handle_state_change()` method if they are different.

        For example:

        ```python
        @call_state_handlers
        def check_if_task_is_pending(self, state: State):
            if not state.is_pending()
                return Failed()
            return state
        ```

        Args:
            - method (Callable): a Runner method with the signature:
                method(self, state: State, *args, **kwargs) -> State

        Returns:
            Callable: a decorated method that calls Runner.handle_state_change() if the
                state it returns is different than the state it was passed.
        """

        @functools.wraps(method)
        def inner(self: "Runner", state: State, *args: Any, **kwargs: Any) -> State:
            raise_end_run = False
            raise_on_exception = prefect.context.get("raise_on_exception", False)

            try:
                new_state = method(self, state, *args, **kwargs)
            except ENDRUN as exc:
                raise_end_run = True
                new_state = exc.state

            # PrefectStateSignals are trapped and turned into States
            except signals.PrefectStateSignal as exc:
                self.logger.debug(
                    "{name} signal raised: {rep}".format(
                        name=type(exc).__name__, rep=repr(exc)
                    )
                )
                if raise_on_exception:
                    raise exc
                new_state = exc.state

            except Exception as exc:
                formatted = "Unexpected error: {}".format(repr(exc))
                self.logger.exception(formatted)
                if raise_on_exception:
                    raise exc
                new_state = Failed(formatted, result=exc)

            if new_state is not state:
                new_state = self.handle_state_change(
                    old_state=state, new_state=new_state
                )

            # if an ENDRUN was raised, reraise so it can be trapped
            if raise_end_run:
                raise ENDRUN(new_state)

            return new_state

        return inner

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
        raise_on_exception = prefect.context.get("raise_on_exception", False)

        try:
            # call runner's own handlers
            for handler in self.state_handlers:
                new_state = handler(self, old_state, new_state) or new_state

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
