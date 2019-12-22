"""
Result Handlers provide the hooks that Prefect uses to store task results in production; a `ResultHandler` can be provided to a `Flow` at creation.

Anytime a task needs its output or inputs stored, a result handler is used to determine where this data should be stored (and how it can be retrieved).
"""
import os
import tempfile
from typing import Any

import cloudpickle

from prefect.engine.result_handlers import ResultHandler
from prefect.utilities.exceptions import PrefectError


class LocalResultHandler(ResultHandler):
    """
    Hook for storing and retrieving task results from local file storage. Only intended to be used
    for local testing and development. Task results are written using `cloudpickle` and stored in the
    provided location for use in future runs.

    **NOTE**: Stored results will _not_ be automatically cleaned up after execution.

    Args:
        - dir (str, optional): the path to a directory for storing all results; defaults to `.prefect/`;
            Note: if the directory does not exist, it will be created.
    """

    def __init__(self, dir: str = ".prefect"):
        self.dir = dir
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)
        super().__init__()

    def _full_path(self, key: str) -> str:
        return os.path.join(self.dir, key)

    def read(self, key: str) -> Any:
        """
        Read a result from the given file location.

        Args:
            - key (str): the path _relative_ handler path where the result is written

        Returns:
            - the read result from the provided file
        """
        if os.path.isabs(key):
            raise PrefectError(
                "Relative path is required (given asbolute): {}".format(key)
            )

        full_path = self._full_path(key)

        self.logger.debug("Starting to read result from {}...".format(full_path))
        with open(full_path, "rb") as f:
            val = cloudpickle.loads(f.read())
        self.logger.debug("Finished reading result from {}...".format(full_path))

        return val

    def write(self, key: str, result: Any) -> str:
        """
        Serialize the provided result to local disk.

        Args:
            - result (Any): the result to write and store

        Returns:
            - str: the _absolute_ path to the written result on disk
        """
        if os.path.isabs(key):
            raise PrefectError(
                "Relative path is required (given asbolute): {}".format(key)
            )

        full_path = self._full_path(key)

        self.logger.debug("Starting to write result to {}...".format(full_path))
        with open(full_path, "wb") as f:
            f.write(cloudpickle.dumps(result))
        self.logger.debug("Finished writing result to {}...".format(full_path))

        return os.path.abspath(full_path)
