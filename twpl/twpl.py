from sys import platform
from tempfile import NamedTemporaryFile
from datetime import datetime
from glob import iglob
from os import access, W_OK, stat, remove
from contextlib import contextmanager
from filelock import Timeout as FileLockTimeoutError, FileLock


__version__ = "0.0.2"

_ERR_EXPLICIT_NOT_IMPLEMENTED = " ".join((
    "Explicit acquire/release methods are not implemented yet. Use context",
    "managers Twpl(...).exclusive() and Twpl(...).concurrent() instead",
))
_DEFAULT_POLL_MS = 100


def _check_platform_capabilities():
    """Only Linux (Unix?) is supported at the moment. /proc/[0-9]+/fd/* must exist and generate sane values"""
    pass # TODO: something smarter than just checking all active PIDs because that feels excessive


def _fds_exceed(filename, fdcount):
    """Check if the number of open file descriptors for `filename` exceeds `fdcount`"""
    pass


class Twpl():
    """Ties itself to a lockfile and provides exclusive (always singular) and concurrent (multiple in absence of exclusive) locks"""
 
    def __init__(self, filename):
        """Create lock object"""
        _check_platform_capabilities()
        self._filename = filename
 
    def __acquire_exclusive(self, poll_ms):
        raise NotImplementedError(_ERR_EXPLICIT_NOT_IMPLEMENTED)
 
    def __release_exclusive(self):
        raise NotImplementedError(_ERR_EXPLICIT_NOT_IMPLEMENTED)
 
    def __acquire_concurrent(self):
        raise NotImplementedError(_ERR_EXPLICIT_NOT_IMPLEMENTED)
 
    def __release_concurrent(self):
        raise NotImplementedError(_ERR_EXPLICIT_NOT_IMPLEMENTED)
 
    def acquire(self, *, exclusive=None, concurrent=None, poll_ms=None):
        """User interface for explicit acquisition. Must specify `exclusive=True` XOR `concurrent=True`. Context manager methods `.exclusive()` and `.concurrent()` are preferred over this"""
        raise NotImplementedError(_ERR_EXPLICIT_NOT_IMPLEMENTED)
        if exclusive and (not concurrent):
            if poll_ms is None:
                poll_ms = _DEFAULT_POLL_MS
            elif not isinstance(poll_ms, (int, float)):
                msg = "argument `poll_ms` must be a numeric value"
                raise ValueError(f"Twpl(...).acquire(exclusive=True) {msg}")
            return self.__acquire_exclusive(poll_ms)
        elif concurrent and (not exclusive):
            if poll_ms is not None:
                msg = "argument `poll_ms` must be None"
                raise ValueError(f"Twpl(...).acquire(concurrent=True) {msg}")
            return self.__acquire_concurrent()
        else:
            msg = "must be either exclusive or concurrent, not both or neither"
            raise ValueError(f"Twpl(...).acquire() {msg}")
 
    def release(self, *, exclusive=None, concurrent=None):
        """User interface for explicit release. Must specify `exclusive=True` XOR `concurrent=True`. Context manager methods `.exclusive()` and `.concurrent()` are preferred over this"""
        raise NotImplementedError(_ERR_EXPLICIT_NOT_IMPLEMENTED)
 
    @contextmanager
    def exclusive(self, *, poll_ms=_DEFAULT_POLL_MS):
        """Wait for all exclusive AND concurrent locks to release, acquire exclusive file lock, enter context, release this exclusive lock"""
        pass
 
    @contextmanager
    def concurrent(self):
        """Wait for all exclusive locks to release, acquire concurrent file lock, enter context, release this concurrent lock"""
        pass
 
    @contextmanager
    def unconditional(self):
        """Dummy context manager that always allows operation"""
        yield
 
    def purge(self, *, min_age_ms=None):
        """Force remove lockfile if age is above `min_age_ms` regardless of state. Useful for cleaning up stale locks after crashes etc"""
        pass
