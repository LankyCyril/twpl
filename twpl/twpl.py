from sys import platform
from os import path, access, W_OK, stat, remove
from glob import iglob
from tempfile import NamedTemporaryFile
from datetime import datetime
from contextlib import contextmanager
from filelock import Timeout as FileLockTimeoutError, FileLock


__version__ = "0.0.2"

_ERR_PROC_TEST = "test poll of /proc returned an unexpected value"
_ERR_PLATFORM_TEST = "/proc not available and/or not a Linux/POSIX system"
_ERR_EXPLICIT_NOT_IMPLEMENTED = " ".join((
    "explicit acquire/release methods are not implemented yet; use context",
    "managers Twpl(...).exclusive() and Twpl(...).concurrent() instead",
))
_DEFAULT_POLL_MS = 100


def fds_exceed(filename, mincount):
    """Check if number of open file descriptors for `filename` exceeds `mincount`"""
    global fds_exceed
    # fds_exceed bootstraps itself on first call; this avoids, on the one hand,
    # checking on import and having to raise exceptions right away if something
    # is wrong, and on the other, checking every time a Twpl object is created.
    if not getattr(fds_exceed, "_bootstrapped", None):
        if platform.startswith("linux") and path.isdir("/proc"):
            def _fds_exceed(filename, mincount):
                """Actual function is defined here"""
                realpath, n = path.realpath(filename), 0
                for fd in iglob("/proc/[0-9]*/fd/*"):
                    if path.realpath(fd) == realpath:
                        n += 1
                        if n > mincount:
                            return True
                else:
                    return False
            with NamedTemporaryFile(mode="w") as tf: # quick self-test
                with open(tf.name):
                    if (not _fds_exceed(tf.name, 1)) or _fds_exceed(tf.name, 2):
                        raise OSError(f"twpl: {_ERR_PROC_TEST}")
                    else:
                        _fds_exceed.__doc__ = fds_exceed.__doc__
                        fds_exceed = _fds_exceed
                        fds_exceed._bootstrapped = True
                        return _fds_exceed(filename, mincount)
        else:
            raise OSError(f"twpl: {_ERR_PLATFORM_TEST}")


class Twpl():
    """Ties itself to a lockfile and provides exclusive (always singular) and concurrent (multiple in absence of exclusive) locks"""
 
    def __init__(self, filename):
        """Create lock object"""
        self._filename = filename
 
    def __acquire_exclusive(self, poll_ms):
        raise NotImplementedError(f"twpl: {_ERR_EXPLICIT_NOT_IMPLEMENTED}")
 
    def __release_exclusive(self):
        raise NotImplementedError(f"twpl: {_ERR_EXPLICIT_NOT_IMPLEMENTED}")
 
    def __acquire_concurrent(self):
        raise NotImplementedError(f"twpl: {_ERR_EXPLICIT_NOT_IMPLEMENTED}")
 
    def __release_concurrent(self):
        raise NotImplementedError(f"twpl: {_ERR_EXPLICIT_NOT_IMPLEMENTED}")
 
    def acquire(self, *, exclusive=None, concurrent=None, poll_ms=None):
        """User interface for explicit acquisition. Must specify `exclusive=True` XOR `concurrent=True`. Context manager methods `.exclusive()` and `.concurrent()` are preferred over this"""
        raise NotImplementedError(f"twpl: {_ERR_EXPLICIT_NOT_IMPLEMENTED}")
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
        raise NotImplementedError(f"twpl: {_ERR_EXPLICIT_NOT_IMPLEMENTED}")
 
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
