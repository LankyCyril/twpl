from sys import platform
from os import path, stat, remove
from glob import iglob
from tempfile import NamedTemporaryFile
from filelock import Timeout as FileLockTimeoutError, FileLock
from contextlib import contextmanager
from time import sleep
from datetime import datetime


__version__ = "0.0.2"

_ERR_PROC_TEST = "test poll of /proc returned an unexpected value"
_ERR_PLATFORM_TEST = "/proc not available and/or not a Linux/POSIX system"
_DEFAULT_POLL_MS = 100

EXCLUSIVE, CONCURRENT, UNCONDITIONAL = 0, 1, 2


def _NOT_IMPLEMENTED():
    raise NotImplementedError((
        "twpl: explicit acquire/release methods are not implemented yet; use "
        "context managers Twpl().exclusive() and Twpl().concurrent() instead"
    ))


def fds_exceed(filename, mincount): # TODO: cache /proc paths
    """Check if number of open file descriptors for `filename` exceeds `mincount`"""
    global fds_exceed
    # fds_exceed bootstraps itself on first call; this avoids, on the one hand,
    # checking on import and having to raise exceptions right away if something
    # is wrong; and on the other, checking every time a Twpl object is created.
    if not getattr(fds_exceed, "_is_bootstrapped", None): # TODO: not needed
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
                        fds_exceed._is_bootstrapped = True
                        return _fds_exceed(filename, mincount)
        else:
            raise OSError(f"twpl: {_ERR_PLATFORM_TEST}")


class Twpl():
    """Ties itself to a lockfile and provides exclusive (always singular) and concurrent (multiple in absence of exclusive) locks"""
 
    __slots__ = "_filename", "__mode"
 
    def __init__(self, filename):
        """Create lock object"""
        with FileLock(filename): # let filelock.FileLock() trigger checks
            self._filename = filename
            self.__mode = None
 
    @property
    def mode(self):
        return self.__mode
 
    def __acquire_exclusive(self, poll_ms): _NOT_IMPLEMENTED()
    def __release_exclusive(self): _NOT_IMPLEMENTED()
    def __acquire_concurrent(self): _NOT_IMPLEMENTED()
    def __release_concurrent(self): _NOT_IMPLEMENTED()
 
    def acquire(self, mode=None, *, poll_ms=None):
        """User interface for explicit acquisition. Must specify `exclusive=True` XOR `concurrent=True`. Context manager methods `.exclusive()` and `.concurrent()` are preferred over this"""
        if mode == EXCLUSIVE:
            if poll_ms is None:
                poll_ms = _DEFAULT_POLL_MS
            elif not isinstance(poll_ms, (int, float)):
                msg = "argument `poll_ms` must be a numeric value"
                raise ValueError(f"Twpl(...).acquire(EXCLUSIVE) {msg}")
            return self.__acquire_exclusive(poll_ms)
        elif mode == CONCURRENT:
            if poll_ms is not None:
                msg = "argument `poll_ms` must be None"
                raise ValueError(f"Twpl(...).acquire(CONCURRENT) {msg}")
            return self.__acquire_concurrent()
        else:
            msg = "argument `mode` must be EXCLUSIVE or CONCURRENT"
            raise ValueError(f"Twpl(...).acquire() {msg}")
 
    def release(self):
        """User interface for explicit release. Must specify `exclusive=True` XOR `concurrent=True`. Context manager methods `.exclusive()` and `.concurrent()` are preferred over this"""
        if self.__mode == EXCLUSIVE:
            return self.__release_exclusive()
        elif self.__mode == CONCURRENT:
            return self.__release_concurrent()
 
    @contextmanager
    def exclusive(self, *, poll_ms=_DEFAULT_POLL_MS):
        """Wait for all exclusive AND concurrent locks to release, acquire exclusive file lock, enter context, release this exclusive lock"""
        if not isinstance(poll_ms, (int, float)):
            raise ValueError(f"Twpl.exclusive(): {poll_ms=!r} is not a number")
        else:
            poll_s = poll_ms / 1000
        with FileLock(self._filename):
            while fds_exceed(self._filename, 1): # wait for all locks
                sleep(poll_s)
            try:
                self.__mode = EXCLUSIVE
                yield self
            finally:
                self.__mode = None
 
    @contextmanager
    def concurrent(self):
        """Wait for all exclusive locks to release, acquire concurrent file lock, enter context, release this concurrent lock"""
        with FileLock(self._filename) as lock: # intercept momentarily
            with open(self._filename): # grow fd count, prevent exclusive locks
                lock.release() # allow other concurrent locks to intercept
                try:
                    self.__mode = CONCURRENT
                    yield self
                finally:
                    self.__mode = None
 
    @contextmanager
    def unconditional(self):
        """Dummy context manager that always allows operation"""
        try:
            self.__mode = UNCONDITIONAL
            yield self
        finally:
            self.__mode = None
 
    def clean(self, *, min_age_ms=None):
        """Force remove lockfile if age is above `min_age_ms` regardless of state. Useful for cleaning up stale locks after crashes etc"""
        if not isinstance(min_age_ms, (int, float)):
            raise ValueError(f"Twpl.clean(): {min_age_ms=!r} is not a number")
        try:
            with FileLock(self._filename, timeout=0): # no exclusive locks now
                if not fds_exceed(self._filename, 1): # no concurrent locks;
                    # new locks (either exclusive or concurrent) will not be
                    # able to intercept while FileLock is locked on! Thus, here
                    # we can be certain we would be removing an unused lockfile.
                    st_ctime = stat(self._filename).st_ctime
                    dt = datetime.now() - datetime.fromtimestamp(st_ctime)
                    lock_age_ms = dt.total_seconds()*1000 + dt.microseconds/1000
                    if lock_age_ms >= min_age_ms:
                        remove(self._filename)
                        return True
        except FileLockTimeoutError: # something is currently locked on
            pass
        return False
