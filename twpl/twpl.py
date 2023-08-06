# TODO: test if threading.Lock() is usable in multiprocessing scenarios
# TODO: implement explicit acquire/release, reuse in context managers
# TODO: make poll_ms an object-level setting
# TODO: adjust FileLock.acquire(poll_interval=) according to poll_ms

from sys import platform
from os import path, stat, remove
from glob import iglob
from tempfile import NamedTemporaryFile
from filelock import Timeout as FileLockTimeoutError, FileLock
from threading import Lock
from contextlib import contextmanager
from time import sleep
from datetime import datetime


__version__ = "0.1.0"

EXCLUSIVE, CONCURRENT, _DEFAULT_POLL_MS = 0, 1, 100
_ERR_PROC_TEST = "test poll of /proc returned an unexpected value"
_ERR_PLATFORM_TEST = "/proc not available and/or not a Linux/POSIX system"


def _NOT_IMPLEMENTED():
    raise NotImplementedError((
        "twpl: explicit acquire/release methods are not implemented yet; use "
        "context managers Twpl().exclusive() and Twpl().concurrent() instead"
    ))


def fds_exceed(filename, mincount, fdcache):
    """Check if number of open file descriptors for `filename` exceeds `mincount`"""
    global fds_exceed
    # fds_exceed bootstraps itself on first call; this avoids, on the one hand,
    # checking on import and having to raise exceptions right away if something
    # is wrong; and on the other, checking every time a Twpl object is created.
    if platform.startswith("linux") and path.isdir("/proc"):
        def _fds_exceed(filename, mincount, fdcache):
            realpath, n, fdcache_copy = path.realpath(filename), 0, set(fdcache)
            def _iter_fds(PAT="/proc/[0-9]*/fd/*"):
                yield from fdcache_copy
                yield from (fd for fd in iglob(PAT) if fd not in fdcache_copy)
            for fd in _iter_fds():
                try:
                    if path.realpath(fd) == realpath:
                        fdcache.add(fd)
                        n += 1
                        if n > mincount:
                            return True
                    elif fd in fdcache:
                        fdcache.remove(fd)
                except FileNotFoundError:
                    if fd in fdcache:
                        fdcache.remove(fd)
            else:
                return False
        with NamedTemporaryFile(mode="w") as t: # quick self-test
            c = set()
            with open(t.name):
                if (not _fds_exceed(t.name, 1, c)) or _fds_exceed(t.name, 2, c):
                    raise OSError(f"twpl: {_ERR_PROC_TEST}")
                else:
                    _fds_exceed.__doc__ = fds_exceed.__doc__
                    fds_exceed = _fds_exceed
                    return _fds_exceed(filename, mincount, fdcache)
    else:
        raise OSError(f"twpl: {_ERR_PLATFORM_TEST}")


class Twpl():
    """Ties itself to a lockfile and provides exclusive (always singular) and concurrent (multiple in absence of exclusive) locks"""
 
    __slots__ = (
        "__filename", "__countlock", "__fdcache",
        "__n_exclusive", "__n_concurrent",
    )
 
    def __init__(self, filename):
        """Create lock object"""
        with FileLock(filename): # let filelock.FileLock() trigger checks
            self.__filename, self.__countlock = filename, Lock()
            self.__fdcache = set()
            self.__n_exclusive, self.__n_concurrent = 0, 0
 
    @property
    def mode(self):
        with self.__countlock:
            if self.__n_exclusive:
                assert self.__n_concurrent == 0, "bug!"
                return EXCLUSIVE
            elif self.__n_concurrent:
                assert self.__n_exclusive == 0, "bug!"
                return CONCURRENT
            else:
                return None
 
    @property
    def filename(self):
        return self.__filename
 
    def __acquire_exclusive(self, poll_ms): _NOT_IMPLEMENTED()
    def __release_exclusive(self): _NOT_IMPLEMENTED()
    def __acquire_concurrent(self): _NOT_IMPLEMENTED()
    def __release_concurrent(self): _NOT_IMPLEMENTED()
 
    def acquire(self, mode, *, poll_ms=None):
        """User interface for explicit acquisition. Context manager methods `.exclusive()` and `.concurrent()` are preferred over this"""
        def error(*args):
            raise ValueError("Twpl().acquire({}) {}".format(*args))
        if mode == EXCLUSIVE:
            if poll_ms is None:
                poll_ms = _DEFAULT_POLL_MS
            elif not isinstance(poll_ms, (int, float)):
                error("EXCLUSIVE", "argument `poll_ms` must be a numeric value")
            return self.__acquire_exclusive(poll_ms)
        elif mode == CONCURRENT:
            if poll_ms is not None:
                error("CONCURRENT", "argument `poll_ms` must be None")
            return self.__acquire_concurrent()
        else:
            error("", "argument `mode` must be EXCLUSIVE or CONCURRENT")
 
    def release(self):
        """User interface for explicit release. Context manager methods `.exclusive()` and `.concurrent()` are preferred over this"""
        if self.mode == EXCLUSIVE:
            return self.__release_exclusive()
        elif self.mode == CONCURRENT:
            return self.__release_concurrent()
 
    @contextmanager
    def exclusive(self, *, poll_ms=_DEFAULT_POLL_MS):
        """Wait for all exclusive AND concurrent locks to release, acquire exclusive file lock, enter context, release this exclusive lock"""
        poll_s = poll_ms / 1000
        with FileLock(self.__filename):
            while fds_exceed(self.__filename, 1, self.__fdcache):
                # wait for all locks
                sleep(poll_s)
            try:
                with self.__countlock:
                    assert self.__n_exclusive == 0, "bug!"
                    assert self.__n_concurrent == 0, "bug!"
                    self.__n_exclusive = 1
                yield self
            finally:
                with self.__countlock:
                    assert self.__n_exclusive == 1, "bug!"
                    self.__n_exclusive = 0
 
    @contextmanager
    def concurrent(self):
        """Wait for all exclusive locks to release, acquire concurrent file lock, enter context, release this concurrent lock"""
        with FileLock(self.__filename) as lock: # intercept momentarily
            with open(self.__filename): # grow fd count, prevent exclusive locks
                lock.release() # allow other concurrent locks to intercept
                try:
                    with self.__countlock:
                        assert self.__n_exclusive == 0, "bug!"
                        self.__n_concurrent += 1
                    yield self
                finally:
                    with self.__countlock:
                        self.__n_concurrent -= 1
                        assert self.__n_concurrent >= 0, "bug!"
 
    def clean(self, *, min_age_ms):
        """Force remove lockfile if age is above `min_age_ms` regardless of state. Useful for cleaning up stale locks after crashes etc"""
        try:
            with FileLock(self.__filename, timeout=0): # no exclusive locks now,
                if not fds_exceed(self.__filename, 1, self.__fdcache): # and ...
                    # ... no concurrent locks. New locks (either exclusive or
                    # concurrent) will not be able to intercept while FileLock
                    # is locked on! Thus, here we can be certain we would be
                    # removing an unused lockfile.
                    st_ctime = stat(self.__filename).st_ctime
                    dt = datetime.now() - datetime.fromtimestamp(st_ctime)
                    lock_age_ms = dt.total_seconds()*1000 + dt.microseconds/1000
                    if lock_age_ms >= min_age_ms:
                        remove(self.__filename)
                        return True
        except FileLockTimeoutError: # something is actively locked on, bail
            pass
        return False
