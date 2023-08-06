# TODO: test that _ERR_CONCURRENT_REACQUIRE happens when it needs to happen
# TODO: make poll_ms an object-level setting
# TODO: adjust FileLock.acquire(poll_interval=) according to poll_ms

from sys import platform
from os import path, stat, remove
from glob import iglob
from tempfile import NamedTemporaryFile
from filelock import Timeout as FileLockTimeoutError, FileLock
from multiprocessing import Lock
from contextlib import contextmanager
from time import sleep
from datetime import datetime


__version__ = "0.1.0"

EXCLUSIVE, CONCURRENT, _DEFAULT_POLL_MS = 0, 1, 100
_ERR_CONCURRENT_REACQUIRE = " ".join((
    "Explicit concurrent acquire of an already acquired Twpl instance;",
    "if you must do it, use a context manager Twpl().concurrent() instead",
))
_ERR_PROC_TEST = "Test poll of /proc returned an unexpected value"
_ERR_PLATFORM_TEST = "/proc is not available and/or not a Linux/POSIX system"

class TwplError(RuntimeError): pass
class TwplPlatformError(OSError): pass


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
                    raise TwplPlatformError(_ERR_PROC_TEST)
                else:
                    _fds_exceed.__doc__ = fds_exceed.__doc__
                    fds_exceed = _fds_exceed
                    return _fds_exceed(filename, mincount, fdcache)
    else:
        raise TwplPlatformError(_ERR_PLATFORM_TEST)


class Twpl():
    """Ties itself to a lockfile and provides exclusive (always singular) and concurrent (multiple in absence of exclusive) locks"""
 
    __slots__ = (
        "__filename", "__filelock", "__fdcache", "__handles", "__last_handle",
        "__countlock", "__n_exclusive", "__n_concurrent",
    )
 
    def __init__(self, filename):
        """Create lock object"""
        with FileLock(filename): # let filelock.FileLock() trigger checks
            self.__filename, self.__filelock = filename, FileLock(filename)
            self.__fdcache = set()
            self.__handles, self.__last_handle = set(), None
            self.__countlock = Lock()
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
 
    @contextmanager
    def exclusive(self, *, poll_ms=_DEFAULT_POLL_MS):
        """Wait for all exclusive AND concurrent locks to release, acquire exclusive file lock, enter context, release this exclusive lock"""
        try:
            yield self.__acquire_exclusive(poll_ms)
        finally:
            self.__release_exclusive()
 
    def __acquire_exclusive(self, poll_ms):
        poll_s = poll_ms / 1000
        self.__filelock.acquire()
        while fds_exceed(self.__filename, 1, self.__fdcache):
            sleep(poll_s) # wait for all locks
        with self.__countlock:
            assert self.__n_exclusive == self.__n_concurrent == 0, "bug!"
            self.__n_exclusive = 1
        return self
 
    def __release_exclusive(self):
        with self.__countlock:
            assert self.__n_exclusive == 1, "bug!"
            self.__n_exclusive = 0
        self.__filelock.release()
 
    @contextmanager
    def concurrent(self):
        """Wait for all exclusive locks to release, acquire concurrent file lock, enter context, release this concurrent lock"""
        try:
            h = self.__acquire_concurrent(_in_context=True)
            yield self
        finally:
            self.__release_concurrent(_in_context=h)
 
    def __acquire_concurrent(self, *, _in_context=False):
        if (not _in_context) and (self.mode is not None):
            raise TwplError(_ERR_CONCURRENT_REACQUIRE)
        self.__filelock.acquire() # intercept momentarily
        h = open(self.__filename) # grow fd count, prevent exclusive locks
        self.__filelock.release() # allow other concurrent locks to intercept
        with self.__countlock:
            assert self.__n_exclusive == 0, "bug!"
            self.__n_concurrent += 1
            self.__handles.add(h)
            self.__last_handle = h
        return _in_context and h or self
 
    def __release_concurrent(self, *, _in_context=False):
        with self.__countlock:
            assert self.__n_concurrent > 0, "bug!"
            self.__n_concurrent -= 1
            self.__handles.remove(_in_context or self.__last_handle)
            self.__last_handle.close() # reduce fd count
 
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
