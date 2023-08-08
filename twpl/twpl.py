# TODO: more tests
# TODO: implement timeout for acquisition
# TODO: glob faster somehow - start from own PID?

from sys import platform
from os import path, stat, remove
from tempfile import NamedTemporaryFile
from glob import iglob
from filelock import Timeout as FileLockTimeoutError, FileLock
from multiprocessing import Lock
from types import SimpleNamespace
from contextlib import contextmanager
from datetime import datetime
from time import sleep


__version__ = "0.1.0"

EXCLUSIVE, CONCURRENT = 1, 2

TwplPlatformError = type("TwplPlatformError", (OSError,), {})
TwplValueError = type("TwplValueError", (ValueError,), {})
_ERR_MODE = "Twpl().acquire() argument `mode` must be EXCLUSIVE or CONCURRENT"
_ERR_PROC_TEST = "Test poll of /proc returned an unexpected value ({})"
_ERR_PLATFORM_TEST = "/proc is not available and/or not a Linux/POSIX system"
_BUGASS = " ".join((
    "twpl got itself into a broken condition at runtime. Please report this at",
    "https://github.com/LankyCyril/twpl/issues and provide a minimal example",
    "that replicates this along with the assertion that failed.",
))


def fds_exceed(filename, mincount, fdcache):
    """Check if number of open file descriptors for `filename` exceeds `mincount`"""
    global fds_exceed
    # fds_exceed bootstraps itself on first call; this avoids, on the one hand,
    # checking on import and having to raise exceptions right away if something
    # is wrong; and on the other, checking every time a Twpl object is created.
    if platform.startswith("linux") and path.isdir("/proc"):
        with NamedTemporaryFile(mode="w") as tf: # self-test
            fdc = set()
            with open(tf.name):
                if not _fds_exceed_POSIX(tf.name, 1, fdc):
                    raise TwplPlatformError(_ERR_PROC_TEST.format("<2"))
                elif _fds_exceed_POSIX(tf.name, 2, fdc):
                    raise TwplPlatformError(_ERR_PROC_TEST.format(">2"))
                else:
                    _fds_exceed_POSIX.__doc__ = fds_exceed.__doc__
                    fds_exceed = _fds_exceed_POSIX
                    return _fds_exceed_POSIX(filename, mincount, fdcache)
    else:
        raise TwplPlatformError(_ERR_PLATFORM_TEST)


def _fds_exceed_POSIX(filename, mincount, fdcache):
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


class Twpl():
    """Ties itself to a lockfile and provides exclusive (always singular) and concurrent (multiple in absence of exclusive) locks"""
 
    __slots__ = (
        "__filename", "__filelock", "__poll_ms",
        "__fdcache", "__countlock", "__handles", "__is_locked_exclusively",
    )
 
    def __init__(self, filename, *, poll_ms=100):
        """Create lock object"""
        with FileLock(filename): # let filelock.FileLock() trigger checks
            self.__filename, self.__filelock = filename, FileLock(filename)
            self.__poll_ms, self.__fdcache = poll_ms, set()
            self.__countlock, self.__handles = Lock(), []
            self.__is_locked_exclusively = False
 
    @property
    def filename(self):
        return self.__filename
 
    @property
    def mode(self):
        with self.__countlock:
            if self.__is_locked_exclusively:
                assert not self.__handles, _BUGASS
                return EXCLUSIVE
            elif self.__handles:
                assert not self.__is_locked_exclusively, _BUGASS
                return CONCURRENT
            else:
                return None
 
    @property
    def state(self):
        return SimpleNamespace(
            mode=self.mode,
            exclusive=self.__is_locked_exclusively,
            concurrent=len(self.__handles),
        )
 
    def acquire(self, mode, *, poll_ms=None):
        """User interface for explicit acquisition. Context manager methods `.exclusive()` and `.concurrent()` are preferred over this"""
        if mode == EXCLUSIVE:
            return self.__acquire_exclusive(poll_ms)
        elif mode == CONCURRENT:
            return self.__acquire_concurrent(poll_ms)
        else:
            raise TwplValueError(_ERR_MODE)
 
    def release(self):
        """User interface for explicit release. Context manager methods `.exclusive()` and `.concurrent()` are preferred over this"""
        if self.mode == EXCLUSIVE:
            return self.__release_exclusive()
        elif self.mode == CONCURRENT:
            return self.__release_concurrent()
        else:
            return self
 
    @contextmanager
    def exclusive(self, *, poll_ms=None):
        """Wait for all exclusive AND concurrent locks to release, acquire exclusive file lock, enter context, release this exclusive lock"""
        try:
            yield self.__acquire_exclusive(poll_ms)
        finally:
            self.__release_exclusive()
 
    @contextmanager
    def concurrent(self, *, poll_ms=None):
        """Wait for all exclusive locks to release, acquire concurrent file lock, enter context, release this concurrent lock"""
        try:
            yield self.__acquire_concurrent(poll_ms)
        finally:
            self.__release_concurrent()
 
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
 
    def __acquire_exclusive(self, poll_ms):
        poll_s = (self.__poll_ms if (poll_ms is None) else poll_ms) / 1000
        self.__filelock.acquire(poll_interval=poll_s/3)
        while fds_exceed(self.__filename, 1, self.__fdcache):
            sleep(poll_s) # wait for all locks
        with self.__countlock:
            assert not (self.__is_locked_exclusively or self.__handles), _BUGASS
            self.__is_locked_exclusively = True
        return self
 
    def __release_exclusive(self):
        with self.__countlock:
            assert self.__is_locked_exclusively, _BUGASS
            self.__is_locked_exclusively = False
            self.__filelock.release()
            return self
 
    def __acquire_concurrent(self, poll_ms):
        poll_s = (self.__poll_ms if (poll_ms is None) else poll_ms) / 1000
        self.__filelock.acquire(poll_interval=poll_s/3) # intercept momentarily
        try:
            h = open(self.__filename) # grow fd count, prevent exclusive locks
        finally:
            self.__filelock.release() # but allow concurrent locks to intercept
        with self.__countlock:
            assert not self.__is_locked_exclusively, _BUGASS
            self.__handles.append(h)
        return self
 
    def __release_concurrent(self):
        with self.__countlock:
            assert self.__handles, _BUGASS
            self.__handles.pop().close() # reduce fd count
            return self
 
    def __del__(self):
        with self.__countlock:
            if self.__is_locked_exclusively:
                self.__is_locked_exclusively = False
                self.__filelock.release()
            while self.__handles:
                self.__handles.pop().close()
