from sys import platform
from os import path, stat, getpid, walk, remove
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
TwplTimeoutError = type("TwplTimeoutError", (FileLockTimeoutError,), {})
_ERR_PROC_TEST = "Test poll of /proc returned an unexpected value ({})".format
_ERR_PLATFORM_TEST = "/proc is not available and/or not a Linux/POSIX system"
_ERR_MODE = "Twpl().acquire() argument `mode` must be EXCLUSIVE or CONCURRENT"
_ERR_ACQUIRE = "File lock could not be acquired in time ({}, timeout={})".format
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
                    raise TwplPlatformError(_ERR_PROC_TEST("<2"))
                elif _fds_exceed_POSIX(tf.name, 2, fdc):
                    raise TwplPlatformError(_ERR_PROC_TEST(">2"))
                else:
                    _fds_exceed_POSIX.__doc__ = fds_exceed.__doc__
                    fds_exceed = _fds_exceed_POSIX
                    return _fds_exceed_POSIX(filename, mincount, fdcache)
    else:
        raise TwplPlatformError(_ERR_PLATFORM_TEST)


def _fds_exceed_POSIX(filename, mincount, fdcache):
    # This replaces fds_exceed. Note that it only checks the number of open fds
    # under an acquired FileLock (Twpl.__acquire_exclusive()), and the number of
    # open fds can only grow under the same FileLock as well: via open() in
    # Twpl.__acquire_concurrent(). So while fd symlinks can disappear while we
    # iterate here, there will never be new relevant fds missed by walk().
    realpath, n, fdcache_copy = path.realpath(filename), 0, set(fdcache)
    def _iter_fds():
        yield from fdcache_copy
        def _iter_pids():
            ownpid, preceding_pids = getpid(), []
            for pid in (int(d) for d in next(walk("/proc"))[1] if d.isdigit()):
                if pid < ownpid:
                    preceding_pids.append(pid)
                else:
                    yield pid
            yield from reversed(preceding_pids)
        for fds in (iglob(f"/proc/{pid}/fd/*") for pid in _iter_pids()):
            yield from (fd for fd in fds if fd not in fdcache_copy)
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
        "__filename", "__poll_interval", "__countlock", "__fdcache",
        "__handles", "__is_locked_exclusively", "__exclusive_filelock",
    )
 
    def __init__(self, filename, *, poll_interval=.1):
        """Create lock object"""
        with FileLock(filename): # let filelock.FileLock() trigger checks
            self.__filename = filename
            self.__poll_interval, self.__countlock = poll_interval, Lock()
            self.__fdcache, self.__handles = set(), []
            self.__is_locked_exclusively = False
            self.__exclusive_filelock = FileLock(filename)
 
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
 
    def acquire(self, mode, *, poll_interval=None, timeout=None):
        """User interface for explicit acquisition. Context manager methods `.exclusive()` and `.concurrent()` are preferred over this"""
        if mode == EXCLUSIVE:
            return self.__acquire_exclusive(poll_interval, timeout)
        elif mode == CONCURRENT:
            return self.__acquire_concurrent(poll_interval, timeout)
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
    def exclusive(self, *, poll_interval=None, timeout=None):
        """Wait for all exclusive AND concurrent locks to release, acquire exclusive file lock, enter context, release this exclusive lock"""
        try:
            yield self.__acquire_exclusive(poll_interval, timeout)
        finally:
            self.__release_exclusive()
 
    @contextmanager
    def concurrent(self, *, poll_interval=None, timeout=None):
        """Wait for all exclusive locks to release, acquire concurrent file lock, enter context, release this concurrent lock"""
        try:
            yield self.__acquire_concurrent(poll_interval, timeout)
        finally:
            self.__release_concurrent()
 
    def clean(self, *, min_age_seconds):
        """Force remove lockfile if age is above `min_age_seconds` regardless of state. Useful for cleaning up stale locks after crashes etc"""
        try:
            with FileLock(self.__filename, timeout=0): # no exclusive locks now,
                if not fds_exceed(self.__filename, 1, self.__fdcache): # and ...
                    # ... no concurrent locks. New locks (either exclusive or
                    # concurrent) will not be able to intercept while FileLock
                    # is locked on! Thus, here we can be certain we would be
                    # removing an unused lockfile.
                    st_ctime = stat(self.__filename).st_ctime
                    dt = datetime.now() - datetime.fromtimestamp(st_ctime)
                    if dt.total_seconds() >= min_age_seconds:
                        remove(self.__filename)
                        return True
        except FileLockTimeoutError: # something is actively locked on, bail
            pass
        return False
 
    def __acquire_exclusive(self, poll_interval, timeout):
        try:
            start_ts = datetime.now()
            self.__exclusive_filelock.acquire(
                poll_interval=(poll_interval or self.__poll_interval)/3,
                timeout=timeout,
            )
            timeout_remaining = (datetime.now() - start_ts).total_seconds()
            while fds_exceed(self.__filename, 1, self.__fdcache):
                timeout_remaining -= (poll_interval or self.__poll_interval)
                if timeout_remaining < 0:
                    raise FileLockTimeoutError(self.__filename)
                else:
                    sleep(poll_interval) # wait for all locks
        except FileLockTimeoutError:
            raise TwplTimeoutError(_ERR_ACQUIRE(self.__filename, timeout))
        with self.__countlock:
            assert not (self.__is_locked_exclusively or self.__handles), _BUGASS
            self.__is_locked_exclusively = True
        return self
 
    def __release_exclusive(self):
        with self.__countlock:
            assert self.__is_locked_exclusively, _BUGASS
            self.__is_locked_exclusively = False
            assert self.__exclusive_filelock.is_locked, _BUGASS
            self.__exclusive_filelock.release()
            return self
 
    def __acquire_concurrent(self, poll_interval, timeout):
        momentary_filelock = FileLock(self.__filename)
        try: # intercept momentarily, forcing all new locks to block:
            momentary_filelock.acquire(
                poll_interval=(poll_interval or self.__poll_interval),
                timeout=timeout,
            )
        except FileLockTimeoutError:
            raise TwplTimeoutError(_ERR_ACQUIRE(self.__filename, timeout))
        try: # grow fd count, prevent exclusive locks
            h = open(self.__filename)
        finally: # but allow other concurrent locks to intercept
            momentary_filelock.release()
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
                assert self.__exclusive_filelock.is_locked, _BUGASS
                self.__exclusive_filelock.release()
                self.__is_locked_exclusively = False
            while self.__handles:
                self.__handles.pop().close()
