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


__version__ = "0.1.3"

NamedInt = lambda v, r: type("NamedInt", (int,), dict(
    __new__=lambda s: int.__new__(s, v), __repr__=lambda s: r,
))()
EXCLUSIVE, CONCURRENT = NamedInt(1, "EXCLUSIVE"), NamedInt(2, "CONCURRENT")

TwplPlatformError = type("TwplPlatformError", (OSError,), {})
TwplValueError = type("TwplValueError", (ValueError,), {})
TwplTimeoutError = type("TwplTimeoutError", (TimeoutError,), {})
TwplStateError = type("TwplStateError", (RuntimeError,), {})

_ERR_PROC_TEST = "Test poll of /proc returned an unexpected value ({})".format
_ERR_PLATFORM_TEST = "/proc is not available and/or not a Linux/POSIX system"
_ERR_MODE = "Twpl().acquire() argument `mode` must be EXCLUSIVE or CONCURRENT"
_ERR_ACQUIRE = "File lock could not be acquired in time ({}, timeout={})".format
_ERR_EXCLUSIVE_CONCURRENCY = "Exclusive mode must have max_concurrency==1"
_ERR_STATE = " ".join((
    "Instance of Twpl() is in an error state due to a previous",
    "(ignored? unhandled?) exception; will not proceed",
))
_BUGASS = " ".join((
    "twpl got itself into a broken condition at runtime. Please report this at",
    "https://github.com/LankyCyril/twpl/issues and provide a minimal example",
    "that replicates this along with the assertion that failed.",
))


def fds_exceed(filename, mincount, fdcache): # NOQA:F811
    """ Check if number of open file descriptors for `filename`
        exceeds `mincount`
    """
    global fds_exceed
    # fds_exceed bootstraps itself on first call; this avoids, on the one hand,
    # checking on import and having to raise exceptions right away if something
    # is wrong; and on the other, checking every time a Twpl object is created.
    if platform.startswith("linux") and path.isdir("/proc"):
        with NamedTemporaryFile(mode="w") as tf: # self-test
            fdc = set()
            with open(tf.name):
                if not _fds_exceed_posix(tf.name, 1, fdc):
                    raise TwplPlatformError(_ERR_PROC_TEST("<2"))
                elif _fds_exceed_posix(tf.name, 2, fdc):
                    raise TwplPlatformError(_ERR_PROC_TEST(">2"))
                else:
                    _fds_exceed_posix.__doc__ = fds_exceed.__doc__
                    fds_exceed = _fds_exceed_posix
                    return _fds_exceed_posix(filename, mincount, fdcache)
    else:
        raise TwplPlatformError(_ERR_PLATFORM_TEST)


def _fds_exceed_posix(filename, mincount, fdcache):
    """ This replaces fds_exceed. Note that it only checks the number of open
        fds under an acquired FileLock (`Twpl.__acquire_exclusive()`), and the
        number of open fds can only grow under the same FileLock as well: via
        `open()` in `Twpl.__acquire_concurrent()`. So while fd symlinks can
        disappear while we iterate here, there will never be new relevant fds
        missed by `walk()`.
    """
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
        except OSError:
            if fd in fdcache:
                fdcache.remove(fd)
    else:
        return False


class Twpl():
    """ Ties itself to a lockfile and provides exclusive (always singular) and
        concurrent (multiple in absence of exclusive) locks
    """
 
    __slots__ = (
        "__filename", "__poll_interval", "__countlock", "__fdcache",
        "__handles", "__is_locked_exclusively", "__exclusive_filelock",
        "__error",
    )
 
    def __init__(self, filename, *, poll_interval=.1):
        """ Create lock object """
        self.__filename = filename
        self.__poll_interval, self.__countlock = poll_interval, Lock()
        self.__fdcache, self.__handles = set(), []
        self.__is_locked_exclusively = False
        self.__exclusive_filelock = FileLock(filename)
        self.__error = None
 
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
            n_exclusive_locks=int(self.__is_locked_exclusively),
            n_concurrent_locks=len(self.__handles),
            error=self.__error,
        )
 
    def acquire(self, mode, *, poll_interval=None, timeout=None, max_concurrency=None):
        """User interface for explicit acquisition. Context manager methods `.exclusive()` and `.concurrent()` are preferred over this"""
        if self.__error:
            raise TwplStateError(_ERR_STATE, self.__error)
        elif mode == EXCLUSIVE:
            if max_concurrency not in {None, 1}:
                raise TwplValueError(_ERR_EXCLUSIVE_CONCURRENCY)
            else:
                return self.__acquire_exclusive(poll_interval, timeout)
        elif mode == CONCURRENT:
            return self.__acquire_concurrent(
                poll_interval, timeout,
                float("inf") if (max_concurrency is None) else max_concurrency,
            )
        else:
            self.__error = TwplValueError(_ERR_MODE)
            raise self.__error
 
    def release(self):
        """User interface for explicit release. Context manager methods `.exclusive()` and `.concurrent()` are preferred over this"""
        if self.__error:
            raise TwplStateError(_ERR_STATE, self.__error)
        elif self.mode == EXCLUSIVE:
            return self.__release_exclusive()
        elif self.mode == CONCURRENT:
            return self.__release_concurrent()
        else:
            return self
 
    @contextmanager
    def exclusive(self, *, poll_interval=None, timeout=None):
        """Wait for all exclusive AND concurrent locks to release, acquire exclusive file lock, enter context, release this exclusive lock"""
        if self.__error:
            raise TwplStateError(_ERR_STATE, self.__error)
        try:
            yield self.__acquire_exclusive(poll_interval, timeout)
        except Exception as e: # NOQA:BLE001
            self.__error = e
            raise self.__error
        finally:
            self.__release_exclusive()
 
    @contextmanager
    def concurrent(self, *, poll_interval=None, timeout=None, max_concurrency=float("inf")):
        """Wait for all exclusive locks to release, acquire concurrent file lock, enter context, release this concurrent lock"""
        if self.__error:
            raise TwplStateError(_ERR_STATE, self.__error)
        try:
            yield self.__acquire_concurrent(
                poll_interval, timeout, max_concurrency,
            )
        except Exception as e: # NOQA:BLE001
            self.__error = e
            raise self.__error
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
                    then = datetime.fromtimestamp(st_ctime) # NOQA:DTZ006
                    dt = datetime.now() - then # NOQA:DTZ005
                    if dt.total_seconds() >= min_age_seconds:
                        remove(self.__filename)
                        return True
        except FileLockTimeoutError: # something is actively locked on, bail
            pass
        return False
 
    def __acquire_exclusive(self, poll_interval, timeout):
        try:
            start_ts = datetime.now() # NOQA:DTZ005
            self.__exclusive_filelock.acquire(
                poll_interval=(poll_interval or self.__poll_interval)/3,
                timeout=timeout,
            )
            if timeout is not None:
                dt = datetime.now() - start_ts # NOQA:DTZ005
                timeout_remaining = timeout - dt.total_seconds()
            while fds_exceed(self.__filename, 1, self.__fdcache):
                if timeout is not None:
                    timeout_remaining -= (poll_interval or self.__poll_interval)
                    if timeout_remaining < 0:
                        raise FileLockTimeoutError(self.__filename)
                # wait for all locks:
                sleep(poll_interval or self.__poll_interval)
        except Exception as e:
            if self.__exclusive_filelock.is_locked:
                self.__exclusive_filelock.release()
            if isinstance(e, FileLockTimeoutError):
                e = TwplTimeoutError(_ERR_ACQUIRE(self.__filename, timeout))
            self.__error = e
            raise e
        with self.__countlock:
            assert not (self.__is_locked_exclusively or self.__handles), _BUGASS
            self.__is_locked_exclusively = True
        return self
 
    def __release_exclusive(self):
        with self.__countlock:
            if not self.__error:
                assert self.__is_locked_exclusively, _BUGASS
                self.__is_locked_exclusively = False
            if not self.__error:
                assert self.__exclusive_filelock.is_locked, _BUGASS
            if self.__exclusive_filelock.is_locked:
                self.__exclusive_filelock.release()
            return self
 
    def __acquire_concurrent(self, poll_interval, timeout, max_concurrency=float("inf")):
        start_ts = datetime.now() # NOQA:DTZ005
        momentary_filelock = FileLock(self.__filename)
        try: # intercept for the duration of the check, so others' locks block:
            momentary_filelock.acquire(
                poll_interval=(poll_interval or self.__poll_interval),
                timeout=timeout,
            )
        except FileLockTimeoutError:
            e = TwplTimeoutError(_ERR_ACQUIRE(self.__filename, timeout))
            self.__error = e
            raise self.__error
        with self.__countlock:
            assert not self.__is_locked_exclusively, _BUGASS
            if timeout is not None:
                dt = datetime.now() - start_ts # NOQA:DTZ005
                timeout_remaining = timeout - dt.total_seconds()
            fds_thresh = max_concurrency
            while fds_exceed(self.__filename, fds_thresh, self.__fdcache):
                if timeout is not None:
                    timeout_remaining -= (poll_interval or self.__poll_interval)
                    if timeout_remaining < 0:
                        err = _ERR_ACQUIRE(self.__filename, timeout)
                        raise TwplTimeoutError(err)
                # wait for acceptable concurrency:
                sleep(poll_interval or self.__poll_interval)
            try: # grow fd count, prevent exclusive locks
                self.__handles.append(open(self.__filename))
            finally: # but allow other concurrent locks to intercept
                momentary_filelock.release()
        return self
 
    def __release_concurrent(self):
        with self.__countlock:
            if not self.__error:
                assert self.__handles, _BUGASS
                self.__handles.pop().close() # reduce fd count
            return self
 
    def _debug_get_fd_count_raw_posix(self):
        """ This method duplicates a lot of code from `_fds_exceed_posix()`;
            this is intentional. `_fds_exceed_posix()` is designed to return as
            soon as possible and provide optimal performance; this here method
            exists just for debugging purposes and will evaluate all fd links it
            can find, without consulting an fd cache.
            As such, there's little performance-oriented reasons to adhere to
            the DRY principle and try to consolidate their functionality.
        """
        realpath, n = path.realpath(self.__filename), 0
        def _iter_fds():
            def _iter_pids():
                ownpid, preceding_pids = getpid(), []
                pids = (int(d) for d in next(walk("/proc"))[1] if d.isdigit())
                for pid in pids:
                    if pid < ownpid:
                        preceding_pids.append(pid)
                    else:
                        yield pid
                yield from reversed(preceding_pids)
            for fds in (iglob(f"/proc/{pid}/fd/*") for pid in _iter_pids()):
                yield from fds
        for fd in _iter_fds():
            try:
                n += (path.realpath(fd) == realpath)
            except OSError:
                pass
        return n
 
    def __del__(self):
        with self.__countlock:
            if self.__is_locked_exclusively:
                if not self.__error:
                    assert self.__exclusive_filelock.is_locked, _BUGASS
                    self.__is_locked_exclusively = False
                self.__exclusive_filelock.release()
            while self.__handles:
                self.__handles.pop().close()
