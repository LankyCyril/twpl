#!/usr/bin/env python
from twpl import Twpl, __version__, EXCLUSIVE, CONCURRENT, TwplTimeoutError
from sys import modules
from datetime import datetime
from contextlib import contextmanager
from threading import Thread
from time import sleep
from os import path, remove

print(f"Loaded from {modules['twpl'].__file__}")
print(f"{__version__=}")


_SLEEP_FACTOR = .02
ts = lambda: datetime.now().timestamp()


@contextmanager
def NamedTest(name, lockfilename):
    start_ts = ts()
    print(f"TEST STARTED | {name}({lockfilename!r})")
    try:
        yield
    except:
        print(f"{'FAIL!':>12} | {name}({lockfilename!r})")
        print(f"{'`':>14} in {ts()-start_ts}")
        raise
    else:
        print(f"{'SUCCESS':>12} | {name}({lockfilename!r})")
        print(f"{'`':>14} in {ts()-start_ts}")
    finally:
        try:
            remove(lockfilename)
        except FileNotFoundError:
            pass


def await_daemons(*daemon_param_tuples):
    # TODO: ExceptionPropagatingThread
    daemons = []
    for target, *args in daemon_param_tuples:
        daemon = Thread(target=target, args=args, daemon=True)
        daemon.start()
        daemons.append(daemon)
    for daemon in daemons:
        daemon.join()


def Reader(lockfilename, name, delay, duration, enter_order, leave_order, timeout=None):
    delay, duration = delay * _SLEEP_FACTOR, duration * _SLEEP_FACTOR
    timeout = None if (timeout is None) else timeout * _SLEEP_FACTOR
    pfx = f"{'|':>14} Reader {name}"
    print(f"{pfx} will idle for {delay} seconds", flush=True)
    sleep(delay)
    print(f"{pfx} will be reading for {duration} seconds", flush=True)
    lock = Twpl(lockfilename, poll_interval=.01*_SLEEP_FACTOR)
    with lock.concurrent(timeout=timeout):
        print(f"{pfx} acquired a concurrent lock at {ts()}", flush=True)
        enter_order.append(name)
        sleep(duration)
    print(f"{pfx} done", flush=True)
    leave_order.append(name)


def Writer(lockfilename, name, delay, duration, enter_order, leave_order, timeout=None):
    delay, duration = delay * _SLEEP_FACTOR, duration * _SLEEP_FACTOR
    timeout = None if (timeout is None) else timeout * _SLEEP_FACTOR
    pfx = f"{'|':>14} Writer {name}"
    print(f"{pfx} will idle for {delay} seconds", flush=True)
    sleep(delay)
    print(f"{pfx} will be writing for {duration} seconds", flush=True)
    lock = Twpl(lockfilename, poll_interval=.01*_SLEEP_FACTOR)
    with lock.exclusive(timeout=timeout):
        print(f"{pfx} acquired an exclusive lock at {ts()}", flush=True)
        enter_order.append(name)
        sleep(duration)
    print(f"{pfx} done", flush=True)
    leave_order.append(name)


def basic_methods(lockfilename):
    with NamedTest(".exclusive", lockfilename):
        with Twpl(lockfilename).exclusive() as lock:
            assert lock.mode == EXCLUSIVE
        assert lock.mode is None
    with NamedTest(".concurrent", lockfilename):
        with Twpl(lockfilename).concurrent() as lock:
            assert lock.mode == CONCURRENT
        assert lock.mode is None
    with NamedTest(f".acquire|release", lockfilename):
        lock = Twpl(lockfilename)
        assert lock.acquire(EXCLUSIVE).mode == EXCLUSIVE
        assert lock.state.exclusive
        assert lock.release().mode is None
        assert lock.acquire(CONCURRENT).mode == CONCURRENT
        assert lock.acquire(CONCURRENT).mode == CONCURRENT
        assert lock.state.concurrent == 2
        assert lock.release().mode == CONCURRENT
        assert lock.release().mode is None
        assert lock.release().mode is None
        assert not (lock.state.concurrent or lock.state.exclusive)
    with NamedTest(".clean", lockfilename):
        assert not Twpl(lockfilename).clean(min_age_seconds=60)
        assert path.isfile(lockfilename)
        assert Twpl(lockfilename).clean(min_age_seconds=0)
        assert not path.exists(lockfilename)


def writers(lockfilename):
    with NamedTest("writers", lockfilename):
        order = []
        await_daemons(*(
            (Writer, lockfilename, reader_name, 0, 10, order, order)
            for reader_name in ("R1", "R2", "R3", "R4", "R5")
        ))
        print(f"{'|':>14} {order=}", flush=True)
        assert order[::2] == order[1::2]


def readers(lockfilename):
    with NamedTest("readers", lockfilename):
        order = []
        await_daemons(*(
            (Reader, lockfilename, reader_name, delay, 10, order, order)
            for delay, reader_name in enumerate(("R1", "R2", "R3", "R4", "R5"))
        ))
        print(f"{'|':>14} {order=}", flush=True)
        assert order[:5] == order[5:]


def nested_readers(lockfilename):
    with NamedTest("nested_readers", lockfilename):
        lock = Twpl(lockfilename)
        writer = Thread(target=Writer, args=(lockfilename, "Wx", 1, 2, [], []))
        writer.start()
        with lock.concurrent():
            with lock.concurrent():
                with lock.concurrent():
                    with lock.concurrent():
                        with lock.concurrent():
                            print(f"{'|':>14} 5 concurrent readers", flush=True)
                            print(f"{'|':>14} {lock.mode=}", flush=True)
        writer.join()


def readers_writer_readers(lockfilename):
    with NamedTest("readers_writer_readers", lockfilename):
        enter_order, leave_order = [], []
        await_daemons(
            (Reader, lockfilename, "R1", 0,  3, enter_order, leave_order),
            (Reader, lockfilename, "R2", 2,  2, enter_order, leave_order),
            (Writer, lockfilename, "W1", 1,  2, enter_order, leave_order),
            (Reader, lockfilename, "R3", 3,  4, enter_order, leave_order),
            (Reader, lockfilename, "R4", 4,  6, enter_order, leave_order),
        )
        print(f"{'|':>14} {enter_order=}", flush=True)
        print(f"{'|':>14} {leave_order=}", flush=True)
        assert enter_order[:2] == ["R1", "W1"]
        assert leave_order == ["R1", "W1", "R2", "R3", "R4"]


def reader_writer_timeout(lockfilename):
    with NamedTest("writer_reader_timeout", lockfilename):
        with Twpl(lockfilename).exclusive():
            try:
                Twpl(lockfilename).acquire(CONCURRENT, timeout=2*_SLEEP_FACTOR)
            except TwplTimeoutError:
                pass
            else:
                raise RuntimeError("Expected TwplTimeoutError, got no error")
        dummy = []
        await_daemons(
            (Writer, lockfilename, "W1", 0, 3, dummy, dummy),
            (Reader, lockfilename, "R1", 1, 3, dummy, dummy, 5),
        )
        try:
            await_daemons(
                (Writer, lockfilename, "W1", 0, 3, dummy, dummy),
                (Reader, lockfilename, "R1", 1, 3, dummy, dummy, 1),
            )
        except TwplTimeoutError:
            pass
        else:
            raise RuntimeError("Expected TwplTimeoutError, got no error")
        try:
            await_daemons(
                (Reader, lockfilename, "R2", 0, 10, dummy, dummy),
                (Writer, lockfilename, "W2", 2, 10, dummy, dummy, 1),
            )
        except TwplTimeoutError:
            pass
        else:
            raise RuntimeError("Expected TwplTimeoutError, got no error")


def reader_writer_timeout_contextmanager(lockfilename):
    with NamedTest("reader_writer_timeout", lockfilename):
        with Twpl(lockfilename).concurrent():
            try:
                Twpl(lockfilename).acquire(EXCLUSIVE, timeout=2*_SLEEP_FACTOR)
            except TwplTimeoutError:
                pass
            else:
                raise RuntimeError("Expected TwplTimeoutError, got no error")
        with Twpl(lockfilename).exclusive():
            try:
                Twpl(lockfilename).acquire(CONCURRENT, timeout=2*_SLEEP_FACTOR)
            except TwplTimeoutError:
                pass
            else:
                raise RuntimeError("Expected TwplTimeoutError, got no error")
        with Twpl(lockfilename).exclusive():
            try:
                Twpl(lockfilename).acquire(EXCLUSIVE, timeout=2*_SLEEP_FACTOR)
            except TwplTimeoutError:
                pass
            else:
                raise RuntimeError("Expected TwplTimeoutError, got no error")
        with Twpl(lockfilename).concurrent():
            with Twpl(lockfilename).acquire(CONCURRENT, timeout=_SLEEP_FACTOR):
                pass


if __name__ == "__main__":
    basic_methods("devel/test/basic.lockfile")
    writers("devel/test/writers.lockfile")
    readers("devel/test/readers.lockfile")
    nested_readers("devel/test/nested_readers.lockfile")
    readers_writer_readers("devel/test/rs_w_rs.lockfile")
    reader_writer_timeout("devel/test/timeouts.lockfile")
    reader_writer_timeout_contextmanager("devel/test/timeouts.lockfile")
