#!/usr/bin/env python
from twpl import Twpl, __version__, EXCLUSIVE, CONCURRENT
from sys import modules
from datetime import datetime
from contextlib import contextmanager
from threading import Thread
from time import sleep
from os import path

print(f"Loaded from {modules['twpl'].__file__}")
print(f"{__version__=}")


_SLEEP_FACTOR = .02
ts = lambda: datetime.now().timestamp()


@contextmanager
def NamedTest(name):
    start_ts = ts()
    print(f"TEST STARTED | {name}")
    try:
        yield
    except:
        print(f"{'FAIL!':>12} | {name}")
        print(f"{'`':>14} in {ts()-start_ts}")
        raise
    else:
        print(f"{'SUCCESS':>12} | {name}")
        print(f"{'`':>14} in {ts()-start_ts}")


def await_daemons(*daemon_param_tuples):
    daemons = []
    for target, *args in daemon_param_tuples:
        daemon = Thread(target=target, args=args, daemon=True)
        daemon.start()
        daemons.append(daemon)
    for daemon in daemons:
        daemon.join()


def Reader(lockfilename, name, delay, duration, enter_order, leave_order):
    delay, duration = delay * _SLEEP_FACTOR, duration * _SLEEP_FACTOR
    pfx = f"{'|':>14} Reader {name}"
    print(f"{pfx} will idle for {delay} seconds", flush=True)
    sleep(delay)
    print(f"{pfx} will be reading for {duration} seconds", flush=True)
    with Twpl(lockfilename, poll_ms=10*_SLEEP_FACTOR).concurrent():
        print(f"{pfx} acquired a concurrent lock at {ts()}", flush=True)
        enter_order.append(name)
        sleep(duration)
    print(f"{pfx} done", flush=True)
    leave_order.append(name)


def Writer(lockfilename, name, delay, duration, enter_order, leave_order):
    delay, duration = delay * _SLEEP_FACTOR, duration * _SLEEP_FACTOR
    pfx = f"{'|':>14} Writer {name}"
    print(f"{pfx} will idle for {delay} seconds", flush=True)
    sleep(delay)
    print(f"{pfx} will be writing for {duration} seconds", flush=True)
    with Twpl(lockfilename, poll_ms=10*_SLEEP_FACTOR).exclusive():
        print(f"{pfx} acquired an exclusive lock at {ts()}", flush=True)
        enter_order.append(name)
        sleep(duration)
    print(f"{pfx} done", flush=True)
    leave_order.append(name)


def basic_methods(lockfilename):
    with NamedTest(f".exclusive({lockfilename!r})"):
        with Twpl(lockfilename).exclusive() as lock:
            assert lock.mode == EXCLUSIVE
        assert lock.mode is None
    with NamedTest(f".concurrent({lockfilename!r})"):
        with Twpl(lockfilename).concurrent() as lock:
            assert lock.mode == CONCURRENT
        assert lock.mode is None
    with NamedTest(f".acquire() / .release()"):
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
    with NamedTest(f".clean({lockfilename!r})"):
        assert not Twpl(lockfilename).clean(min_age_ms=60000)
        assert path.isfile(lockfilename)
        assert Twpl(lockfilename).clean(min_age_ms=0)
        assert not path.exists(lockfilename)


def writers(lockfilename):
    with NamedTest(f"writers({lockfilename!r})"):
        order = []
        await_daemons(*(
            (Writer, lockfilename, reader_name, 0, 10, order, order)
            for reader_name in ("R1", "R2", "R3", "R4", "R5")
        ))
        Twpl(lockfilename).clean(min_age_ms=0)
        print(f"{'|':>14} {order=}", flush=True)
        assert order[::2] == order[1::2]


def readers(lockfilename):
    with NamedTest(f"readers({lockfilename!r})"):
        order = []
        await_daemons(*(
            (Reader, lockfilename, reader_name, delay, 10, order, order)
            for delay, reader_name in enumerate(("R1", "R2", "R3", "R4", "R5"))
        ))
        Twpl(lockfilename).clean(min_age_ms=0)
        print(f"{'|':>14} {order=}", flush=True)
        assert order[:5] == order[5:]


def nested_readers(lockfilename):
    with NamedTest(f"nested_readers({lockfilename!r})"):
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
        Twpl(lockfilename).clean(min_age_ms=0)


def readers_writer_readers(lockfilename):
    with NamedTest(f"readers_writer_readers({lockfilename!r})"):
        enter_order, leave_order = [], []
        await_daemons(
            (Reader, lockfilename, "R1", 0,  3, enter_order, leave_order),
            (Reader, lockfilename, "R2", 2,  2, enter_order, leave_order),
            (Writer, lockfilename, "W1", 1,  2, enter_order, leave_order),
            (Reader, lockfilename, "R3", 3,  4, enter_order, leave_order),
            (Reader, lockfilename, "R4", 4,  6, enter_order, leave_order),
        )
        Twpl(lockfilename).clean(min_age_ms=0)
        print(f"{'|':>14} {enter_order=}", flush=True)
        print(f"{'|':>14} {leave_order=}", flush=True)
        assert enter_order[:2] == ["R1", "W1"]
        assert leave_order == ["R1", "W1", "R2", "R3", "R4"]

if __name__ == "__main__":
    basic_methods("devel/test/basic.lockfile")
    writers("devel/test/writers.lockfile")
    readers("devel/test/readers.lockfile")
    nested_readers("devel/test/nested_readers.lockfile")
    readers_writer_readers("devel/test/rs_w_rs.lockfile")
