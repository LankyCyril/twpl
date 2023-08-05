#!/usr/bin/env python
from twpl import Twpl, __version__, EXCLUSIVE, CONCURRENT
from sys import modules, stderr
from datetime import datetime
from contextlib import contextmanager
from threading import Thread
from time import sleep
from os import path

print(f"Loaded from {modules['twpl'].__file__}")
print(f"{__version__=}")


def ts():
    return datetime.now().timestamp()


@contextmanager
def NamedTest(name):
    print(f"TEST STARTED | {name}", file=stderr)
    try:
        yield
    except:
        print(f"{'FAIL!':>12} | {name}", file=stderr)
        raise
    else:
        print(f"{'SUCCESS':>12} | {name}", file=stderr)


def await_daemons(*daemon_param_tuples):
    daemons = []
    for target, *args in daemon_param_tuples:
        daemon = Thread(target=target, args=args, daemon=True)
        daemon.start()
        daemons.append(daemon)
    for daemon in daemons:
        daemon.join()


def Reader(lockfilename, name, delay, duration, enter_order, leave_order):
    delay, duration = delay / 10, duration / 10
    pfx = f"{'|':>14} Reader {name}"
    print(f"{pfx} will idle for {delay} seconds", flush=True)
    sleep(delay)
    print(f"{pfx} will be reading for {duration} seconds", flush=True)
    with Twpl(lockfilename).concurrent():
        print(f"{pfx} acquired a concurrent lock at {ts()}", flush=True)
        enter_order.append(name)
        sleep(duration)
    print(f"{pfx} done", flush=True)
    leave_order.append(name)


def Writer(lockfilename, name, delay, duration, enter_order, leave_order):
    delay, duration = delay / 10, duration / 10
    pfx = f"{'|':>14} Writer {name}"
    print(f"{pfx} will idle for {delay} seconds", flush=True)
    sleep(delay)
    print(f"{pfx} will be writing for {duration} seconds", flush=True)
    with Twpl(lockfilename).exclusive(poll_ms=10):
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
    with NamedTest(f".unconditional({lockfilename!r})"):
        with Twpl(lockfilename).unconditional() as lock:
            assert lock.mode is None
        assert lock.mode is None
    with NamedTest(f"_NOT_IMPLEMENTED"):
        for mode in EXCLUSIVE, CONCURRENT:
            try:
                Twpl(lockfilename).acquire(mode)
            except Exception as e:
                assert isinstance(e, NotImplementedError)
        try:
            Twpl(lockfilename).release()
        except Exception as e:
            assert isinstance(e, NotImplementedError)
    with NamedTest(f".clean({lockfilename!r})"):
        assert not Twpl(lockfilename).clean(min_age_ms=60000)
        assert path.isfile(lockfilename)
        assert Twpl(lockfilename).clean(min_age_ms=0)
        assert not path.exists(lockfilename)


def readers_writer_readers(lockfilename):
    with NamedTest(f"readers_writer_readers({lockfilename!r})"):
        enter_order, leave_order = [], []
        await_daemons(
            (Reader, lockfilename, "R1", 0,  4, enter_order, leave_order),
            (Reader, lockfilename, "R2", 1,  1, enter_order, leave_order),
            (Writer, lockfilename, "W1", .2, 2, enter_order, leave_order),
            (Reader, lockfilename, "R3", 5,  2, enter_order, leave_order),
            (Reader, lockfilename, "R4", 5,  3, enter_order, leave_order),
        )
        Twpl(lockfilename).clean(min_age_ms=0)
        assert enter_order[:2] == ["R1", "W1"]
        assert leave_order == ["R1", "W1", "R2", "R3", "R4"]

if __name__ == "__main__":
    basic_methods("devel/test/basic.lockfile")
    readers_writer_readers("devel/test/rs_w_rs.lockfile")
