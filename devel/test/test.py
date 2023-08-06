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


_SLEEP_FACTOR = .03


def ts():
    return datetime.now().timestamp()


@contextmanager
def NamedTest(name):
    start_ts = ts()
    print(f"TEST STARTED | {name}", file=stderr)
    try:
        yield
    except:
        print(f"{'FAIL!':>12} | {name}", file=stderr)
        print(f"{'`':>14} in {ts()-start_ts}", file=stderr)
        raise
    else:
        print(f"{'SUCCESS':>12} | {name}", file=stderr)
        print(f"{'`':>14} in {ts()-start_ts}", file=stderr)


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
    with NamedTest(f"_NOT_IMPLEMENTED"):
        # TODO: they're implemented now - btw, this still passes because
        # this block has never checked for "no exceptions"
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


def readers(lockfilename):
    with NamedTest(f"readers({lockfilename!r})"):
        start_ts, dummy = ts(), []
        await_daemons(*(
            (Reader, lockfilename, reader_name, 0, 5, dummy, dummy)
            for reader_name in ("R1", "R2", "R3", "R4", "R5")
        ))
        Twpl(lockfilename).clean(min_age_ms=0)
        assert (ts() - start_ts) < 10 * _SLEEP_FACTOR
        # note: this assertion may fail if _SLEEP_FACTOR is really low and the
        # hardware doesn't keep up


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


def writers(lockfilename):
    with NamedTest(f"writers({lockfilename!r})"):
        enter_order, leave_order = [], []
        await_daemons(*(
            (Writer, lockfilename, writer_name, 0, 2, enter_order, leave_order)
            for writer_name in (f"W{i}" for i in range(1, 10))
        ))
        Twpl(lockfilename).clean(min_age_ms=0)
        assert enter_order == leave_order


def readers_writer_readers(lockfilename):
    with NamedTest(f"readers_writer_readers({lockfilename!r})"):
        enter_order, leave_order = [], []
        await_daemons(
            (Reader, lockfilename, "R1", 0,  4, enter_order, leave_order),
            (Reader, lockfilename, "R2", 1,  2, enter_order, leave_order),
            (Writer, lockfilename, "W1", .2, 2, enter_order, leave_order),
            (Reader, lockfilename, "R3", 5,  4, enter_order, leave_order),
            (Reader, lockfilename, "R4", 5,  6, enter_order, leave_order),
        )
        Twpl(lockfilename).clean(min_age_ms=0)
        assert enter_order[:2] == ["R1", "W1"]
        assert leave_order == ["R1", "W1", "R2", "R3", "R4"]

if __name__ == "__main__":
    basic_methods("devel/test/basic.lockfile")
    readers("devel/test/readers.lockfile")
    nested_readers("devel/test/nested_readers.lockfile")
    writers("devel/test/writers.lockfile")
    readers_writer_readers("devel/test/rs_w_rs.lockfile")
