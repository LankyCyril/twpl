#!/usr/bin/env python
from twpl import Twpl, __version__, EXCLUSIVE, CONCURRENT, UNCONDITIONAL
from sys import modules, stderr
from contextlib import contextmanager
from os import path

print(f"Loaded from {modules['twpl'].__file__}")
print(f"{__version__=}")


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
            assert lock.mode == UNCONDITIONAL
        assert lock.mode is None
    with NamedTest(f"_NOT_IMPLEMENTED"):
        for mode in EXCLUSIVE, CONCURRENT:
            try:
                Twpl(lockfilename).acquire(mode)
            except Exception as e:
                assert isinstance(e, NotImplementedError)
        try:
            Twpl(lockfilename).acquire(UNCONDITIONAL)
        except Exception as e:
            assert isinstance(e, ValueError)
        try:
            Twpl(lockfilename).release()
        except Exception as e:
            assert isinstance(e, NotImplementedError)
    with NamedTest(f".clean({lockfilename!r})"):
        Twpl(lockfilename).clean(min_age_ms=60000)
        assert path.isfile(lockfilename)
        Twpl(lockfilename).clean(min_age_ms=0)
        assert not path.exists(lockfilename)


if __name__ == "__main__":
    basic_methods("devel/test/basic.lockfile")
