#!/usr/bin/env python
from sys import modules
from twpl import Twpl, __version__


print(f"Loaded from {modules['twpl'].__file__!r}")
print(f"{__version__=}")
print(lock := Twpl("devel/test/01.lockfile"))
