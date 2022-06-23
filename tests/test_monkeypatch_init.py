import sys
import os
import traceback
import inspect
from types import FrameType
from typing import List, cast

class Foo():
    def __init__(self, a, b):
        self.a = a
        self.b = b

    def add(self):
        return self.a + self.b

foo = Foo(1, 2)
print(foo.add())

def monkeypatch_init(self, a, b):
    self.a = a
    self.b = b

    print('logging!: ', a, b)
    print(*get_callstack(3), sep='\n')

Foo.__init__ = monkeypatch_init

def my_method():
    foo = Foo(2, 3)
    print(foo.add())

my_method()