import argparse
from argparse import ArgumentParser
from typing import Callable


class JobController:
    """
    Controls job system
    """

    def __init__(self, argparser: ArgumentParser) -> None:
        argparser.add_argument('-j', '--job', type=str)
        self.argparser = argparser
        pass

    def on_call(self, func: Callable):
        func()
        return
