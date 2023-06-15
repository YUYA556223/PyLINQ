from argparse import ArgumentParser
from typing import Any, Dict, List, Union

from celline.utils.exceptions import InvalidJobException
from celline.utils.typing import NullableString


class ThreadRunner:
    def __init__(self, argparser: ArgumentParser) -> None:
        argparser.add_argument("-n", "--nthread", type=int)
        self.argparser = argparser
        pass

    def analyze(self, options: List[str]):
        self.nthread: int = self.argparser.parse_args(
            options).nthread
        return
