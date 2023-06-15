from argparse import ArgumentParser
from typing import Any, Dict, List, Union

from celline.utils.exceptions import InvalidJobException
from celline.utils.typing import NullableString


class SampleRunner:
    def __init__(self, argparser: ArgumentParser) -> None:
        argparser.add_argument("-s", "--samplename", type=str)
        self.argparser = argparser
        pass

    def analyze(self, options: List[str]):
        self.default_sample_name: NullableString = self.argparser.parse_args(
            options).samplename
        return
