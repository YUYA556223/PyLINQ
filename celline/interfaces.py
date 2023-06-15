from __future__ import annotations
from celline.config import Config, Setting
import os
import subprocess

# from celline.database import NCBI, GSE, GSM
from typing import Any, Callable, Generic, List, TypeVar, Union, Dict, Tuple, Final
from celline.functions._base import CellineFunction


class Project:
    """
    Celline project
    """

    EXEC_PATH: Final[str]
    PROJ_PATH: Final[str]
    __nthread: int

    def __init__(self, project_dir: str, proj_name: str = "", r_path: str = "") -> None:
        """
        Load new celline project
        """

        def get_r_path() -> str:
            proc = subprocess.Popen("which R", stdout=subprocess.PIPE, shell=True)
            result = proc.communicate()
            return result[0].decode("utf-8").replace("\n", "")

        def get_default_proj_name() -> str:
            return os.path.basename(project_dir)

        self.EXEC_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        self.PROJ_PATH = project_dir
        if not os.path.isfile(f"{self.PROJ_PATH}/setting.toml"):
            Setting.name = get_default_proj_name() if proj_name == "" else proj_name
            Setting.r_path = get_r_path() if r_path == "" else r_path
            Setting.version = "0.01"
            Setting.wait_time = 4
            Setting.flush()

    @property
    def nthread(self) -> int:
        return self.__nthread

    def call(self, func: CellineFunction):
        func.call(self)
        return self

    def call_if_else(
        self,
        condition: Callable[[Project], bool],
        true: CellineFunction,
        false: CellineFunction,
    ):
        """Call function if"""
        if condition(self):
            true.call(self)
        else:
            false.call(self)
        return self

    def parallelize(self, nthread: int):
        """
        Starts parallel computation\n
        @ nthread<int>: Number of thread
        """
        self.__nthread = nthread
        return self

    def singularize(self):
        return self

    def start_logging(self):
        return self

    def end_logging(self):
        return self

    def if_else(
        self,
        condition: Callable[["Project"], bool],
        true: Callable[["Project"], None],
        false: Callable[["Project"], None],
    ):
        if condition(self):
            true(self)
        else:
            false(self)
        return self

    def execute(self, usePBS: bool, max_nthread: int):
        print("Execution started")
        return

    # def observable(self) -> Observable:
    #     return Observable(self)
