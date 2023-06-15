import os
from typing import Any, Dict

import toml  # type: ignore

from celline.utils.typing import NullableString


class Config:
    EXEC_ROOT: str
    PROJ_ROOT: str

    @staticmethod
    def initialize(exec_root_path: str, proj_root_path: NullableString):
        Config.EXEC_ROOT = exec_root_path
        if proj_root_path is None:
            return
        if not os.path.isfile(f"{proj_root_path}/setting.toml"):
            raise FileNotFoundError(
                "Could not find setting.toml. Please create or initialize your project.")
        Config.PROJ_ROOT = proj_root_path


class Setting:
    # model variables
    name: str
    version: float
    wait_time: int
    r_path: str

    @staticmethod
    def validate():
        if not os.path.isfile(f"{Config.PROJ_ROOT}/setting.toml"):
            raise FileNotFoundError(
                "Could not find setting file in your project.")

    @staticmethod
    def as_dict():
        return {
            "project": {
                "name": Setting.name,
                "version": Setting.version
            },
            "fetch": {
                "wait_time": Setting.wait_time
            },
            "R": {
                "r_path": Setting.r_path
            }
        }

    @staticmethod
    def as_cfg_obj(dict: Dict[str, Any]):
        Setting.name = dict["project"]["name"]
        Setting.version = dict["project"]["version"]
        Setting.wait_time = dict["fetch"]["wait_time"]
        Setting.r_path = dict["R"]["r_path"]

    @staticmethod
    def initialize():
        Setting.validate()
        with open(f"{Config.PROJ_ROOT}/setting.toml", mode="r") as f:
            Setting.as_cfg_obj(toml.load(f))

    @staticmethod
    def flush():
        with open(f"{Config.PROJ_ROOT}/setting.toml", mode="w") as f:
            toml.dump(Setting.as_dict(), f)
