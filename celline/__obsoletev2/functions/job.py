from celline.functions._base import CellineFunction
from celline.plugins.collections.generic import DictionaryC, ListC
from typing import Optional, List, Dict
from celline.config import Config
import shutil


class Job(CellineFunction):
    def register(self) -> str:
        return "job"

    def on_call(self, args: Dict[str, DictionaryC[str, Optional[str]]]):
        command = args["options"]["req_1"]
        if command is None:
            print("[ERROR] Please specify target command.")
            quit()
        if command == "clear":
            shutil.rmtree(f"{Config.PROJ_ROOT}/jobs/")
        else:
            print(f"[ERROR] Unknown command: {command}")
            quit()
