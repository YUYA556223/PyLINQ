from celline.functions._base import CellineFunction
from celline.plugins.collections.generic import DictionaryC, ListC
from typing import Optional, List, Dict
from celline.database import NCBI, GSM
from celline.utils.project import DirectoryManager


class Test(CellineFunction):
    def register(self) -> str:
        return "test"

    def on_call(self, args: Dict[str, DictionaryC[str, Optional[str]]]):
        options = args["options"]
        id = options["req_1"]
        if id is not None:
            print(DirectoryManager.is_dumped(id))
        return
