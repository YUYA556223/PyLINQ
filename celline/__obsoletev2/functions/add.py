from celline.functions._base import CellineFunction
from celline.plugins.collections.generic import DictionaryC, ListC
from typing import Optional, List, Dict
from celline.database import NCBI, GSE, GSM
import asyncio


class Add(CellineFunction):
    def register(self) -> str:
        return "add"

    def on_call(self, args: Dict[str, DictionaryC[str, Optional[str]]]):
        options = args["options"]
        id = options["req_1"]
        if id is not None:
            NCBI.add(id)

            # result = NCBI.search("GSE")
            # if id.startswith("GSE"):
            #     gse: GSE = result
            #     append_runs(gse.child_gsm_ids, "TEST")
            # elif id.startswith("GSM"):
            #     gsm: List[GSM] = result
            #     append_runs(gse.child_gsm_ids)
        return
