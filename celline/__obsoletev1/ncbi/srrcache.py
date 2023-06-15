import os
from typing import Dict, List, Union

import pandas as pd
from pandas import DataFrame

from celline.utils.config import Config


class SRRCache:
    CACHE_PATH = f"{Config.EXEC_ROOT}/run_cache.tsv"
    cache: Union[DataFrame, None] = None

    @staticmethod
    def get(run_id: str) -> Union[Dict[str, List[str]], None]:
        if SRRCache.cache is None:
            if not os.path.isfile(SRRCache.CACHE_PATH):
                df = DataFrame(
                    data=[],
                    columns=["SRR", "GSM", "GSE"],
                    index=[]
                ).to_csv(SRRCache.CACHE_PATH, sep="\t")
            SRRCache.cache = pd.read_csv(
                SRRCache.CACHE_PATH, sep="\t")
        if run_id.startswith("SRR"):
            df = SRRCache.cache[SRRCache.cache["SRR"] == run_id]
        elif run_id.startswith("GSM"):
            df = SRRCache.cache[SRRCache.cache["GSM"] == run_id]
        elif run_id.startswith("GSE"):
            df = SRRCache.cache[SRRCache.cache["GSE"] == run_id]
        else:
            raise KeyError("run_id must be SRR, GSM, or GSE.")
        if len(df.index) == 0:
            return None
        else:
            return {
                "SRR": df["SRR"].tolist(),
                "GSM": df["GSM"].tolist(),
                "GSE": df["GSE"].tolist()
            }

    @staticmethod
    def write(run_id: str, info: Dict[str, List[str]]):
        if SRRCache.get(run_id) is not None:
            return
        if SRRCache.cache is None:
            return
        if ("SRR" in info) & ("GSM" in info) & ("GSE" in info):
            pd.concat(
                [
                    SRRCache.cache,
                    DataFrame(
                        data={
                            "SRR": info["SRR"],
                            "GSM": info["GSM"],
                            "GSE": info["GSE"]
                        }
                    )
                ]
            ).to_csv(SRRCache.CACHE_PATH, sep="\t")
        else:
            raise ValueError()
