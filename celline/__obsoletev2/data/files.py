from __future__ import annotations
from celline.config import Config
import yaml
from typing import Dict, List, Union, Optional
import os
import pandas as pd
from pprint import pprint


class FileManager:
    @staticmethod
    def read_accessions() -> Dict[str, Dict]:
        filepath = f"{Config.EXEC_ROOT}/DB/accessions.yaml"
        if os.path.exists(filepath):
            with open(filepath, mode="r") as f:
                return yaml.safe_load(f)
        else:
            return {"SRR": {}, "GSM": {}, "GSE": {}}

    @staticmethod
    def write_accessions(yamldata: Dict):
        if not os.path.isdir(f"{Config.EXEC_ROOT}/DB"):
            os.makedirs(f"{Config.EXEC_ROOT}/DB")
        with open(f"{Config.EXEC_ROOT}/DB/accessions.yaml", mode="w") as f:
            yaml.dump(yamldata, f)

    @staticmethod
    def append_accessions(accessions: Dict):
        existing = FileManager.read_accessions()
        gse = accessions["GSE"]
        if gse["runid"] not in existing["GSE"]:
            existing["GSE"][gse["runid"]] = gse
        for gsmid in accessions["GSM"]:
            if gsmid["runid"] not in existing["GSM"]:
                existing["GSM"][gsmid["runid"]] = gsmid
        for el in accessions["SRR"]:
            for srrid in el:
                if srrid["runid"] not in existing["SRR"]:
                    existing["SRR"][srrid["runid"]] = el
        FileManager.write_accessions(existing)

    @staticmethod
    def read_runs() -> pd.DataFrame:
        filepath = f"{Config.PROJ_ROOT}/runs.tsv"
        if os.path.exists(filepath):
            return pd.read_csv(filepath, sep="\t")
        else:
            return pd.DataFrame(columns=["gsm_id", "sample_name"])

    @staticmethod
    def append_runs(gsm_id: str, sample_name: str):
        filepath = f"{Config.PROJ_ROOT}/runs.tsv"
        runs = FileManager.read_runs()
        if runs.pipe(lambda df: df[df.gsm_id == gsm_id]).index.size == 0:
            runs = pd.concat(
                [
                    runs,
                    pd.DataFrame(
                        index=[0], data={"gsm_id": gsm_id, "sample_name": sample_name}
                    ),
                ]
            )
        runs.set_index("gsm_id").to_csv(filepath, sep="\t")
