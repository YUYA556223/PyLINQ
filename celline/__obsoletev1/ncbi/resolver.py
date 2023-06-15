import os
import re
from typing import List

import pandas as pd
from pandas import DataFrame

from celline.utils.config import Config


class RuntableResolver:
    @staticmethod
    def has_error(runtable: DataFrame, target_runid: str):
        if target_runid.startswith("SRR"):
            identity = "run_id"
        elif target_runid.startswith("GSM"):
            identity = "gsm_id"
        else:
            return True
        return (
            runtable[runtable[identity] == target_runid]["dumped_filepath"]
            .str.contains("None")
            .any()
        )

    @staticmethod
    def get_all_error_dfs(runtable: DataFrame) -> List[str]:
        error_dfs = runtable[
            runtable["dumped_filepath"].str.contains("None")
        ].drop_duplicates()
        if error_dfs.size == 0:
            return []
        return error_dfs["gsm_id"].drop_duplicates().tolist()

    @staticmethod
    def validate(runtable: DataFrame):
        gsms = RuntableResolver.get_all_error_dfs(runtable)
        for gsm in gsms:
            print(f"[ERROR] Error detected in {gsm}. None data appearing")
        if len(gsms) != 0:
            quit()

    @staticmethod
    def fix(runtable: DataFrame):
        errors = runtable[runtable["filetype"] == "fastq"]
        error_srrs = errors.drop_duplicates("run_id")["run_id"].tolist()
        print("***** SRA Run Fixer *****")
        for index in range(len(error_srrs)):
            srr_id = error_srrs[index]
            gsm_id: str = runtable[runtable["run_id"]
                                   == srr_id]["gsm_id"].iloc[0]
            print(
                f"""--------------------------------------------
┏━ [{index+1}/{len(error_srrs)}] Fix errors on {srr_id}.━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┣━ Refer to [https://trace.ncbi.nlm.nih.gov/Traces/index.html?view=run_browser&acc={srr_id}&display=data-access]"""
            )
            target_laneid: str = errors[errors["run_id"]
                                        == srr_id]["lane_id"].iloc[0]
            req_correct: bool = pd.isna(target_laneid)  #
            target_laneid: str = str(target_laneid)
            if not target_laneid.startswith("L"):
                req_correct = True
            if req_correct:
                print(f"┣━ ERROR in lane id")
                exists_laneID: List[str] = [
                    x
                    for x in runtable[runtable["gsm_id"] == gsm_id]["lane_id"]
                    .unique()
                    .tolist()
                    if x != "nan"
                ]
                lane_id = "L{:0=3}".format(len(exists_laneID))
                runtable.loc[runtable["run_id"] == srr_id, "lane_id"] = lane_id
                print(f"┣━ ┗> fixed lane_id automatically to: {lane_id}")
            read_types: List[str] = errors[errors["run_id"] == srr_id][
                "read_type"
            ].tolist()
            req_correct: bool = False
            for read_type in read_types:
                __req_correct = (
                    (read_type == "nan") | (
                        read_type == "") | (pd.isna(read_type))
                )
                if not __req_correct:
                    __req_correct = (
                        re.match(pattern=r"[RI][12]", string=read_type) is None
                    )
                if __req_correct:
                    req_correct = True
                    break
            if req_correct:
                print(f"┣━ ERROR in read type.")
                if len(read_types) == 1:
                    print(
                        "┣━ ┗> read_type should minimally contains R1 and R2. Delete this column."
                    )
                    print(f"┣━ ┗> {read_types}")
                    # runtable.drop(runtable[runtable['run_id'] == srr_id].index, axis=1)
                elif len(read_types) == 2:
                    print("┣━ ┗> fixed read_type automatically to: R1 and R2")
                    runtable.loc[runtable["run_id"] == srr_id, "read_type"] = [
                        "R1",
                        "R2",
                    ]
                elif len(read_types) == 3:
                    print("┣━ ┣━ Detected index file. Which one is index file?")
                    targets: List[str] = [
                        filename.split("/")[-1]
                        for filename in runtable[runtable["run_id"] == srr_id][
                            "cloud_filepath"
                        ].tolist()
                    ]
                    print(
                        f"""
        ┣━ ┣━ ┏ 1) {targets[0]}
        ┣━ ┣━ ┣ 2) {targets[1]}
        ┣━ ┣━ ┗ 3) {targets[3]}
        """
                    )
                    new_read_type: List[str] = []
                    while True:
                        lindex = input("1, 2, 3")
                        try:
                            lindex = int(lindex, 10)
                            if lindex <= 3 & lindex >= 1:
                                if lindex == 1:
                                    new_read_type = ["L1", "R1", "R2"]
                                elif lindex == 2:
                                    new_read_type = ["R1", "L1", "R2"]
                                else:
                                    new_read_type = ["R1", "R2", "L1"]
                                break
                            else:
                                print("Specify an integer value between 1 and 3.")
                        except ValueError:
                            print("Specify an integer value between 1 and 3.")
                    runtable.loc[
                        runtable["run_id"] == srr_id, "read_type"
                    ] = new_read_type
            for index in runtable[runtable["run_id"] == srr_id].index.tolist():
                result = runtable[runtable.index == index].iloc[0]
                dumped_filename = f'{result["gsm_id"]}_S1_{result["lane_id"]}_{result["read_type"]}_001.fastq.gz'
                dumped_filepath = f'{result["sample_name"]}/0_dumped/{result["gsm_id"]}/fastqs/{dumped_filename}'
                runtable.loc[
                    runtable.index == index, "dumped_filename"
                ] = dumped_filename
                runtable.loc[
                    runtable.index == index, "dumped_filepath"
                ] = dumped_filepath
            print(
                f"┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛"
            )
        return runtable
