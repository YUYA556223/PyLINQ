import math
import os
import re
import shutil
import subprocess
from time import time
from typing import Dict, List

import pandas as pd  # type: ignore
from pandas import DataFrame, Series
from pysradb.sraweb import SRAweb  # type: ignore
from requests_html import AsyncHTMLSession, HTMLResponse  # type: ignore
from tqdm import tqdm  # type: ignore

from celline.jobs.jobs import Jobs, JobSystem  # type: ignore
from celline.ncbi.genome import Genome
from celline.ncbi.resolver import RuntableResolver
from celline.utils.config import Config, Setting
from celline.utils.directory import Directory, DirectoryType
from celline.utils.exceptions import (InvalidDataFrameHeaderException,
                                      InvalidServerNameException,
                                      NCBIException)
from celline.utils.loader import Loader
from celline.utils.typing import NullableString


class _SRR:
    dumped_filepath: str = ""
    dumped_filename: str = ""
    cloud_filepath: str = ""
    raw_filename: str = ""
    sample_name: str = ""
    sample_id: NullableString = None
    lane_id: NullableString = None
    read_type: NullableString = None
    replicate: int
    """
    Read type of each lane, structually defined R1, R2, I1, I2
    """
    run_id: str = ""
    gsm_id: str = ""
    gse_id: str = ""
    egress: str = "-"
    filetype: str = ""
    spieces: str = ""
    location: str = ""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        pass


class SRR:
    def __init__(self) -> None:
        pass

    SRADB = SRAweb()

    @staticmethod
    def get_runtable():
        file = f"{Config.PROJ_ROOT}/runs.tsv"
        if os.path.isfile(file):
            return pd.read_csv(file, sep="\t")
        else:
            return None

    @staticmethod
    async def __fetch(run_id: str, visualize=True):
        """
        Fetch target sra run data
        """
        if visualize:
            thread_loader = Loader()
            thread_loader.delay = 0.15
            thread_loader.format = "{SYMBOL}  {STATUS}"
            thread_loader.start_loading(f"⚙️ Fetching {run_id}...")
        else:
            thread_loader = None
        session = AsyncHTMLSession()
        r: HTMLResponse = await session.get(
            f"https://trace.ncbi.nlm.nih.gov/Traces/index.html?view=run_browser&acc={run_id}&display=data-access"
        )  # type: ignore
        await r.html.arender(
            wait=Setting.wait_time / 2, sleep=int(Setting.wait_time / 2)
        )
        if thread_loader is not None:
            thread_loader.stop_loading(status="Finished fetching")
        return r

    @staticmethod
    def __build(
        response: HTMLResponse,
        run_id: str,
        sample_name: str,
        repid: int,
        use_interactive=True,
        visualize=True,
    ):
        if visualize:
            thread_loader = Loader()
            thread_loader.delay = 0.15
            thread_loader.format = "{SYMBOL}  {STATUS}"
            thread_loader.start_loading(f"⚙️ Building {run_id}...")
        else:
            thread_loader = None

        def get_headers() -> List[str]:
            headers = response.html.find(
                # type: ignore
                "#ph-run-browser-data-access > div:nth-child(2) > table > thead > tr"
            )[0].text.split("\n")
            return headers[2: len(headers)]

        def build_run_table():
            num = 1
            all_data: List[List[str]] = []
            while True:
                data = response.html.find(
                    f"#ph-run-browser-data-access > div:nth-child(2) > table > tbody > tr:nth-child({num})"
                )
                if len(data) == 0:  # type: ignore
                    break
                data = data[0].text.split("\n")  # type: ignore
                data = data[len(data) - 4: len(data)]
                if num == 1:
                    all_data = [data]
                else:
                    all_data.append(data)
                num += 1
            return DataFrame(all_data, columns=get_headers())

        runtable = build_run_table()

        def get_filetype() -> str:
            # Define filetype
            cloud_filepath: str = str(runtable["Name"][0])
            if (
                (re.search(".fastq.gz", cloud_filepath) is not None)
                | (re.search(".fq.gz", cloud_filepath) is not None)
                | (re.search(".fastq", cloud_filepath) is not None)
                | (re.search(".fq", cloud_filepath) is not None)
            ):
                return "fastq"
            elif re.search(".bam", cloud_filepath) is not None:
                return "bam"
            else:
                return "unknown"

        filetype = get_filetype()

        def get_location() -> str:
            # Decide location
            if filetype == "fastq":
                return runtable["Location"].unique()[0]
            else:
                locations = runtable[runtable["Free Egress"] == "worldwide"][
                    "Location"
                ].unique()
                if len(locations) == 0:
                    raise NCBIException(
                        "[ERROR] Detected non-fastq filetype (bam?), but could not access file. (Not permitted anonymous access?)"
                    )
                return locations[0]

        location = get_location()

        def get_sizeGB() -> DataFrame:
            num = 1
            size_data: List[float] = []
            while True:
                rae_size_el = response.html.find(
                    f"#ph-run-browser-data-access > div:nth-child(2) > table > tbody > tr:nth-child({num}) > td:nth-child(2)"
                )
                if len(rae_size_el) == 0:  # type: ignore
                    break
                raw_size = rae_size_el[0].text  # type: ignore
                tb = re.search("T", raw_size)
                gb = re.search("G", raw_size)
                mb = re.search("M", raw_size)
                kb = re.search("K", raw_size)
                if tb is not None:
                    size = float(raw_size[0: tb.span()[0]]) * 1024
                elif gb is not None:
                    size = float(raw_size[0: gb.span()[0]])
                elif mb is not None:
                    size = float(raw_size[0: mb.span()[0]]) / 1024
                elif kb is not None:
                    size = float(raw_size[0: kb.span()[0]]) / (1024 ^ 2)
                else:
                    raise ValueError(
                        f"Could not convert size data: {raw_size}")
                size_data.append(size)
                num += 2
            return DataFrame({"size": size_data})

        runtable = runtable[runtable["Location"] == location].reset_index()
        del runtable["index"]

        def get_GSM_spieces() -> List[str]:
            metainfo = response.html.find("#ph-run_browser > h1")[0].text.split(  # type: ignore
                ";"
            )
            return [metainfo[0].split(": ")[0], metainfo[1].replace(" ", "")]

        gsm_spieces = get_GSM_spieces()
        runtable["GSM"] = gsm_spieces[0]
        runtable["GSE"] = SRR.SRADB.gsm_to_gse(gsm_spieces[0])[
            "study_alias"][0]
        runtable["spieces"] = gsm_spieces[1]

        def build_SRR(series: Series, run_id: str, use_interactive: bool) -> _SRR:
            def get_sampleid(cloud_file_name: str, filetype: str):
                # search_result = re.search("_S", cloud_file_name)
                # if filetype == "fastq":
                #     if search_result is not None:
                #         sampleid = cloud_file_name[search_result.span()[1]]
                #     else:
                #         sampleid = "1"
                #     return sampleid
                # else:
                #     return None
                return "1"

            def get_laneid(cloud_file_name: str, filetype: str, interactive: bool):
                if filetype == "fastq":
                    current_runtable = SRR.get_runtable()
                    if current_runtable is None:
                        return "L001"
                    gsm_id = gsm_spieces[0]
                    exists_laneID: List[str] = [
                        x
                        for x in current_runtable[current_runtable["gsm_id"] == gsm_id][
                            "lane_id"
                        ]
                        .unique()
                        .tolist()
                        if x != "nan"
                    ]
                    lane_id = "L{:0=3}".format(len(exists_laneID) + 1)
                    return lane_id
                else:
                    return None

            def get_readtype(cloud_file_name: str, filetype: str, interactive: bool):
                if filetype == "fastq":
                    search_result_R = re.search("_R", cloud_file_name)
                    if search_result_R is None:
                        search_result_R = re.search(".R", cloud_file_name)
                    if search_result_R is not None:
                        index = search_result_R.span()[1]
                        idnum = f"R{cloud_file_name[index]}"
                        return idnum
                    else:
                        search_result_I = re.search("_I", cloud_file_name)
                        if search_result_I is None:
                            search_result_I = re.search(".I", cloud_file_name)
                        # suspect index
                        if search_result_I is not None:
                            index = search_result_I.span()[1]
                            idnum = f"I{cloud_file_name[index]}"
                            return idnum
                        else:
                            return None
                else:
                    return None

            def get_raw_filename(
                cloud_file_path: str,
                read_type: NullableString,
                run_id: str,
                filetype: str,
            ):
                raw_fname = cloud_file_path.split("/")[-1]
                if filetype == "fastq":
                    if read_type == "R1":
                        return f"{run_id}_1.fastq.gz"
                    elif read_type == "R2":
                        return f"{run_id}_2.fastq.gz"
                    elif read_type == "I1":
                        return f"{run_id}_3.fastq.gz"
                    elif read_type == "I2":
                        return f"{run_id}_4.fastq.gz"
                    else:
                        return raw_fname
                elif filetype == "bam":
                    return raw_fname
                else:
                    return raw_fname

            def build_dumped_filename(
                gsm_id: str,
                filetype: str,
                sample_id: NullableString,
                lane_id: NullableString,
                read_type: NullableString,
            ):
                if filetype == "fastq":
                    return f"{gsm_id}_S{sample_id}_{lane_id}_{read_type}_001.fastq.gz"
                elif filetype == "bam":
                    return f"{gsm_id}.bam"
                else:
                    return f"{gsm_id}.unknown"

            srr = _SRR(run_id)
            srr.gsm_id = str(series["GSM"])
            srr.gse_id = str(series["GSE"])
            current = SRR.get_runtable()
            if current is not None:
                exists_samplename = current[current["gsm_id"] == srr.gsm_id][
                    "sample_name"
                ].tolist()
                if len(exists_samplename) > 0:
                    if sample_name != exists_samplename[0]:
                        print(
                            f"\n   [Error] Duplicated name on same GSM ID is not allowed.\n   Old: {exists_samplename[0]} <-> New: {sample_name}"
                        )
                        if thread_loader is not None:
                            thread_loader.stop_loading(
                                status="Aborted building", failed=True
                            )
                        quit()
            srr.sample_name = sample_name
            srr.spieces = str(series["spieces"])
            # Assign parametres
            srr.cloud_filepath = str(series["Name"])
            srr.filetype = filetype
            srr.location = location
            cloud_file_name: str = srr.cloud_filepath.split("/")[-1]
            srr.sample_id = get_sampleid(cloud_file_name, filetype)
            srr.lane_id = get_laneid(
                cloud_file_name=cloud_file_name,
                filetype=filetype,
                interactive=use_interactive,
            )
            srr.read_type = get_readtype(
                cloud_file_name=cloud_file_name,
                filetype=filetype,
                interactive=use_interactive,
            )
            srr.egress = str(series["Free Egress"][0])
            srr.raw_filename = get_raw_filename(
                cloud_file_path=cloud_file_name,
                read_type=srr.read_type,
                run_id=run_id,
                filetype=filetype,
            )
            srr.dumped_filename = build_dumped_filename(
                gsm_id=srr.gsm_id,
                filetype=srr.filetype,
                sample_id=srr.sample_id,
                lane_id=srr.lane_id,
                read_type=srr.read_type,
            )
            srr.replicate = repid
            if srr.filetype == "fastq":
                srr.dumped_filepath = f"{srr.sample_name}/0_dumped/{srr.gsm_id}/fastqs/{srr.dumped_filename}"
            elif srr.filetype == "bam":
                srr.dumped_filepath = f"{srr.sample_name}/0_dumped/{srr.gsm_id}/bams/{srr.dumped_filename}"
            else:
                raise NCBIException("Unrecognized filetype")
            return srr

        results: Dict[str, _SRR] = {}
        for col_n in range(len(runtable.index)):
            srr = build_SRR(
                series=runtable[runtable.index == col_n].iloc[0],
                run_id=run_id,
                use_interactive=use_interactive,
            )
            results[srr.cloud_filepath] = srr
        if thread_loader is not None:
            thread_loader.stop_loading(status="Finished building")
        return results

    @staticmethod
    def get_sample_name(visualize=True):
        while True:
            sample_name_str: str = input("Sample name? ")
            if sample_name_str != "":
                break
            else:
                print("Empty sample name is not allowed")
        if visualize:
            print(f"\033[32m✓\033[0m  Sample name: {sample_name_str}")
        return sample_name_str

    @staticmethod
    async def __add(
        run_id: str,
        default_sample_name: str,
        repid: int,
        use_interactive=True,
        visualize=True,
    ):

        run_path = f"{Config.PROJ_ROOT}/runs.tsv"
        if os.path.isfile(run_path):
            current = pd.read_csv(run_path, sep="\t")
        else:
            current = DataFrame()
        # Fetch response via fetch function.
        response = await SRR.__fetch(run_id, visualize)
        # Get runs
        runs = SRR.__build(
            response=response,
            run_id=run_id,
            sample_name=default_sample_name,
            repid=repid,
            use_interactive=use_interactive,
            visualize=visualize,
        )
        compiled = {}
        for run in runs.keys():
            compiled[run] = vars(runs[run])
        df = pd.concat([current, DataFrame(compiled).T])
        df = df.drop_duplicates(subset="cloud_filepath", keep="last")
        df.set_index("dumped_filepath", inplace=True)
        df.to_csv(f"{Config.PROJ_ROOT}/runs.tsv", sep="\t")
        if visualize:
            print(f"\033[32m✓\033[0m  Finished writing runs.")

    @staticmethod
    async def add(
        run_id: str,
        default_sample_name: NullableString = None,
        use_interactive=True,
        visualize=True,
        update=False,
    ):
        """
        Auto-fetch and write the given run_id information. fastq file or bam will be added.

         Parameters
         ----------
         run_id : str
             SRA Run ID or GSM ID
         default_sample_name : NullableString[str or None]
             Sample name in the given run ID, or interactively enter a sample name if None is assigned.
         use_interactive: bool = True
             Use interactive interface?
         visualize: bool = True
             Visualize terminal interface?
        """
        runtable = SRR.get_runtable()
        if runtable is not None:
            if not update:
                if run_id.startswith("SRR"):
                    if run_id in runtable["run_id"].tolist():
                        print(f"[INFO] {run_id} is already exist. Ignore.")
                        return
                elif run_id.startswith("GSM"):
                    if run_id in runtable["gsm_id"].tolist():
                        print(f"[INFO] {run_id} is already exist. Ignore.")
                        return
        if default_sample_name is None:
            sample_name = SRR.get_sample_name(visualize)
        else:
            sample_name = default_sample_name
        if visualize:
            thread_loader = Loader()
            thread_loader.delay = 0.15
            thread_loader.format = "{SYMBOL}  {STATUS}"
            thread_loader.start_loading(f"⚙️ Fetching GSM information...")
        else:
            thread_loader = None
        if run_id.startswith("SRR"):
            ids = SRR.SRADB.srr_to_gsm(run_id)["run_accession"].tolist()
            if thread_loader is not None:
                thread_loader.stop_loading(status="Finished fetching GSM.")
            await SRR.__add(
                run_id=run_id,
                default_sample_name=sample_name,
                repid=ids.index(run_id),
                use_interactive=use_interactive,
                visualize=visualize,
            )
        elif run_id.startswith("GSM"):
            try:
                ids = SRR.SRADB.gsm_to_srr(run_id)["run_accession"].tolist()
            except:
                print(
                    f"[WARNING] Fetching process encounted error, ignore.: {run_id}")
                ids = None
            if thread_loader is not None:
                thread_loader.stop_loading(status="Finished fetching GSM.")
            if ids is not None:
                cnt = 0
                for target_id in ids:
                    await SRR.__add(
                        run_id=target_id,
                        default_sample_name=sample_name,
                        repid=cnt,
                        use_interactive=use_interactive,
                        visualize=visualize,
                    )
                    cnt += 1
        else:
            if thread_loader is not None:
                thread_loader.stop_loading(status="Failed.")
            print(f"[ERROR] Could not detect accession type of {run_id}")
        if visualize:
            runtable = SRR.get_runtable()
            if runtable is not None:
                runtable = RuntableResolver.fix(runtable)
                runtable.set_index("dumped_filepath", inplace=True)
                runtable.to_csv(f"{Config.PROJ_ROOT}/runs.tsv", sep="\t")

    @staticmethod
    async def add_range(run_list_path: str):
        """
        Auto-fetch and write the given run_id information in the SRA Run list file (PROJ_ROOT/runs.tsv).

         Parameters
         ----------
         run_list_path : str
             SRA Run list path
        """
        COLUMS = ["SRR_ID", "sample_name"]
        if not os.path.isfile(run_list_path):
            df = DataFrame(columns=COLUMS, index=None)
            df.set_index("SRR_ID", inplace=True)
            df.to_csv(run_list_path, sep="\t")
            raise FileNotFoundError(
                f"Could not find SRR list in your project. Please write run list to {run_list_path}.\n(We prepared SRR_list template :))"
            )
        runs = SRR.get_runtable()
        run_list = pd.read_csv(run_list_path, sep="\t")
        if not run_list.columns.tolist() == COLUMS:
            raise InvalidDataFrameHeaderException(
                f"Header must contains {COLUMS}")
        if runs is not None:
            all_srr = runs["run_id"].unique().tolist()
            run_list = run_list[~run_list["SRR_ID"].isin(
                all_srr)].reset_index()
        all_len = len(run_list["SRR_ID"])

        if all_len != 0:
            bar = tqdm(total=all_len)
            for i in range(all_len):
                target = run_list[run_list.index == i]
                srrID: str = target["SRR_ID"].unique()[0]
                sampleName: str = target["sample_name"].unique()[0]
                await SRR.add(
                    run_id=srrID,
                    default_sample_name=sampleName,
                    use_interactive=False,
                    visualize=False,
                )
                bar.update(1)
            bar.clear()
        runtable = SRR.get_runtable()
        if runtable is not None:
            runtable = RuntableResolver.fix(runtable)
            runtable.set_index("dumped_filepath", inplace=True)
            runtable.to_csv(f"{Config.PROJ_ROOT}/runs.tsv", sep="\t")

    @staticmethod
    def dump(
        jobsystem: JobSystem,
        max_nthread: int,
        cluster_server_name: NullableString = None,
    ):
        """
        Download all sequence files that have not yet downloaded in the project via the specified job system.

        Parameters
        ----------
        jobsystem : JobSystem[celline.jobs.jobs.JobSystem]
            Job system to dump data.
        max_nthread : int
            Total number of thread to dump data.
            The dumping process occupy 1 thread for each cluster computer to download in parallel.
        cluster_server_name: NullableString[str or None] = None
            Cluster server name to use PBS system. If jobsystem is default_bash or nohup, you will assign None.
        """
        Directory.initialize()
        nowtime = str(time())
        os.makedirs(
            f"{Config.PROJ_ROOT}/jobs/auto/0_dump/{nowtime}", exist_ok=True)
        runtable = SRR.get_runtable()
        if runtable is None:
            print("[ERROR] Could not find run.tsv in your project.")
            quit()
        RuntableResolver.validate(runtable)

        runtable["dumped_filepath"] = runtable.apply(
            lambda ser: f"{Config.PROJ_ROOT}/resources/{ser['dumped_filepath']}", axis=1
        )
        runtable["fileexists"] = runtable.apply(
            lambda ser: os.path.isfile(str(ser["dumped_filepath"])), axis=1
        )
        runtable = runtable[~runtable["fileexists"]]
        del runtable["fileexists"]
        run_ids: List[str] = runtable["run_id"].unique().tolist()
        if jobsystem == JobSystem.default_bash:
            print(
                "[Warning] Only 1 thread will be used because you use default bash system."
            )
            max_nthread = 1
        if len(run_ids) < max_nthread:
            print(
                "[WARNING] The number of threads was suppressed due to the number of threads exceeding the required number."
            )
            max_nthread = len(run_ids)
        eachsize = math.ceil(len(run_ids) / max_nthread)
        for threadnum in range(max_nthread):
            write_target_sh = []
            log_location = f"{Config.PROJ_ROOT}/jobs/auto/0_dump/{nowtime}/logs"
            os.makedirs(log_location, exist_ok=True)
            if jobsystem == JobSystem.PBS:
                if cluster_server_name is None:
                    raise InvalidServerNameException(
                        "Please specify cluster server name (like yuri) to use PBS job system."
                    )
                write_target_sh = Jobs.build(
                    template_path=f"{Config.EXEC_ROOT}/templates/controllers/PBS.csh",
                    replace_params={
                        "cluster": cluster_server_name,
                        "log": f"{log_location}/dump_cluster_{threadnum}.log",
                        "jobname": "dump",
                        "nthread": 1,
                    },
                )
            if threadnum == max_nthread - 1:
                target_run = run_ids[eachsize * threadnum: len(run_ids)]
            else:
                target_run = run_ids[eachsize *
                                     threadnum: eachsize * (threadnum + 1)]
            target_data = runtable[runtable["run_id"].isin(
                target_run)].reset_index()
            for run_id in target_run:
                targetcol = target_data[target_data["run_id"] == run_id]
                rootdir = "/".join(
                    targetcol["dumped_filepath"].iloc[0].split("/")[0:-1]
                )
                os.makedirs(rootdir, exist_ok=True)
                if targetcol["filetype"].iloc[0] == "bam":
                    write_target_sh.append(
                        f"""
cd "{rootdir}" || exit
wget {targetcol["cloud_filepath"].iloc[0]}
mv {targetcol["raw_filename"].iloc[0]} {targetcol["dumped_filename"].iloc[0]}
"""
                    )
                elif targetcol["filetype"].iloc[0] == "fastq":
                    write_target_sh.append(
                        f"""
cd "{rootdir}" || exit
scfastq-dump {targetcol["run_id"].unique().tolist()[0]}
"""
                    )
                    for target_rawfilename in (
                        targetcol["raw_filename"].unique().tolist()
                    ):
                        moved_name = targetcol[
                            targetcol["raw_filename"] == target_rawfilename
                        ]["dumped_filename"].tolist()[0]
                        write_target_sh.append(
                            f"mv {target_rawfilename} {moved_name}\n"
                        )
            srcfile = f"{Config.PROJ_ROOT}/jobs/auto/0_dump/{nowtime}/dump_cluster_{threadnum}.sh"
            with open(srcfile, mode="w") as f:
                f.writelines(write_target_sh)
            if jobsystem == JobSystem.default_bash:
                subprocess.run(f"bash {srcfile}", shell=True)
            elif jobsystem == JobSystem.nohup:
                subprocess.run(
                    f"nohup bash {srcfile} > {log_location}/dump_cluster_{threadnum}.log &",
                    shell=True,
                )
            elif jobsystem == JobSystem.PBS:
                subprocess.run(f"qsub {srcfile}", shell=True)

    @staticmethod
    def count(
        jobsystem: JobSystem,
        each_nthread: int,
        max_nthread: int,
        cluster_server_name: NullableString = None,
    ):
        Directory.initialize()
        nowtime = str(time())
        os.makedirs(
            f"{Config.PROJ_ROOT}/jobs/auto/1_count/{nowtime}", exist_ok=True)
        runtable = SRR.get_runtable()
        if runtable is None:
            print("[ERROR] Could not find run table in your project.")
            quit()
        RuntableResolver.validate(runtable)
        runtable["run"] = runtable.apply(
            lambda ser: os.path.isfile(
                Directory.get_filepath(
                    ser["dumped_filename"], type=DirectoryType.dumped_file
                )
            )
            & os.path.isdir(
                Directory.get_filepath(
                    ser["dumped_filename"], type=DirectoryType.counted
                )
            )
            == False,
            axis=1,
        )

        runtable = runtable[runtable["run"]]
        del runtable["run"]

        def initialize_countdir(ser: Series):
            directory = f'{Directory.get_filepath(str(ser["dumped_filename"]), DirectoryType.count)}/{ser["gsm_id"]}'
            if os.path.isdir(directory):
                if os.path.isdir(f"{directory}/outs"):
                    return True
                else:
                    print(
                        "[WARNING] There is an abnormally terminated Cellranger output. Delete these incomplete files."
                    )
                    shutil.rmtree(directory, ignore_errors=True)
                    return False
            else:
                return False

        runtable["counted_directory"] = runtable.apply(
            lambda ser: initialize_countdir(ser),
            axis=1,
        )
        runtable = runtable[~runtable["counted_directory"]]
        print(runtable)
        ############################################
        total_size = runtable.index.size
        pararell_num = max_nthread // each_nthread
        if total_size * each_nthread < max_nthread:
            max_nthread = total_size * each_nthread
        eachsize = total_size // pararell_num
        print(
            f"Compute {pararell_num} jobs in parallel, using a total of {max_nthread} threads. ({each_nthread} threads are allocated for each job)"
        )
        for threadnum in range(pararell_num):
            write_target_sh = []
            log_location = f"{Config.PROJ_ROOT}/jobs/auto/1_count/{nowtime}/logs"
            os.makedirs(log_location, exist_ok=True)
            if jobsystem == JobSystem.PBS:
                if cluster_server_name is None:
                    raise InvalidServerNameException(
                        "Please specify cluster server name (like yuri) to use PBS job system."
                    )
                write_target_sh = Jobs.build(
                    template_path=f"{Config.EXEC_ROOT}/templates/controllers/PBS.csh",
                    replace_params={
                        "cluster": cluster_server_name,
                        "log": f"{log_location}/count_cluster_{threadnum}.log",
                        "jobname": "count",
                        "nthread": each_nthread,
                    },
                )
            if threadnum == max_nthread - 1:
                target_data = runtable[
                    eachsize * threadnum: runtable.index.size
                ].reset_index()
            else:
                target_data = runtable[
                    eachsize * threadnum: eachsize * (threadnum + 1)
                ].reset_index()
            for run_id in target_data["gsm_id"].unique().tolist():
                print(run_id)
                targetcol = target_data[target_data["gsm_id"] == run_id]
                gsmid: str = targetcol["gsm_id"].iloc[0]
                dumped_file_path_spl: List[str] = (
                    targetcol["dumped_filepath"].iloc[0].split("/")
                )
                root_dir = f"{Config.PROJ_ROOT}/resources/{dumped_file_path_spl[0]}"
                src_dir = f"{root_dir}/0_dumped/{gsmid}"
                dist_dir = f"{root_dir}/1_count"
                os.makedirs(dist_dir, exist_ok=True)
                if targetcol["filetype"].iloc[0] == "bam":
                    write_target_sh.append(
                        f"""
cd {root_dir}/0_dumped/{gsmid}
echo "Converting bam to fastq files."
rm -rf fastqs
cellranger bamtofastq --nthreads={each_nthread} bams/{gsmid}.bam fastqs
counted={dist_dir}
raw_path={src_dir}/fastqs
cd {Config.EXEC_ROOT}
dirpath=$(poetry run python {Config.EXEC_ROOT}/bin/runtime/get_subdir.py $raw_path)
cd $counted
cellranger count --id={run_id} --fastqs=$dirpath --sample=bamtofastq --transcriptome={Genome.get(targetcol["spieces"].iloc[0])} --no-bam --localcores {each_nthread}
"""
                    )
                elif targetcol["filetype"].iloc[0] == "fastq":
                    write_target_sh.append(
                        f"""
counted="{dist_dir}"
raw_path="{src_dir}/fastqs"
cd $counted
cellranger count --id={targetcol["gsm_id"].iloc[0]} --fastqs=$raw_path --sample={run_id} --transcriptome={Genome.get(targetcol["spieces"].iloc[0])} --no-bam --localcores {each_nthread}
"""
                    )
                else:
                    raise NCBIException("Unknown protocol file")
            srcfile = f"{Config.PROJ_ROOT}/jobs/auto/1_count/{nowtime}/count_cluster_{threadnum}.sh"
            with open(srcfile, mode="w") as f:
                f.writelines(write_target_sh)
            if jobsystem == JobSystem.default_bash:
                subprocess.run(f"bash {srcfile}", shell=True)
            elif jobsystem == JobSystem.nohup:
                subprocess.run(
                    f"nohup bash {srcfile} > {log_location}/count_cluster_{threadnum}.log &",
                    shell=True,
                )
            # elif jobsystem == JobSystem.PBS:
            #     subprocess.run(f"qsub {srcfile}", shell=True)

    @staticmethod
    def seurat(
        jobsystem: JobSystem,
        cluster_server_name: NullableString = None,
    ):
        Directory.initialize()
        nowtime = str(time())
        srcroot = f"{Config.PROJ_ROOT}/jobs/auto/2_seurat/{nowtime}"
        os.makedirs(srcroot, exist_ok=True)
        runtable = SRR.get_runtable()
        if runtable is None:
            print("[ERROR] Could not find run.tsv in your project.")
            quit()
        RuntableResolver.validate(runtable)

        runtable["seurat_filepath"] = runtable.apply(
            lambda ser: f"{Config.PROJ_ROOT}/resources/{ser['sample_name']}/2_seurat/seurat.h5seurat", axis=1
        )
        runtable["fileexists"] = runtable.apply(
            lambda ser: os.path.isfile(str(ser["seurat_filepath"])), axis=1
        )
        runtable = runtable[~runtable["fileexists"]]
        del runtable["fileexists"]
        sample_names: List[str] = runtable["sample_name"].unique().tolist()
        if jobsystem == JobSystem.default_bash:
            print(
                "[Error] Please use nohup or job system."
            )
            quit()
        for target_sample in sample_names:
            write_target_sh = []
            log_location = f"{srcroot}/logs"
            os.makedirs(log_location, exist_ok=True)
            if jobsystem == JobSystem.PBS:
                if cluster_server_name is None:
                    raise InvalidServerNameException(
                        "Please specify cluster server name (like yuri) to use PBS job system."
                    )
                write_target_sh = Jobs.build(
                    template_path=f"{Config.EXEC_ROOT}/templates/controllers/PBS.csh",
                    replace_params={
                        "cluster": cluster_server_name,
                        "log": f"{log_location}/mkseurat_{target_sample}.log",
                        "jobname": "mkseurat",
                        "nthread": 1,
                    },
                )
            if Setting.r_path is None or Setting.r_path == "":
                print("[Error] Please set r_path setting.toml in your project.")
                quit()
            write_target_sh.append(
                f"\n{Setting.r_path}script {Config.EXEC_ROOT}/R/integrate.R {Setting.name} {target_sample} {Config.PROJ_ROOT}/resources/{target_sample}/1_count"
            )
            srcfile = f"{srcroot}/mkseurat_{target_sample}.sh"
            with open(srcfile, mode="w") as f:
                f.writelines(write_target_sh)
            if jobsystem == JobSystem.nohup:
                subprocess.run(
                    f"nohup bash {srcfile} > {log_location}/mkseurat_{target_sample} &",
                    shell=True,
                )
            elif jobsystem == JobSystem.PBS:
                subprocess.run(f"qsub {srcfile}", shell=True)
