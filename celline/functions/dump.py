import math
import os
import datetime
from typing import Dict, List, Optional

from celline.database import NCBI
from celline.functions._base import CellineFunction
from celline.job.PBS import PBS
from celline.job.jobsystem import JobSystem
from celline.plugins.collections.generic import DictionaryC, ListC
from celline.config import Config, Setting
from celline.database import NCBI
from celline.data.ncbi import SRR
from celline.utils.thread import split_jobs
from celline.utils.project import DirectoryManager
import subprocess


class Dump(CellineFunction):
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

    def register(self) -> str:
        return "dump"

    @property
    def cluster_num(self):
        cluster = self.options["parallel"]
        if cluster is None:
            return 1
        elif cluster.isdecimal():
            print("[ERROR] 'parallel' argument requires int number.")
            quit()
        return int(cluster)

    def mkdir(self, path: List[str]):
        for p in path:
            os.makedirs(p, exist_ok=True)
        return

    def init_directory(self):
        """returns: Target GSM ID"""
        target_gsm: List[str] = []
        for gsm_id in NCBI.get_gsms():
            root = f"{Config.PROJ_ROOT}/resources/{gsm_id}"
            self.mkdir(
                [
                    root,
                    f"{root}/raw",
                ]
            )
            if DirectoryManager.is_dumped(gsm_id):
                print(f"[WARNING] Specified GSM ID ({gsm_id}) is already dumped. Skip.")
            else:
                target_gsm.append(gsm_id)
        return target_gsm

    def on_call(self, args: Dict[str, DictionaryC[str, Optional[str]]]):
        self.options = args["options"]
        ct = datetime.datetime.now()
        directory_time_str = ct.strftime("%Y%m%d%H%M%S")
        target_gsms: List[str] = self.init_directory()
        if self.options["gsm"] is not None:
            target_gsms = str(self.options["gsm"]).split(",")
        with open(f"{Config.PROJ_ROOT}/log.log", mode="a") as f:
            f.write(f"\n{target_gsms}")
        job_directory = f"{Config.PROJ_ROOT}/jobs/auto/0_dump/{directory_time_str}"
        os.makedirs(job_directory, exist_ok=True)
        header: str = ""
        job_system: JobSystem = JobSystem.default_bash
        server_name: str = ""
        job_cluster = self.options["job"]
        if job_cluster is not None:
            if "PBS" in job_cluster:
                if "@" not in job_cluster:
                    print("[ERROR] PBS job shold called as PBS@<cluster_server_name>")
                    quit()
                splitted = job_cluster.split("@")
                if splitted[0] == "PBS":
                    job_system = JobSystem.PBS
                    if len(splitted) != 2:
                        print(
                            "[ERROR] PBS job shold called as PBS@<cluster_server_name>"
                        )
                        quit()
                    server_name = splitted[1]
                else:
                    print("[ERROR] PBS job shold called as PBS@<cluster_server_name>")
                    quit()
            elif "nohup" in job_cluster:
                job_system = JobSystem.nohup
        __nthread = self.options["nthread"]
        if __nthread is None:
            print("[Warning] nthread will be 1 automatically")
            nthread = 1
        else:
            try:
                nthread = int(__nthread, 10)
            except ValueError:
                print("[ERROR] nthread parameter should express as <int> format")
                quit()
        log_dir = f"{Config.PROJ_ROOT}/jobs/auto/0_dump/{directory_time_str}/logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        cmds: List[str] = []
        cached_gsms = NCBI.get_gsms()
        for gsm in target_gsms:
            root = f"{Config.PROJ_ROOT}/resources/{gsm}"
            os.makedirs(f"{root}/raw/fastqs", exist_ok=True)
            srrs = cached_gsms[gsm].child_srr_ids
            for srr_id in srrs:
                srr = SRR.search(srr_id)
                if srr.file_type == SRR.ScRun.FileType.Fastq:
                    cmd = f"cd {root}/raw && scfastq-dump {srr_id}"
                    sample_id = f"S{srrs.index(srr_id) + 1}"
                    for run in srr.sc_runs:
                        dumped_name = f"{gsm}_{sample_id}_{run.lane.name}_{run.readtype.name}.fastq.gz"
                        raw_name = srr_id
                        if run.readtype == SRR.ScRun.ReadType.R1:
                            raw_name += "_1.fastq.gz"
                        elif run.readtype == SRR.ScRun.ReadType.R2:
                            raw_name += "_2.fastq.gz"
                        elif run.readtype == SRR.ScRun.ReadType.I1:
                            raw_name += "_3.fastq.gz"
                        elif run.readtype == SRR.ScRun.ReadType.I2:
                            raw_name += "_4.fastq.gz"
                        else:
                            print("[ERROR] Unknown read type")
                        cmd += f" && mv {raw_name} fastqs/{dumped_name}"
                    cmds.append(cmd)
                elif srr.file_type == SRR.ScRun.FileType.Bam:
                    for run in srr.sc_runs:
                        srr.parent_gsm
                        cmds.append(
                            f"cd {root}/raw && wget {run.cloud_path.path} -O {srr.runid}.bam"
                        )

        __base_job_num = 0
        cluster_num = 0
        for job in split_jobs(len(cmds), nthread):
            if job_system == JobSystem.PBS:
                result_cmd = (
                    "".join(
                        PBS(
                            1,
                            server_name,
                            job_system.name,
                            f"{log_dir}/cluster{cluster_num}.log",
                        ).header
                    )
                    + "\n"
                )
            else:
                result_cmd = ""
            if job != 0:
                result_cmd += "\n".join(cmds[__base_job_num : __base_job_num + job])
                with open(f"{job_directory}/cluster{cluster_num}.sh", mode="w") as f:
                    f.write(result_cmd)
                cluster_num += 1
            __base_job_num += job
        if not self.options.ContainsKey("norun"):
            for target_cluster in range(cluster_num):
                if job_system == JobSystem.default_bash:
                    subprocess.run(
                        f"bash {job_directory}/cluster{target_cluster}.sh", shell=True
                    )
                elif job_system == JobSystem.nohup:
                    subprocess.run(
                        f"nohup bash {job_directory}/cluster{target_cluster}.sh > {Config.PROJ_ROOT}/jobs/auto/0_dump/{directory_time_str}/__logs.log &",
                        shell=True,
                    )
                elif job_system == JobSystem.PBS:
                    subprocess.run(
                        f"qsub {job_directory}/cluster{target_cluster}.sh", shell=True
                    )
                else:
                    print("[ERROR] Unknown job system :(")
