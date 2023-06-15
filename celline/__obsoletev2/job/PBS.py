import datetime
import time

from celline.config import Config, Setting
from celline.job.jobsystem import Jobs  # type: ignore
import os


class PBS:
    def __init__(self, nthread: int, cluster_server_name: str, job_name: str, log_path: str) -> None:
        # Build PBS header

        self.header = Jobs.build(
            template_path=f"{Config.EXEC_ROOT}/templates/controllers/PBS.sh",
            replace_params={
                "cluster": cluster_server_name,
                "log": log_path,
                "jobname": job_name,
                "nthread": nthread
            }
        )
