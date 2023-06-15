import datetime
import time

from celline.jobs.jobs import Jobs  # type: ignore
from celline.utils.config import Config, Setting


class PBS:
    def __init__(self, nthread: int, cluster_server_name: str, job_name: str) -> None:
        nowtime = str(time.time())
        # Build PBS header
        header = Jobs.build(
            template_path=f"{Config.EXEC_ROOT}/templates/controllers/PBS.sh",
            replace_params={
                "cluster": cluster_server_name,
                "log": f"{Config.PROJ_ROOT}/logs/{nowtime}_",
                "jobname": job_name,
                "nthread": nthread
            }
        )
