import subprocess
import sys
from typing import Literal, Union

import toml  # type: ignore

from celline.jobs.jobs import JobSystem  # type:ignore
from celline.ncbi.srr import SRR
from celline.utils.config import Config
from celline.utils.directory import Directory
from celline.utils.exceptions import NCBIException


class Test:
    @staticmethod
    def entry():
        # SRR.dump(
        #     jobsystem=JobSystem.PBS,
        #     cluster_server_name="cosmos",
        #     total_nthread=2
        # )

        process = subprocess.run(
            'read -p "Hit enter: "',
            shell=True,
            stdout=subprocess.PIPE
        )
        print(process.stdout.decode("UTF-8"))

        # print(
        #     DataFrame(
        #         index=["TEST", "TEST2"],
        #         data={
        #             "Data1": 100,
        #             "Data2": 200
        #         }
        #     ).to_dict()
        # )
        # print(
        #     vars(SRR())
        # )
