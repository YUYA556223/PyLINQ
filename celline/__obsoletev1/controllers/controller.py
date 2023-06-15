import asyncio
import os
from typing import Any, Dict, List, Union

import yaml
from yaml import Loader

from celline.jobs.jobs import JobSystem
from celline.ncbi.genome import Genome
from celline.ncbi.srr import SRR
from celline.utils.config import Config, Setting
from celline.utils.exceptions import InvalidArgumentException
from celline.cmd.help import Commands
from celline.utils.typing import NullableString


class AddController:
    run_id: str

    def __init__(self, options: List[str]) -> None:
        if len(options) < 1:
            raise InvalidArgumentException("Please specify run id")
        self.run_id = options[0]
        if not (
            self.run_id.startswith("SRR")
            | self.run_id.startswith("GSM")
            | os.path.isfile(self.run_id)
        ):
            print("Run ID should SRR ID or GSM ID.\nUsage:")
            HelpController.call("add")
            quit()
        pass

    def call(self) -> None:
        if os.path.isfile(self.run_id):
            asyncio.get_event_loop().run_until_complete(SRR.add_range(self.run_id))
        else:
            asyncio.get_event_loop().run_until_complete(SRR.add(self.run_id))


class DumpController:
    def __init__(self) -> None:
        pass

    def call(
        self, jobsystem: JobSystem, nthread: int, cluster_server_name: NullableString
    ) -> None:
        SRR.dump(
            jobsystem=jobsystem,
            max_nthread=nthread,
            cluster_server_name=cluster_server_name,
        )
        return


class CountController:
    def __init__(self) -> None:
        pass

    def call(
        self,
        jobsystem: JobSystem,
        each_nthread: int,
        nthread: int,
        cluster_server_name: NullableString,
    ) -> None:
        SRR.count(
            jobsystem=jobsystem,
            each_nthread=each_nthread,
            max_nthread=nthread,
            cluster_server_name=cluster_server_name,
        )
        return


class SeuratController:
    def __init__(self) -> None:
        pass

    def call(
        self,
        jobsystem: JobSystem,
        cluster_server_name: NullableString,
    ) -> None:
        SRR.seurat(
            jobsystem=jobsystem,
            cluster_server_name=cluster_server_name,
        )
        return


class AddRefController:
    def __init__(self, options: List[str]) -> None:
        if len(options) < 2:
            raise InvalidArgumentException(
                "Please specify celline addref [species] [path]"
            )
        self.species: str = options[0]
        self.path: str = options[1]
        pass

    def call(self) -> None:
        Genome.add(self.species, self.path)
        return


class InitializeController:
    def __init__(self) -> None:
        pass

    def call(self, pwd: str) -> None:
        Config.PROJ_ROOT = pwd
        Setting.name = input("Project name?: ")
        Setting.version = 0.1
        Setting.wait_time = 4
        Setting.flush()
        return


class HelpController:
    helpobj: Union[Commands, None] = None

    @staticmethod
    def call(command: str):
        if HelpController.helpobj is None:
            HelpController.helpobj = Commands()
        print(HelpController.helpobj.help(command))
        return
