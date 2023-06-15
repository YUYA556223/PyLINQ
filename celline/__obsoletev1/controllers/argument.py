from argparse import ArgumentParser
from typing import Any, Dict, List, Union

from celline.jobs.jobs import JobSystem
from celline.utils.exceptions import (InvalidArgumentException,
                                      InvalidJobException,
                                      InvalidJobSystemException)
from celline.utils.typing import NullableString


class Argument:
    @staticmethod
    def analyze(options: List[str], command: str):
        if command not in options:
            return None
        else:
            index = options.index(command)
            if index + 1 > len(options):
                print(
                    "[ERROR] Invalid argument style. Please specify argument variable like `command --option value`")
                quit()


class JobArgument:
    def __init__(self, options: List[str]) -> None:
        if "--job" in options:
            index = options.index("--job")
            if index + 1 > len(options):
                raise InvalidJobException(
                    "Please specify jobsystem like --job [JobSystem]@[ClusterServer]")
            jobsys_server = options[index + 1]
            if "@" not in jobsys_server:
                raise InvalidJobException(
                    "Please specify jobsystem like [JobSystem]@[ClusterServer]")
            jobs = jobsys_server.split("@")
            jobsystem = jobs[0]
            if jobsystem not in [x.name for x in JobSystem]:
                raise InvalidJobSystemException(
                    f"Unknown job system: {jobsystem}")
            self.jobsystem = JobSystem(jobsystem)
            self.cluster_server_name: NullableString = jobs[1]
        else:
            self.jobsystem = JobSystem.default_bash
            self.cluster_server_name = None
        pass


class ThreadArgument:
    def __init__(self, options: List[str]) -> None:
        if "--nthread" in options:
            index = options.index("--nthread")
            if index + 1 > len(options):
                raise InvalidJobException(
                    "Please specify number of thread like --nthread [nthread]")
            nthread_str = options[index + 1]
            try:
                self.nthread = int(nthread_str, 10)
            except ValueError:
                raise InvalidArgumentException(
                    "Please specify number(int) of thread like --nthread [nthread]")
            if "--each" in options:
                index = options.index("--each")
                if index + 1 > len(options):
                    raise InvalidJobException(
                        "Please specify number of each thread like --each [each_nthread]")
                eachnthread_str = options[index + 1]
                try:
                    self.each_nthread = int(eachnthread_str, 10)
                except ValueError:
                    raise InvalidArgumentException(
                        "Please specify each number(int) of thread like --each [each_nthread]")
            else:
                self.each_nthread = 3
        else:
            self.nthread = 1
            self.each_nthread = 3
        pass
