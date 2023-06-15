import argparse
import asyncio
import sys
from typing import Any, List

from celline.cmd.help import Commands
from celline.controllers.argument import JobArgument, ThreadArgument
from celline.controllers.controller import (AddController, AddRefController,
                                            CountController, DumpController,
                                            SeuratController,
                                            HelpController,
                                            InitializeController)
from celline.ncbi.genome import Genome
from celline.plugins.collections.generic import ListC
from celline.plugins.reflection.activator import Activator
from celline.plugins.reflection.module import Module
from celline.plugins.reflection.type import (BindingFlags, DictionaryC,
                                             KeyValuePair, TypeC, typeof)
from celline.utils.config import Config, Setting


class ControllerManager:
    t_obj: ListC[KeyValuePair[TypeC, Any]]
    commands: ListC[KeyValuePair[str, KeyValuePair[TypeC, Any]]]

    @staticmethod
    def initialize():
        ControllerManager.t_obj = (
            Module.GetModules(dirs="celline/controllers")
            .Select(lambda module: module.GetTypes().First())
            .Where(
                lambda t: not t.IsAbstract
                & (t.BaseType.FullName == "celline.controllers.controller+Controller")
            )
            .Select(lambda t: KeyValuePair(t, Activator.CreateInstance(t)))
        )
        ControllerManager.commands = ControllerManager.t_obj.Select(
            lambda t: KeyValuePair(
                key=str(t.Key.GetProperty("command").GetValue(t.Value)), value=t
            )
        )

    # if not ControllerManager.commands.Contains(cmd):
    #     raise InvalidArgumentException(f"Could not found {cmd}")
    # target_command = (
    #     ControllerManager.commands.Where(lambda t_obj: t_obj.Key == cmd).First().Value
    # )
    # target_command.Key.GetMethod("call").Invoke(target_command.Value, options)o


if __name__ == "__main__":
    cmd = sys.argv[3]
    options = sys.argv[4:]
    if cmd == "init":
        InitializeController().call(pwd=sys.argv[2])
        quit()
# TODO: Future work, generalize these codes with abstract
    # Commands.help(cmd)
    Config.initialize(exec_root_path=sys.argv[1], proj_root_path=sys.argv[2])
    Setting.initialize()
    Genome.initialize()
    # Analyze
    job_arg = JobArgument(options=options)
    th_arg = ThreadArgument(options=options)
    if cmd == "add":
        AddController(options=options).call()
    elif cmd == "dump":
        DumpController().call(
            jobsystem=job_arg.jobsystem,
            nthread=th_arg.nthread,
            cluster_server_name=job_arg.cluster_server_name
        )
    elif cmd == "count":
        CountController().call(
            jobsystem=job_arg.jobsystem,
            each_nthread=th_arg.each_nthread,
            nthread=th_arg.nthread,
            cluster_server_name=job_arg.cluster_server_name
        )
    elif cmd == "mkseurat":
        SeuratController().call(
            jobsystem=job_arg.jobsystem,
            cluster_server_name=job_arg.cluster_server_name
        )
    elif cmd == "addref":
        AddRefController(options=options).call()
    else:
        HelpController().call(cmd)
    # SRR.dump(
    #     jobsystem=JobSystem.default_bash,
    #     max_nthread=1,
    #     cluster_server_name="cosmos",
    # )
    # elif cmd == "addref":
    #     print(pull_n_option(options, 0))
