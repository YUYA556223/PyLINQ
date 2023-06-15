import sys
from typing import Any, Dict, Union, TypeVar

import yaml
from yaml import Dumper, Loader

Commands = TypeVar("Commands")  # type: ignore
Arguments = TypeVar("Arguments")


class Commands:
    """
    Support command
    """
    class Arguments:
        def __init__(self, command: Commands) -> None:
            self.command: Commands = command
            pass
    contents: Dict[str, Dict[str, Any]]
    is_loaded: bool = False

    @staticmethod
    def __build(command: str, build_args: bool = True) -> str:
        target = Commands.contents[command]
        cmd = f"𓈒𓏸 celline {command}: {target['description']}"
        if build_args:
            args = target["args"]
            for arg in args:
                if args[arg]["optional"]:
                    cmd += f"\n    --{arg}"
                else:
                    cmd += f"\n    {arg}"
                cmd += f"[{args[arg]['type']}]: {args[arg]['description']}"
        return cmd

    def __init__(self, command: str) -> None:
        if Commands.is_loaded is False:
            with open(f"{sys.path[0]}/docs/__help.yaml", mode="r") as f:
                Commands.contents = yaml.load(f, Loader=Loader)
            Commands.is_loaded = True
        self.command: str = command
        pass

    @property
    def help(self):
        return Commands.__build(self.command)

    def print_help(self):
        if self.command not in Commands.contents:
            print(f"[ERROR] Command not found: {self.command}")
            print("┏━━━━━━━━━━ Available Commands ━━━━━━━━━━┓")
            print("\n".join([Commands.__build(cmd, build_args=False)
                  for cmd in Commands.contents.keys()]))
            print("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛")
        else:
            print(Commands.__build(self.command))
