from __future__ import annotations  # type: ignore

from celline.plugins.collections.generic import ListC, DictionaryC
from typing import List, Optional, Any
from celline.utils.type import ClassProperty

from celline.plugins.reflection.activator import Activator
from celline.plugins.reflection.module import Module
from celline.plugins.reflection.type import (
    BindingFlags,
    DictionaryC,
    KeyValuePair,
    TypeC,
    typeof,
)
from celline.functions._base import CellineFunction


class FunctionManager:
    __t_obj: Optional[ListC[KeyValuePair[TypeC, Any]]] = None
    __commands: Optional[ListC[KeyValuePair[str,
                                            KeyValuePair[TypeC, Any]]]] = None

    @ClassProperty
    @classmethod
    def commands(cls) -> ListC[KeyValuePair[str, KeyValuePair[TypeC, Any]]]:
        if FunctionManager.__t_obj is None:
            FunctionManager.__t_obj = (
                Module.GetModules(dirs="celline/functions")
                .Where(
                    lambda module: module.GetTypes().Length > 0
                )
                .Select(lambda module: module.GetTypes().First())
                .Where(
                    lambda t: (
                        t.BaseType.FullName == "celline.functions._base+CellineFunction"
                    )
                )
                .Select(lambda t: KeyValuePair(t, Activator.CreateInstance(t)))
            )
            FunctionManager.__commands = FunctionManager.__t_obj.Select(
                lambda t: KeyValuePair(
                    key=str(t.Key.GetMethod("register").Invoke(t.Value)), value=t
                )
            )
        if FunctionManager.__commands is None:
            raise SystemError("")
        return FunctionManager.__commands

    @staticmethod
    def call(argv: List[str]):
        """Analyze command and call"""
        num = 1
        if len(argv) < 1:
            print("Please specify command")
            quit()
        target_cmd: str = argv[0]
        options: DictionaryC[str, Optional[str]
                             ] = DictionaryC[str, Optional[str]]()
        while True:
            if num >= len(argv):
                break
            if "--" in argv[num]:
                key = argv[num].replace("--", "")
                if num >= len(argv) - 1:
                    val = None
                elif "--" in argv[num + 1]:
                    val = None
                else:
                    num += 1
                    val = argv[num]
                options.Add(key=key, value=val)
            else:
                options.Add(key=f"req_{num}", value=argv[num])
            num += 1
        # seach commands, call
        candidates = FunctionManager.commands.Where(
            lambda cmd: cmd.Key == target_cmd)
        if candidates.Length == 0:
            print(f"[ERROR] Could not find target command: {target_cmd}")
            quit()
        candidates = candidates.First().Value
        candidates.Key.GetMethod("on_call").Invoke(
            candidates.Value, options=options)
        return
