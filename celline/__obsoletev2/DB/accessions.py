from abc import ABCMeta, abstractmethod
from typing import List, Dict
from celline.plugins.reflection.module import Module
from celline.config import Config


# class Project(metaclass=ABCMeta):
#     """"""

#     @abstractmethod
#     def project_name(self) -> str:
#         return ""

#     @abstractmethod
#     def sample_name(self) -> str:
#         return ""

#     @abstractmethod
#     def onadd(self):
#         return

# @staticmethod
# def project_names() -> List[str]:
#     Module.GetModules(f"{Config.EXEC_ROOT}/celline").ForEach(
#         lambda d: print(d.Name)
#     )


class AccessionManager:
    """
    Manage project
    """
