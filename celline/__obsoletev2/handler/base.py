from abc import ABCMeta, abstractmethod


class BaseDBHandler(metaclass=ABCMeta):
    """
    Fundamental handler class to manage external database
    """

    @abstractmethod
    def project_name(self) -> str:
        return ""

    @abstractmethod
    def sample_name(self) -> str:
        return ""

    @abstractmethod
    def onadd(self):
        return
