from typing import (
    Protocol,
    List,
    TypeVar,
    Generic,
    Final,
    Optional,
    Type,
    NamedTuple,
    overload,
)
import polars as pl
from enum import Enum

# from celline.config import Config
from abc import ABCMeta, abstractmethod
import os
import pprint

## Type vars #############
T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")
T6 = TypeVar("T6")
T7 = TypeVar("T7")
T8 = TypeVar("T8")
##########################


class DBBase(metaclass=ABCMeta):
    class Scheme(NamedTuple):
        notoverrided: str

    df: pl.DataFrame
    class_name: str = ""
    scheme: Type[Scheme]
    PATH: Final[str]

    def __init__(self) -> None:
        if (self.class_name == "") | (self.scheme == DBBase.Scheme.notoverrided):
            raise LookupError(
                "Please override class_name and scheme variable in your custom DB."
            )
        self.PATH = f"{Config.EXEC_ROOT}/DB/{self.class_name}.parquet"
        if os.path.isfile(self.PATH):
            self.df = pl.read_parquet(self.PATH)
        else:
            self.df = pl.DataFrame(
                {},
                schema={
                    name: element.value[1]
                    for name, element in self.scheme.__members__.items()
                },
            )
            self.df.write_parquet(self.PATH)

    class ResultStructure(Generic[T1, T2]):
        col1: List[T1]
        col2: List[T2]
        col3: List[T3]
        col4: List[T4]
        col5: List[T5]
        col6: List[T6]
        col7: List[T7]
        col8: List[T8]

    def get(
        self,
        col1: T1,
        col2: T1 = None,
        col3: T1 = None,
        col4: T1 = None,
        col5: T1 = None,
        col6: T1 = None,
        col7: T1 = None,
        col8: T1 = None,
    ) -> ResultStructure:
        t1name, t2name, t3name, t4name, t5name, t6name, t7name, t8name = ""
        for name in self.scheme._fields:
            if getattr(self.scheme, name) == col1:
                t1name = name
            if getattr(self.scheme, name) == col2:
                t2name = name
            if getattr(self.scheme, name) == col3:
                t3name = name
            if getattr(self.scheme, name) == col4:
                t4name = name
            if getattr(self.scheme, name) == col5:
                t5name = name
            if getattr(self.scheme, name) == col6:
                t6name = name
            if getattr(self.scheme, name) == col7:
                t7name = name
            if getattr(self.scheme, name) == col8:
                t8name = name
        return DBBase.ResultStructure(
            self.df.get_column(t1name).to_list(),
            self.df.get_column(t2name).to_list(),
            self.df.get_column(t3name).to_list(),
            self.df.get_column(t4name).to_list(),
            self.df.get_column(t5name).to_list(),
            self.df.get_column(t6name).to_list(),
            self.df.get_column(t7name).to_list(),
            self.df.get_column(t8name).to_list(),
        )

    @overload
    def get(self, col1: T1) -> T1:
        pass

    def get_value(self, col: T1) -> List[T1]:
        return self.get(col).to_list()

    def plptr(self, col: T1) -> pl.Expr:
        """Returns a pointer to the column that applies to col."""
        tname = ""
        for name in self.scheme._fields:
            if getattr(self.scheme, name) == col:
                tname = name
                break
        return pl.col(tname)

    def gets(self, col: List[Scheme]):
        expr: List[str] = []
        for _c in col.values():
            expr.append(_c.name)
        return self.df.select(pl.col(expr))

    def flush(self):
        self.df.write_parquet(f"{Config.EXEC_ROOT}/DB/{self.class_name}.parquet")


class Primary(Generic[T1]):
    """Set Primary"""


class Ref(Generic[T2]):
    """Set Reference"""
