from enum import Enum, unique
import polars as pl
from typing import List, Final
from celline.DB._base import DBBase, Primary, Ref
from celline.DB.definition.gse import GSE
from pysradb.sraweb import SRAweb
from pprint import pprint


class GSM(DBBase):
    DB: Final[SRAweb] = SRAweb()

    @unique
    class Scheme(Enum):
        accession_id = ("accession", str)
        title = ("title", str)
        summary = ("summary", str)
        species = ("taxon", str)
        raw_link = ("ftplink", str)
        srx_id = ("SRA", str)
        parent_gse_id = ("parent_gse", str)

    def __init__(self) -> None:
        self.class_name = __class__.__name__
        self.scheme = GSM.Scheme
        super().__init__()

    def exist(self, gsm_id: str):
        return (
            self.df.filter(pl.col(GSM.Scheme.accession_id.name) == gsm_id).shape[0]
        ) != 0

    def search(self, gsm_id: str):
        if self.exist(gsm_id):
            return self.df.filter(pl.col(GSM.Scheme.accession_id.name) == gsm_id).head(
                1
            )
        __result = GSM.DB.fetch_gds_results(gsm_id)
        if __result is None:
            raise KeyError(f"Requested GSM: {gsm_id} does not exists in database.")
        # target_gsm = (__result.query(f'accession == "{gsm_id}"')).to_dict()
        # del __result
        newdata = pl.DataFrame(
            {
                GSM.Scheme.accession_id.name: str(
                    __result[GSM.Scheme.accession_id.value[0]][1]
                ),
                GSM.Scheme.title.name: str(__result[GSM.Scheme.title.value[0]][0]),
                GSM.Scheme.summary.name: str(__result[GSM.Scheme.summary.value[0]][0]),
                GSM.Scheme.species.name: str(__result[GSM.Scheme.species.value[0]][0]),
                GSM.Scheme.raw_link.name: str(
                    __result[GSM.Scheme.raw_link.value[0]][0]
                ),
                GSM.Scheme.srx_id.name: str(__result[GSM.Scheme.srx_id.value[0]][0]),
                GSM.Scheme.parent_gse_id.name: str(
                    __result[GSM.Scheme.accession_id.value[0]][0]
                ),
            },
            schema={
                name: element.value[1]
                for name, element in GSM.Scheme.__members__.items()
            },
        )
        self.df = pl.concat([self.df, newdata])
        self.flush()
        return newdata
