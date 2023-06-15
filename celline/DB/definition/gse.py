# import celline.DB._base import
from enum import Enum, unique
import polars as pl
from typing import List, Final, NamedTuple
from celline.DB._base import DBBase, Primary, Ref
from pysradb.sraweb import SRAweb
from pprint import pprint

# from celline.utils.type import pl_ptr


class GSE(DBBase):
    DB: Final[SRAweb] = SRAweb()

    class Scheme(NamedTuple):
        accession_id: str
        title: str
        summary: str
        child_gsm_ids: str
        # projna_id = Ref
        # srp_id = "srp_id"

    def __init__(self) -> None:
        self.class_name = __class__.__name__
        self.scheme = GSE.Scheme
        super().__init__()

    def exist(self, gse_id: str):
        return (
            self.df.filter(self.plptr(GSE.Scheme.accession_id) == gse_id).shape[0]
        ) != 0

    def search(self, gse_id: str):
        if self.exist(gse_id):
            return self.df.filter(pl.col(GSE.Scheme.accession_id.name) == gse_id).head(
                1
            )
        __result = GSE.DB.fetch_gds_results(gse_id)
        if __result is None:
            raise KeyError(f"Requested GSE: {gse_id} does not exists in database.")
        target_gsm = (__result.query(f'accession == "{gse_id}"')).to_dict()
        del __result
        newdata = pl.DataFrame(
            {
                GSE.Scheme.accession_id.name: str(
                    target_gsm[GSE.Scheme.accession_id.value[0]][0]
                ),
                GSE.Scheme.title.name: str(target_gsm[GSE.Scheme.title.value[0]][0]),
                GSE.Scheme.summary.name: str(
                    target_gsm[GSE.Scheme.summary.value[0]][0]
                ),
                GSE.Scheme.child_gsm_ids.name: ",".join(
                    d["accession"]
                    for d in target_gsm[GSE.Scheme.child_gsm_ids.value[0]][0]
                ),
            },
            schema={
                name: element.value[1]
                for name, element in GSE.Scheme.__members__.items()
            },
        )
        self.df = pl.concat([self.df, newdata])
        self.flush()
        return newdata
