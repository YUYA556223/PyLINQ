from celline.database import GSE
from celline.data.files import FileManager
from celline.config import Config
import os
from glob import glob


class Status:
    """Manage job status"""

    def __init__(self, gse_id: str) -> None:
        self.gse_id = gse_id
        pass

    def test(self):
        target_gse = GSE.search(self.gse_id)
        if isinstance(target_gse, str):
            return target_gse

        for gsm_id in [d for d in FileManager.read_runs()["gsm_id"].to_list()
                       if d in [gsm["accession"] for gsm in target_gse.child_gsm_ids]]:
            gsm_id = str(gsm_id)
            is_empty = len(glob(
                f"{Config.PROJ_ROOT}/resources/{gsm_id}/raw/**/*", recursive=True)) == 0
            if is_empty:
                return True
        return
