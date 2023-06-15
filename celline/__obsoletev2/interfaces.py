from celline.config import Config, Setting
import os
import subprocess
from celline.database import NCBI, GSE, GSM


class Project:
    """
    Celline project
    """

    def __init__(self, project_dir: str, proj_name="", r_path="") -> None:
        """
        Load new celline project
        """

        def getRPath() -> str:
            proc = subprocess.Popen("which R", stdout=subprocess.PIPE, shell=True)
            result = proc.communicate()
            return result[0].decode("utf-8").replace("\n", "")

        def getDefaultProjName() -> str:
            return os.path.basename(project_dir)

        Config.EXEC_ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        Config.PROJ_ROOT = project_dir
        if not os.path.isfile(f"{Config.PROJ_ROOT}/setting.toml"):
            Setting.name = getDefaultProjName() if proj_name == "" else proj_name
            Setting.r_path = getRPath() if r_path == "" else r_path
            Setting.version = "0.01"
            Setting.wait_time = 4
            Setting.flush()
        pass

    def add_from_project_id(self, project_id: str):
        """
        Add GSE project
        """
        NCBI.add(project_id)
        return self

    def dump(self):
        return

    # def observable(self) -> Observable:
    #     return Observable(self)
