from argparse import ArgumentParser
from enum import Enum
import os
import re
from typing import Any, Dict, List, Union


class Jobs:
    @staticmethod
    def build(template_path: str, replace_params: Dict[str, Any]) -> List[str]:
        """
        Build job script
        """
        if not os.path.isfile(template_path):
            raise FileNotFoundError(
                f"Could not find template file: {template_path}")
        target_sh = []
        with open(template_path, mode="r") as template:
            for line in template.readlines():
                for param in replace_params:
                    line = re.sub(
                        f'@{param}@', f"{replace_params[param]}", line, 10)
                target_sh.append(line)
        return target_sh


class JobSystem(Enum):
    default_bash = "default_bash"
    nohup = "nohup"
    PBS = "PBS"
