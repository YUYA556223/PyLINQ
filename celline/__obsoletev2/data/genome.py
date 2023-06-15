import os
from typing import Dict, List, Optional

import toml  # type:ignore

from celline.data.config import Config


class Genome:

    __genomes: Dict[str, str] = {}

    @staticmethod
    def initialize():
        path = f"{Config.EXEC_ROOT}/genomes.toml"
        if not os.path.isfile(path):
            with open(path, mode="w") as f:
                toml.dump(Genome.__genomes, f)
        with open(path, mode="r") as f:
            Genome.__genomes = toml.load(f)

    @staticmethod
    def add(species: str, path: str):
        if species not in Genome.__genomes:
            Genome.__genomes[species] = path
        Genome.__flush()

    @staticmethod
    def remove(species: str):
        if species not in Genome.__genomes:
            raise FileNotFoundError(
                f"Could not find reference genome for {species}"
            )
        del Genome.__genomes[species]
        Genome.__flush()

    @staticmethod
    def get(species: str) -> Optional[str]:
        if species not in Genome.__genomes:
            return None
        if not os.path.isdir(Genome.__genomes[species]):
            return None
        return Genome.__genomes[species]

    @staticmethod
    def __flush():
        with open(f"{Config.EXEC_ROOT}/genomes.toml", mode="w") as f:
            toml.dump(Genome.__genomes, f)
