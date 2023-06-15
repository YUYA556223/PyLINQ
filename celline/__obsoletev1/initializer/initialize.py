import sys


from celline.ncbi.genome import Genome
from celline.utils.config import Config
Config.initialize(sys.argv[3], None)
Genome.initialize()
Genome.add(
    species=sys.argv[4],
    path=sys.argv[5]
)
