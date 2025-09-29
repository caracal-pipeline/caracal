from typing import Dict, List
from dataclasses import dataclass
from caracal.utils.basetypes import (
    File,
    MS,
    Directory,
)

@dataclass
class Worker:
    name: str
    worker_path: File
    indir: Directory
    outdir: Directory
    msdir: Directory
    config: Dict
    datasets: List[MS]
    
    def run(self):
        pass
    