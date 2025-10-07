from dataclasses import dataclass
from typing import Dict
from scabha.basetypes import EmptyDictDefault
from scabha.cargo import Parameter

@dataclass
class WorkerSchema(object):
    name: str
    info: str
    inputs: Dict[str, Parameter]
    outputs: Dict[str, Parameter] = EmptyDictDefault()
