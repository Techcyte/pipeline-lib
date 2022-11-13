from dataclasses import dataclass
from typing import Callable, Optional, Dict, Any


@dataclass
class PipelineTask:
    generator: Callable
    constants: Optional[Dict[str, Any]]=None
    num_procs: int = 1
    out_buffer_size: int = 4

    @property
    def name(self):
        return self.generator.__name__
