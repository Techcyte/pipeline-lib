from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional


@dataclass
class PipelineTask:
    """
    Definition of a task to place in the pipeline
    """

    generator: Callable
    constants: Optional[Dict[str, Any]] = None
    num_procs: int = 1
    out_buffer_size: int = 4

    @property
    def name(self):
        return self.generator.__name__
