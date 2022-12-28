from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

DEFAULT_BUF_SIZE = 131072


@dataclass
class PipelineTask:
    """
    Definition of a task to place in the pipeline
    """

    generator: Callable
    constants: Optional[Dict[str, Any]] = None
    num_workers: int = 1
    # one packet in flight means that between both the producer and consumer,
    # only one packet ever is being processed at a time.
    # So one packet means full execution synchronization
    packets_in_flight: int = 5
    # only applicable in multiprocessing setting
    max_message_size: int = DEFAULT_BUF_SIZE

    @property
    def name(self):
        return self.generator.__name__
