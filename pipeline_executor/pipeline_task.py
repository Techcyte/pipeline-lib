from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

DEFAULT_BUF_SIZE = 131072
DEFAULT_NUM_WORKERS = 1
DEFAULT_PACKETS_IN_FLIGHT = 1


@dataclass
class PipelineTask:
    """
    Definition of a task to place in the pipeline
    """

    generator: Callable
    constants: Optional[Dict[str, Any]] = None
    num_workers: int = DEFAULT_NUM_WORKERS
    # one packet in flight means that between both the producer and consumer,
    # only one packet ever is being processed at a time.
    # So one packet means full execution synchronization
    packets_in_flight: int = DEFAULT_PACKETS_IN_FLIGHT
    # only applicable in multiprocessing setting
    # if set to None, then no limit on message size, but
    # message reads will be much slower
    max_message_size: Optional[int] = None
    # when max_message_size is set, the downstream pipeline reads memory from
    # a shared buffer without copying. This is valid if the downstream pipeline step
    # deletes all references to all the data it receives from the previous iteration
    # of the iterable
    # default is to copy this buffer to guarantee no data races
    shared_buffer: bool = False

    @property
    def name(self) -> str:
        return self.generator.__name__

    @property
    def constants_dict(self) -> Dict[str, Any]:
        return {} if self.constants is None else self.constants


class TaskError(RuntimeError):
    """Error for miscellaneous, unidentifiable issues that come up during task execution"""


class InactivityError(RuntimeError):
    """Error for when the inactivity timeout expires without any messages being passed"""
