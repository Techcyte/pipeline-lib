import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable

import numpy as np

from pipeline_executor import PARALLELISM_STRATEGIES, PipelineTask, execute

N_MANY_MESSAGES = 50000
N_BIG_MESSAGES = 100
BIG_MESSAGE_SIZE = 10000000
BIG_MESSAGE_BYTES = 4 * BIG_MESSAGE_SIZE + 500


def generate_many_messages() -> Iterable[Dict[str, Any]]:
    for _ in range(N_MANY_MESSAGES):
        yield {"message_type": "many", "message_value": 123456}


def generate_large_messages() -> Iterable[Dict[str, Any]]:
    for i in range(N_BIG_MESSAGES):
        yield {
            "message_type": "big",
            "message_value": np.arange(BIG_MESSAGE_SIZE, dtype="int32") + i,
        }


def process_message(messages: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for msg in messages:
        msg["processed"] = True
        msg["message_value"] += 1
        yield msg


def consume_messages(messages: Iterable[Dict[str, Any]]) -> None:
    for msg in messages:
        pass


def run_big_messages(n_procs: int, packets_in_flight: int, parallelism_type: str):
    execute(
        [
            PipelineTask(
                generate_large_messages,
                max_message_size=BIG_MESSAGE_BYTES,
            ),
            PipelineTask(
                process_message,
                max_message_size=BIG_MESSAGE_BYTES,
                num_workers=n_procs,
                packets_in_flight=packets_in_flight,
            ),
            PipelineTask(
                consume_messages,
                num_workers=n_procs,
                packets_in_flight=packets_in_flight,
            ),
        ],
        parallelism_type,
    )


def run_small_messages(n_procs: int, packets_in_flight: int, parallelism_type: str):
    execute(
        [
            PipelineTask(
                generate_many_messages,
                max_message_size=BIG_MESSAGE_BYTES,
            ),
            PipelineTask(
                process_message,
                num_workers=n_procs,
                packets_in_flight=packets_in_flight,
            ),
            PipelineTask(
                consume_messages,
                num_workers=n_procs,
                packets_in_flight=packets_in_flight,
            ),
        ],
        parallelism_type,
    )


@dataclass
class ParameterCombination:
    n_procs: int
    packets_in_flight: int
    name: str
    parallel_type: str


def benchmark_execution():
    parameter_combinations = [
        comb
        for parallel_type in PARALLELISM_STRATEGIES
        for comb in [
            ParameterCombination(1, 1, "sequential", parallel_type),
            ParameterCombination(1, 4, "buffered", parallel_type),
            ParameterCombination(4, 12, "parallel", parallel_type),
        ]
    ]
    functions = [run_small_messages, run_big_messages]
    markdown_lines = []
    markdown_lines.append(
        "|".join(f"{comb.name}-{comb.parallel_type}" for comb in parameter_combinations)
    )
    markdown_lines.append("|".join("---" for _ in parameter_combinations))
    for run_fn in functions:
        results = []
        for comb in parameter_combinations:
            start_t = time.time()
            run_fn(comb.n_procs, comb.packets_in_flight, comb.parallel_type)
            end_t = time.time()
            results.append(end_t - start_t)
        max_val = min(results)
        markdown_lines.append(
            "|".join(f"{res}" if res != max_val else f"**{res}**" for res in results)
        )
    return "\n".join(markdown_lines)


if __name__ == "__main__":
    print(benchmark_execution())
