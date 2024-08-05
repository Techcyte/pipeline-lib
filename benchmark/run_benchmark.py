import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Literal

import numpy as np

from pipeline_executor import PARALLELISM_STRATEGIES, PipelineTask, execute

N_MANY_MESSAGES = 50000
N_BIG_MESSAGES = 100
BIG_MESSAGE_SIZE = 10000000
BIG_MESSAGE_BYTES = 4 * BIG_MESSAGE_SIZE + 500

MessageType = Literal["big", "small"]


def generate_messages(
    num_messages: int, message_type: MessageType
) -> Iterable[Dict[str, Any]]:
    for i in range(num_messages):
        if message_type == "small":
            yield {"message_type": "many", "message_value": 123456}
        else:
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


def run_messages(
    n_procs: int,
    packets_in_flight: int,
    parallelism_type: str,
    num_messages: int,
    message_type: MessageType,
    max_message_size: int | None,
):
    execute(
        [
            PipelineTask(
                generate_messages,
                max_message_size=max_message_size,
                constants={"num_messages": num_messages, "message_type": message_type},
            ),
            PipelineTask(
                process_message,
                max_message_size=max_message_size,
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
    num_messages = [N_MANY_MESSAGES, N_MANY_MESSAGES, N_BIG_MESSAGES, N_BIG_MESSAGES]
    message_types: list[MessageType] = ["small","small","big","big"]
    buffer_sizes = [
        BIG_MESSAGE_BYTES,
        None,
        BIG_MESSAGE_BYTES,
        None,
    ]
    markdown_lines = []
    markdown_lines.append(
        "num messages|message size|message type|"
        + "|".join(
            f"{comb.name}-{comb.parallel_type}" for comb in parameter_combinations
        )
    )
    markdown_lines.append("|".join(["---"] * (len(parameter_combinations) + 3)))
    for n_msgs, buf_size, msg_type in zip(num_messages, buffer_sizes, message_types):
        results = []
        for comb in parameter_combinations:
            start_t = time.time()
            run_messages(
                n_procs=comb.n_procs,
                packets_in_flight=comb.packets_in_flight,
                parallelism_type=comb.parallel_type,
                num_messages=n_msgs,
                message_type=msg_type,
                max_message_size=buf_size,
            )
            end_t = time.time()
            results.append(end_t - start_t)
        max_val = min(results)
        buf_type = "pipe" if buf_size is None else "shared-mem"
        msg_bytes = 100 if msg_type == "small" else BIG_MESSAGE_BYTES
        markdown_lines.append(
            f"{n_msgs}|{msg_bytes}|{buf_type}|"
            + "|".join(f"{res}" if res != max_val else f"**{res}**" for res in results)
        )
    return "\n".join(markdown_lines)


if __name__ == "__main__":
    print(benchmark_execution())
