## pipeline_lib: A streaming pipeline accelerator for Python

![Linux test badge](https://github.com/Techcyte/box-intersect-lib/actions/workflows/test-linux.yml/badge.svg)
![Build publish badge](https://github.com/Techcyte/box-intersect-lib/actions/workflows/publish.yml/badge.svg)


What Python's `multiprocessing.Pool` provides for data parallelism, this micro-framework attempts to provide for stream parallelism: high quality pythonic tooling for supporting simple, fast, pure-python parallel stream processing, with robust, pythonic error handling.

This unopinionated tooling allows users to keep control of process state so this tooling remains simple, light-weight and robust while efficiently scaling to the complexities of modern hardware (GPU management, high CPU counts, persistent database connections, etc) and modern stream-processing workflows (deep learning inference and computational biology/chemistry). It works best for fairly consistent and linear data pipelines seen in production workflows, and is expected to be less useful for more complex multi-source pipelines in ML training or scientific development.

The framework utilizes best-of-class practices in python multiprocessing with an ambition to drive multiprocessing bugs to zero. Heavy-load testing is used to try to detect parallelism bugs with brute force, and detected bugs are left open until solved and tested. Users are encouraged to rely on the framework as much as possible to minimize bugs and optimize performance; however, there is nothing preventing users from creating their own flow control using additional multiprocessing locks, thread pools, and other concurrency primitives.

## Usage

### Defining a pipeline workers

The central worker in a pipeline is a python generator (a function with a yield statement inside) that takes as its first argument another python iterator. See the code snippet below for a simple example of what this can look like. If you are unfamiliar with how python generators work, or how to write them (they are fairly rare in programming languages), you can see [these simple python docs](https://wiki.python.org/moin/Generators) for some concepts and examples.

```python
def stream(input_iter: Iterable[InputType], **kwargs)->Iterable[OutputType]:
    # global setup
    for input_element in input_iter:
        # processing input
        yield OutputType(...)
        # can yield multiple outputs for each input, or zero
        # outputs, the framework doesn't care
        yield OutputType(...)
```

### Example

(see `examples/image_batch_example.py`) for the complete example. You can run this example by installing the dev dependencies with `pip install ".[dev]"` then running `python -m examples.image_batch_example`.

```python
#...imports
from pipeline_lib import PipelineTask, execute

"""
Each of these functions are valid pipeline steps.

Note that the typings are checked at runtime for
consistency with downstream steps. So you will
get an error if it is untyped or incorrectly typed.
"""


def run_model(
    img_data: Iterable[np.ndarray], model_source: str, model_name: str
) -> Iterable[np.ndarray]:
    model = torch.hub.load(model_source, model_name)
    for img in img_data:
        results = model(img)
        yield results


def load_images(imgs: List[str]) -> Iterable[np.ndarray]:
    """
    Load images in the image list into memory and yields them.

    Note that as the first step in the pipeline, it does not need
    to accept an iterable, it can pull from a distributed queue,
    or a database, or anything else.

    Once parallelized in the pipeline-lib framework,
    these images will be loaded in parallel with the model inference
    """
    for img in imgs:
        with urllib.request.urlopen(img) as response:
            img_pil = Image.open(response, formats=["JPEG"])
            img_numpy = np.array(img_pil)
            yield img_numpy


def run_model(
    img_data: Iterable[np.ndarray], model_source: str, model_name: str
) -> Iterable[pandas.DataFrame]:
    """
    Run a model on every input from the img_data generator
    """
    model = torch.hub.load(model_source, model_name)
    for img in img_data:
        results = model(img).pandas().xyxy
        yield results


def remap_results(
    model_results: Iterable[pandas.DataFrame], classmap: Dict[int, str]
) -> Iterable[Tuple[str, float]]:
    """
    Post-processes neural network results. This example does something silly and
    chooses the highest confidence single box in an object prediction task from the scene
    """
    for result in model_results:
        df = result[0]
        result_class_idx = np.argmax(df["confidence"])
        best_row = df.loc[result_class_idx]
        result_confidence = best_row["confidence"].item()
        result_class_id = best_row["class"].item()
        result_class = classmap[result_class_id]
        yield (result_class, result_confidence)


def aggregate_results(classes: Iterable[Tuple[str, float]]) -> None:
    """
    Post-processing and reporting are combined in this step for simplicity.
    There could be multiple post-processing steps if you wish.
    """
    results = list(classes)
    class_stats = Counter(name for name, conf in results)
    print(class_stats)


def main():
    imgs = [
        "https://ultralytics.com/images/zidane.jpg",
        "https://ultralytics.com/images/zidane.jpg",
        "https://ultralytics.com/images/zidane.jpg",
    ]
    # The system details of the pipeline (number of processes, max buffer size, etc)
    # are defined in a list of simple PipelineTask objects, then executed.

    # Note that in theory, this list of PipelineTask can be built dynamically,
    # allowing for various sorts of encapsulation to be built around this library.
    execute(
        tasks=[
            PipelineTask(
                load_images,
                constants={
                    "imgs": imgs,
                },
                packets_in_flight=2,
            ),
            PipelineTask(
                run_model,
                constants={
                    "model_name": "yolov5s",  # or yolov5n - yolov5x6, custom
                    "model_source": "ultralytics/yolov5",
                },
                packets_in_flight=4,
            ),
            PipelineTask(
                remap_results,
                constants={
                    "classmap": {
                        0: "cat",
                        1: "dog",
                    }
                },
            ),
            PipelineTask(aggregate_results),
        ]
    )
```

### Step Requirements

* Each pipeline step except the last one must be a python generator that uses the `yield` syntax.
* Each pipeline step must generate data serializable by the `cloudpickle` library, which includes all pickleable data and also most other pure-python constructs including lambdas. If you have objects which are not pickeable by default (sometimes data structures from C libraries do not have pickle support built-in), you can wrap them in an object and manually create `__setstate__` and `__getstate__` methods to serialize/deserialize your data.
* To keep code quality high, each pipeline step must be type hinted (checked at runtime). This is enabled by default, to diable set `execute(...,type_check_pipeline=False)`

## Design

### Compute model

A Pipeline has three parts:

1. A *source* generator, outputting a stream of work items
2. *Processor* generators, consuming a linear stream of inputs and producing stream of outputs. These streams do not have to be one-to-one. If the inputs and outputs can be handled independently (user responsible for verifying this), then these processors can be multiplexed across parallel threads.
3. A *sink*: a function that consumes an iterator, returns None

The runtime execution model has a few key concepts:

1. Max Packets in Flight: Max number of total packets being constructed or being consumed. A "packet" is assumed to be under construction whenever a producer or a consumer worker is running. So `packets_in_flight=1` means that the work on the data is completed fully synchronously. If the number of packets is greater than the number of workers, they are stored FIFO queue buffer. See [synchronous processing section below](#synchronous-processing) for more details.
1. Workers: A worker is an independent thread of execution working in an instance of a generator. More than one worker can potentially lead to greater throughput, depending on the implementation.
1. Buffer size (*multiprocessing only*): If `max_message_size` is set, then uses a shared memory scheme to pass data between producer and consumer very efficiently (see benchmark results below). **Warning**: If the actual pickled size of the data exceeds the specified size, then an error is raised, and there is no performance cost to the buffer being too large, so having large buffers is encouraged. If the `max_message_size` is not set, then it uses a pipe to communicate arbitrary amounts of data.

#### Synchronous processing

A unique feature of the pipeline lib is *synchronous processing*, an odd feature in a parallel pipeline, but one designed to minimize total processing latency when needed.  This is particularly useful for distributed data processing systems where each worker is consuming from a shared pool of work, and should not reserve too much work for itself that it cannot process quickly.

This tradeoff latency vs bandwidth of pipeline message processing is controlled by a single parameter `packets_in_flight`. From a consumer's perspective, the `packets_in_flight` is an ordinary queue buffer size. If there are available packets that a producer has placed in the buffer, then the consumer can consume them. For example, see the following diagram, which is limited by producer capacity.

![producer bound system](docs/producer_bound.png)

From the producer side, however, it is quite different than a queue, in that the system will not yield control back to the worker until there are empty slots available to start producing. See diagram below of system which is limited by consumer capacity. The producers are blocking because all 7 slots are filled, with 5 messages stored in the buffer, waiting to be consumed, and 2 of which are being processed by consumers.

![consumer bound system](docs/consumer_bound.png)

Note the effect of having a series of tasks with `packets_in_flight=1` means that multiple steps execute sequentially. For example, in the below diagram, task 1 is being blocked on the single packet in queue 1 being released, as that is being held by task 2. However, task 2 is in turn being blocked by task 3. Note that even though task 2 is blocked, it still reserving the space on queue 1.

![sequential execution chain](docs/sequential_chain.png)


The system enforces this by not yielding control back to the producer until there is a slot available

```python
def generator():
    ...
    # will block until there is space available
    # to produce the next message
    yield message
    ...
```

### Runtime error handling behavior

The following rules for handling errors are tested.

1. If any task exits with *either* an exception *or* a non-zero process exit code, then no more packets will be passed, the whole pipeline will be asked to finish working on its packet for at least 15 seconds, and then be forcefully terminated. The first exception raised, or the first non-zero exit code encountered, will be raised as an exception from the `execute` call.

### Type checking

This library enforces strict type hint checking at pipeline build time through runtime type annotation introspection. So similarly to `pydantic` or `cattrs`, it will validate your pipeline based on whether the input of a processor (the first argument) in the pipeline matches the type of the output of the processor before it. Rules include:

1. First argument of any processor or sink must be an `Iterable[<some_type>]` where that type matches the return type of the previous function
1. Any source or processor function must return an `Iterable[<some_type>]`
1. All arguments other than the first are specified in the `constants` input dict to the PipelineTask (the types of these objects are not currently checked)

There are also some sanity checks on the runtime values

1. `num_workers > 0`
1. `num_workers <= MAX_NUM_WORKERS` (currently fixed at 128)
1. `num_workers <= packets_in_flight` (can deadlock if this isn't true)


## Benchmarks

This gives a rough estimation of how much overhead each parallelism technique has for different workloads.
It is produced by running `python -m benchmark.run_benchmark`. Results below are on a native linux system on a desktop.

num messages|message size|message type|sequential-thread|buffered-thread|parallel-thread|sequential-process-fork|buffered-process-fork|parallel-process-fork|sequential-process-spawn|buffered-process-spawn|parallel-process-spawn|sequential-coroutine|buffered-coroutine|parallel-coroutine
---|---|---|---|---|---|---|---|---|---|---|---|---|---|---
50000|100|shared-mem|0.8488271236419678|0.8710956573486328|1.2174339294433594|1.824021816253662|1.5044441223144531|1.931725025177002|1.9258394241333008|1.587036371231079|2.264118194580078|0.007179975509643555|0.007019996643066406|**0.006929636001586914**
50000|100|pipe|0.9899630546569824|0.9465904235839844|1.1442694664001465|5.144912958145142|3.8206257820129395|5.115261077880859|6.0045647621154785|4.56999659538269|5.484697580337524|0.011531591415405273|**0.011212348937988281**|0.011239290237426758
100|40000000|shared-mem|0.7200231552124023|0.6950497627258301|0.7079834938049316|5.803555727005005|4.634066820144653|5.669986724853516|5.966506242752075|4.477019309997559|5.78363037109375|**0.6917431354522705**|0.6918675899505615|0.6935021877288818
100|40000000|pipe|0.7293474674224854|0.706463098526001|0.7223975658416748|23.841850757598877|18.06218123435974|17.78823232650757|23.93111753463745|18.04807448387146|17.767908334732056|0.6972365379333496|**0.6939477920532227**|0.6940650939941406

Two insights are:

1. No matter which option is taken, message bandwidth/latency is far better when using a finite sized memory buffer configured with `max_message_size` than the unbounded message interface enabled by default.
2. Multiprocessing has more overhead than threads, which have more overhead than sequential coroutines. But of course, the amount of possible parallelism is maximized for multiprocessing, limited for threads, and missing for coroutines.
3. Shared memory communication (with fixed buffer sizes) is much faster than piped (infinite buffer size) communication, for both small metadata packets and larger numpy arrays.


## Development

Package can be installed locally with `pip install -e .`

Tests can be run with `pytest -n 8 test`

See [CONTRIBUTIONS.md](/CONTRIBUTIONS.md) for more information on what sorts of contibutions we are looking for.

## Neighboring projects

### Similar pipeline tooling

* YAPP: C++ stream processing library with similar architecture and concepts https://github.com/picanumber/yapp
* Apache Beam: Multi-language framework for multi-step data processing that supports streams. https://beam.apache.org/documentation/sdks/python-streaming/
* Huge list of pipeline projects of different levels of similarity: https://github.com/pditommaso/awesome-pipeline?tab=readme-ov-file#pipeline-frameworks--libraries

<!--
### Downstream projects

 Complete this section when we have some downstream projects which actually use this -->
