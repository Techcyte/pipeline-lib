## Pipeline executor

This library allows for simple, dynamic generation of a high throughput sequential data processing pipeline in python.

While not all high throughput data processing can be described by sequential data pipelines, when it can be, this library enables clean, reliabile, testable, and performant code built on top of, simple, pythonic unit testable iterator based compute units.

### Example

(see `examples/pytorch_batcher.py`) for the complete example.

```python
# imports ...
from pipeline_executor import execute, PipelineTask

"""
Each part of the pipeline are python generators,
easily unit testable in isolation;
no multithreading or multiprocessing is necessary in each step
"""
def run_model(img_data: Iterable[np.array], model_source: str, model_name: str)->Iterable[np.ndarray]:
    model = torch.hub.load(model_source, model_name)
    for img in img_data:
        results = model(img)
        yield results


def load_images(imgs: List[str])->Iterable[np.ndarray]:
    for img in imgs:
        with urllib.request.urlopen(img) as response:
            img_bytes = response.read()
            img_pil = Image.open(img_bytes, formats=["JPEG"])
            img_numpy = np.array(img_pil)
            yield img_numpy


def remap_results(model_results: Iterable[np.array], classmap: Dict[int, str])->Iterable[Tuple[str, float]]:
    for result in model_results:
        result_class_idx = np.argmax(result)
        result_confidence = result[result_class]
        result_class = classmap[result_class_idx]
        yield (result_class, result_confidence)


def aggregate_results(classes: Iterable[Tuple[str, float]])->None:
    results = list(classes)
    class_stats = Counter(clas for clas, conf in results)
    print(class_stats)

"""
The system details of the pipeline (number of processes, max buffer size, etc)
are defined in a list of simple PipelineTask objects, then executed.

Note that in theory, this list of PipelineTask can be built dynamically,
allowing for various sorts of encapsulation to be built around this library.
"""
def main():
    imgs = [
        'https://ultralytics.com/images/zidane.jpg',
        'https://ultralytics.com/images/zidane.jpg',
        'https://ultralytics.com/images/zidane.jpg'
    ]
    execute(tasks=[
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
                "model_name": 'yolov5s', # or yolov5n - yolov5x6, custom
                "model_source": 'ultralytics/yolov5',
            },
            packets_in_flight=4,
            num_workers=2,
        ),
        PipelineTask(
            remap_results,
            constants={
                "classmap": {
                    0: "cat",
                    1: "dog",
                }
            }
        ),
        PipelineTask(
            aggregate_results
        )
    ])


```

### Compute model

A Pipeline has three parts:

1. A *source* generator, outputting a stream of work items
2. *Processor* generators, consuming a linear stream of inputs and producing stream of outputs. These streams do not have to be one-to-one. If the inputs and outputs can be handled independently (user responsible for verifying this), then these processors can be multiplexed across parallel threads.
3. A *sink*: a function that consumes an iterator, returns None

The runtime execution model has two concepts:

1. Max Packets in Flight: Max number of total packets being constructed or being consumed. A "packet" is assumped to be under construction whenever a producer or a consumer worker is running. So `packets_in_flight=1` means that the work on the data is completed fully synchronously. If the number of packets is greater than the number of workers, they are stored FIFO queue buffer.
1. Workers: A worker is an independent thread of execution working in an instance of a generator. More than one worker can potentially lead to greater throughput, depending on the implementation.

### Runtime error handling behavior

The following rules for handling errors are tested.

1. If the *source* generator stops normally before downstream *processor* or *sinks*, then the remaining workers will continue to consume thier buffers without issue.
1. If a *processor* generator stops normally **after** its upstream threads have finished, then remainder of the pipeline continues proccessing the remainder of the buffered work
1. If any *source*, *processor*, or *sink* raises an exception, the entire queue is killed and an error is raised in the main thread with a helpful error message detailing exactly which pipeline step(s) failed and with what error(s).

### Type checking

This library enforces strict type hint checking at pipeline build time through runtime type annotation introspection. So similarly to pydantic or cattrs, it will validate your pipeline based on whether the input of a processor (the first argument) in the pipeline matches the type of the output of the processor before it. Rules include:

1. First argument of any processor or sink must be an `Iterable[<some_type>]` where that type matches the return type of the previous function
1. Any source or processor function must return an `Iterable[<some_type>]`
1. All arguments other than the first are specified in the `constants` input dict to the PipelineTask (the types of these objects are not currently checked)

There are also some sanity checks on the runtime values

1. `num_workers > 0`
1. `num_workers <= MAX_NUM_WORKERS` (currently fixed at 128)
1. `num_workers <= packets_in_flight` (can deadlock if this isn't true)


## Benchmarks

This gives a rough estimation of how much overhead each parallelism technique has for different workloads. 
It is produced by running `benchmark/run_benchmark.py`. Results below are on a native linux system on a desktop.

x|sequential-thread|buffered-thread|parallel-thread|sequential-process|buffered-process|parallel-process|sequential-coroutine|buffered-coroutine|parallel-coroutine
---|---|---|---|---|---|---|---|---|---
many-small|0.16642332077026367|0.12173676490783691|0.31929612159729004|0.24623417854309082|0.16981244087219238|0.16688156127929688|0.0017588138580322266|0.0015652179718017578|**0.0015361309051513672**
few-large|0.028592824935913086|0.027452468872070312|0.029271841049194336|0.15716552734375|0.19743132591247559|0.220017671585083|0.024496078491210938|**0.024237394332885742**|0.024244308471679688

The above suggests a good heuristic is: "the more parallelism capabilities, the larger the overhead". Threading allows for efficient sharing of large objects, but is almost as slow as multiprocessing for small objects. Native python coroutines have effectively free communication, but no parallelism, wheras processes have completely indepent python interpreters running in parallel in best case, but significant overhead copying large and small objects around.
