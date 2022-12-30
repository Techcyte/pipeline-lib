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

num messages|message size|sequential-thread|buffered-thread|parallel-thread|sequential-process-fork|buffered-process-fork|parallel-process-fork|sequential-process-spawn|buffered-process-spawn|parallel-process-spawn|sequential-coroutine|buffered-coroutine|parallel-coroutine
---|---|---|---|---|---|---|---|---|---|---|---|---|---
100|50000|0.7421092987060547|0.9426014423370361|1.2167184352874756|1.3306736946105957|0.9456183910369873|0.9424421787261963|1.4792447090148926|1.0399765968322754|1.178037166595459|0.006626605987548828|**0.0065424442291259766**|0.006624460220336914
40000500|100|0.7252142429351807|0.7077171802520752|0.7210838794708252|1.621253490447998|1.7208893299102783|2.283454656600952|1.7601733207702637|1.8722140789031982|2.427269697189331|0.6818287372589111|**0.6789970397949219**|0.6875081062316895

The above suggests a good heuristic is: "the more parallelism capabilities, the larger the overhead". Threading allows for efficient sharing of large objects, but is almost as slow as multiprocessing for small objects. Native python coroutines have effectively free communication, but no parallelism, wheras processes have completely indepent python interpreters running in parallel in best case, but significant overhead copying large and small objects around.
