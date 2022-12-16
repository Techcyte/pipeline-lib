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
def run_model(img_data: Iterable[np.array], model_source: str, model_name: str)->Iterable[np.array]:
    model = torch.hub.load(model_source, model_name)  
    for img in img_data:
        results = model(img)
        yield results


def load_images(imgs: List[str])->Iterable[np.array]:
    for img in imgs:
        with urllib.request.urlopen(img) as response:
            img_bytes = response.read()
            img_pil = Image.open(img_bytes, formats=["JPEG"])
            img_numpy = np.array(img_pil)
            yield img_pil


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
            }
        ),
        PipelineTask(
            run_model,
            constants={
                "model_name": 'yolov5s', # or yolov5n - yolov5x6, custom
                "model_source": 'ultralytics/yolov5',
            },
            num_procs=2,
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

1. A single *source* generator, outputting a linear stream of work items
2. *Worker* generators, consuming a linear stream of inputs and producing stream of outputs. These streams do not have to be one-to-one. If the inputs and outputs can be handled independently (user responsible for verifying this), then these workers can be multiplexed across processes.
3. A *sink*: either another function that consumes items, or an iterator that the main process can use to veiw the outputs. If it is a function that consumes items, then it can be multiplexed like a worker.

Each *source*, *worker* and *sink* is allocated its own process (TODO: support spawning threads instead for compute-light workflow steps). Users should note that python multiprocesses duplicates the entire process image, potentially leading to greatly increased memory overhead with large numbers of processes, especially if the process so far has allocated significant memory resources.

### Runtime error handling behavior

The bulk of this library's complexity is in robust error handling. The following rules for handling errors are tested (TODO: link these to the relevant unit test names)

1. If the *source* generator stops normally before downstream *workers* or *sinks*, then the remaining workers will continue to consume thier buffers without issue.
1. If a *worker* generator stops normally **after** its upstream processes have finished, then remainder of the pipeline continues proccessing the remainder of the buffered work 
1. If any *worker* generator stops **before** its last upstream *worker* or *source* stops, then the system assumes that this worker dropped important messages, and the whole pipeline is killed as soon as possible, raising an error message defailing which generator stopped early.
1. If any *source* or *worker* raises an exception, or dies with a non-zero exit code, the entire queue is killed and an error is raised in the main process with a helpful error message detailing exactly which pipeline step(s) failed and with what error(s).
1. If the *sink* raises an exception before any *source* or *worker* finishes, then all other workers in the pipeline are killed, the original error is propogated in the main process.

Note: Rule #3 above is the inspiration behind the system limitation of having only a single *source* process. It is difficult to make an educated guess to whether messages are dropped unless there is a single source of truth defining messages at the root.

### Type checking 

This library enforces strict type hint checking at pipeline build time through runtime type annotation introspection. So similarly to pydantic or cattrs, it will validate your pipeline based on whether the input of a worker (the first argument) in the pipeline matches the type of the output of the worker before it. Rules include:

1. First argument of any worker or sink must be an `Iterable[<some_type>]` where that type matches the return type of the previous function
1. Any source or worker function must return an `Iterable[<some_type>]`
1. All arguments other than the first are specified in the `constants` input dict to the PipelineTask (the types of these objects are not currently checked)

