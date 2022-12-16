## Pipeline executor

This library has a much simpler model of a computation: a sequential string of iterators.

While the sorts of computations that this model can support is quite limited (much more limited than a Directed Acyclic Graph, for example), it allows for exceptionally powerful reliability and performance features, and a beautiful, unit testable iterator based compute units.

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

This library enforces very strict type checking at pipeline build time.
