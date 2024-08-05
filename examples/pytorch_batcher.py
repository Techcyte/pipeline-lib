import urllib
from collections import Counter
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas
import torch
from PIL import Image
from torch import nn

from pipeline_executor import PipelineTask, execute

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
    model_results: Iterable[np.ndarray], classmap: Dict[int, str]
) -> Iterable[Tuple[str, float]]:
    """
    Post-processes neural network results
    """
    for result in model_results:
        result_class_idx = np.argmax(result)
        result_confidence = result[result_class]
        result_class = classmap[result_class_idx]
        yield (result_class, result_confidence)


def aggregate_results(classes: Iterable[pandas.DataFrame]) -> None:
    """
    Post-processing and reporting are combined in this step for simplicity.
    There could be multiple post-processing steps if you wish.
    """
    results = list(classes)
    class_stats = Counter(name for result in results for res in result for name in res.loc[:,'name'])
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


if __name__ == "__main__":
    main()
