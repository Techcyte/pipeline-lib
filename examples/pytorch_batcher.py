import urllib
from collections import Counter
from typing import Dict, Iterable, List, Tuple

import numpy as np
import torch
from PIL import Image
from torch import nn

from pipeline_executor import PipelineTask, execute


def run_model(
    img_data: Iterable[np.ndarray], model_source: str, model_name: str
) -> Iterable[np.ndarray]:
    model = torch.hub.load(model_source, model_name)
    for img in img_data:
        results = model(img)
        yield results


def load_images(imgs: List[str]) -> Iterable[np.ndarray]:
    for img in imgs:
        with urllib.request.urlopen(img) as response:
            img_pil = Image.open(response, formats=["JPEG"])
            img_numpy = np.ndarray(img_pil)
            yield img_numpy


def remap_results(
    model_results: Iterable[np.ndarray], classmap: Dict[int, str]
) -> Iterable[Tuple[str, float]]:
    for result in model_results:
        result_class_idx = np.argmax(result)
        result_confidence = result[result_class]
        result_class = classmap[result_class_idx]
        yield (result_class, result_confidence)


def aggregate_results(classes: Iterable[Tuple[str, float]]) -> None:
    results = list(classes)
    class_stats = Counter(clas for clas, conf in results)
    print(class_stats)


def main():
    imgs = [
        "https://ultralytics.com/images/zidane.jpg",
        "https://ultralytics.com/images/zidane.jpg",
        "https://ultralytics.com/images/zidane.jpg",
    ]
    execute(
        tasks=[
            PipelineTask(
                load_images,
                constants={
                    "imgs": imgs,
                },
            ),
            PipelineTask(
                run_model,
                constants={
                    "model_name": "yolov5s",  # or yolov5n - yolov5x6, custom
                    "model_source": "ultralytics/yolov5",
                },
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
