from dataclasses import dataclass
import os
import shutil
import tempfile
from typing import Iterator
from pipeline_lib import PipelineTask, execute
import urllib.request
import zipfile


def data_source(zip_file_links: list[str]) -> Iterator[str]:
    for item in zip_file_links:
        yield item


@dataclass 
class DownloadResult:
    link: str
    filepath:str 


def downloader(zip_file_links: Iterator[str]) -> Iterator[DownloadResult]:
    """
    Takes in links, downloads them to a temporary filename which is yielded
    """
    for link in zip_file_links:
        # cleanup of this file will happen in the next pipeline step
        filename,_ = tempfile.mkstemp(suffix=".zip")
        urllib.request.urlretrieve(link, filename)
        yield DownloadResult(
            link=link,
            filepath=filename
        )


@dataclass
class TextFileResults:
    link: str
    extracted_filenames:list[str] 
    

def unzipper(downloads: Iterator[DownloadResult]) -> Iterator[TextFileResults]:
    for download in downloads:
        out_dir = tempfile.mkdtemp()
        with zipfile.ZipFile(download.filepath, 'r') as zip_file:
            zip_file.extractall(out_dir)
        out_filenames = []
        for root, dir, fname in os.walk(out_dir):
            out_filenames.append(os.path.join(root, fname))
        yield out_filenames


@dataclass
class FileSplitResult:
    link:str
    filename: str
    total_filename_count: int

def 