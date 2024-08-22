import bz2
from dataclasses import dataclass
import os
import shutil
import tempfile
from typing import Iterator
from pipeline_lib import PipelineTask, execute
import urllib.request
import zipfile
import logging

logger = logging.Logger(__name__)


def data_source(zip_file_links: list[str]) -> Iterator[str]:
    for item in zip_file_links:
        yield item


@dataclass 
class DownloadResult:
    link: str
    chunk:bytes 


def downloader(zip_file_links: Iterator[str]) -> Iterator[DownloadResult]:
    """
    Takes in links, downloads them to a temporary filename which is yielded
    """
    for link in zip_file_links:
        # cleanup of this file will happen in the next pipeline step
        with urllib.request.urlopen(link) as download_pipe:
            # download_pipe
            for chunk in download_pipe:
                yield  DownloadResult(
                    link=link,
                    chunk=chunk
                )


@dataclass
class TextFileResults:
    link: str
    extracted_bytes:bytes 


def unzipper(downloads: Iterator[DownloadResult]) -> Iterator[TextFileResults]:
    current_link = None
    current_decompressor = bz2.BZ2Decompressor()
    for download_chunk in downloads:
        if current_link is None:
            current_link = download_chunk.link
        if current_link != download_chunk.link:
            logger.error(f"Incomplete bz2 file: {current_link}")
            logger.error(f"Continuing despite error....")
            current_link = download_chunk.link
    
        new_bytes = current_decompressor.decompress(download_chunk.chunk)
        yield TextFileResults(current_link, new_bytes)
        if current_decompressor.eof:
            current_link = None
        if current_decompressor.unused_data:
            logger.warn(f"Unused data after end of bz2 file: {current_decompressor.unused_data}")


@dataclass
class FileSplitResult:
    link:str
    filename: str
    total_filename_count: int


def main():
    #https://dumps.wikimedia.your.org/metawiki/20220820/metawiki-20220820-pages-articles-multistream1.xml-p1p368138.bz2
    link_list = """
        metawiki-20220820-pages-articles-multistream1.xml-p1p368138.bz2
        metawiki-20220820-pages-articles-multistream2.xml-p368139p1868138.bz2
        metawiki-20220820-pages-articles-multistream2.xml-p1868139p2303800.bz2
        metawiki-20220820-pages-articles-multistream3.xml-p2303801p3803800.bz2
        metawiki-20220820-pages-articles-multistream3.xml-p3803801p4964617.bz2
        metawiki-20220820-pages-articles-multistream4.xml-p4964618p6464617.bz2
        metawiki-20220820-pages-articles-multistream4.xml-p6464618p7711374.bz2
        metawiki-20220820-pages-articles-multistream5.xml-p7711375p9211374.bz2
        metawiki-20220820-pages-articles-multistream5.xml-p9211375p10191021.bz2
        metawiki-20220820-pages-articles-multistream6.xml-p10191022p11691021.bz2
        metawiki-20220820-pages-articles-multistream6.xml-p11691022p11940817.bz2
    """.split()
    full_links = [f"https://dumps.wikimedia.your.org/metawiki/20220820/{rem}" for rem in link_list]
    print(link_list)