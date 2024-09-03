import bz2
from dataclasses import dataclass
import os
import shutil
import tempfile
from typing import Iterator

from xml.etree.ElementTree import XMLPullParser
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
    chunk: bytes


def downloader(zip_file_links: Iterator[str]) -> Iterator[DownloadResult]:
    """
    Takes in links, downloads them to a temporary filename which is yielded
    """
    for link in zip_file_links:
        # cleanup of this file will happen in the next pipeline step
        with urllib.request.urlopen(link) as download_pipe:
            # download_pipe
            for chunk in download_pipe:
                yield DownloadResult(link=link, chunk=chunk)


@dataclass
class TextFileResults:
    link: str
    extracted_bytes: bytes


def unzipper(downloads: Iterator[DownloadResult]) -> Iterator[TextFileResults]:
    current_link = None
    current_decompressor = bz2.BZ2Decompressor()
    for download_chunk in downloads:
        if current_link is None:
            current_link = download_chunk.link
            current_decompressor = bz2.BZ2Decompressor()
        if current_link != download_chunk.link:
            logger.error(
                f"Didn't complete reading of bz2 file '{current_link}' before next link '{chunk.link}' hit\nContinuing despite error."
            )
            current_link = download_chunk.link
            current_decompressor = bz2.BZ2Decompressor()

        new_bytes = current_decompressor.decompress(download_chunk.chunk)
        yield TextFileResults(current_link, new_bytes)
        if current_decompressor.eof:
            current_link = None
        if current_decompressor.unused_data:
            logger.warn(
                f"Unused data after end of bz2 file: {current_decompressor.unused_data}"
            )


@dataclass
class FileSplitResult:
    link: str
    article_title: str
    article_contents: str


def file_splitter(file_chunks: Iterator[TextFileResults]) -> Iterator[FileSplitResult]:
    current_link = None
    current_parser = XMLPullParser()
    current_title = ""
    current_text = ""
    for chunk in file_chunks:
        if current_link == None:
            current_link = chunk.link
            current_parser = XMLPullParser()
        if current_link != chunk.link:
            logger.error(
                f"Didn't complete xml file '{current_link}' before next link '{chunk.link}' hit\nContinuing despite error."
            )
            current_link = chunk.link
            current_parser = XMLPullParser()

        current_parser.feed(chunk.extracted_bytes)
        for event, elem in current_parser.read_events():
            if elem.tag == "page":
                if current_text or current_title:
                    yield FileSplitResult(
                        current_link,
                        article_title=current_title,
                        article_contents=current_text,
                    )
                current_text = ""
                current_title = ""
            if elem.tag == "text":
                current_text = elem.text
            if elem.tag == "title":
                current_title = elem.text


@dataclass
class ArticleSemanticAnalysis:
    link: str
    article_title: str
    analysis



def main():
    # https://dumps.wikimedia.your.org/metawiki/20220820/metawiki-20220820-pages-articles-multistream1.xml-p1p368138.bz2
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
    full_links = [
        f"https://dumps.wikimedia.your.org/metawiki/20220820/{rem}" for rem in link_list
    ]
    print(link_list)


def test_downloader():
    out_chunks = [*downloader(["http://www.neverssl.com/"])]
    cat_chunks = b"".join(chunk.chunk for chunk in out_chunks)
    with tempfile.NamedTemporaryFile() as reg_download:
        urllib.request.urlretrieve("http://www.neverssl.com/", reg_download.name)
        reg_download.seek(0)
        reg_text_out = reg_download.read()
    assert len(reg_text_out) == len(
        cat_chunks
    ), f"{len(reg_text_out)} == {len(cat_chunks)}"
    assert reg_text_out == cat_chunks


def test_unzipper():
    orig_data = b"1234567abcdefgABC" * 1000
    compressed_bytes = bz2.compress(orig_data)
    chunk_size = 123
    in_chunks = [
        compressed_bytes[i : i + chunk_size]
        for i in range(0, len(compressed_bytes), chunk_size)
    ]
    in_chunk_objs = [DownloadResult(link="data1", chunk=chunk) for chunk in in_chunks]
    decompressed_out_chunks = [*unzipper(in_chunk_objs)]
    decompressed_result = b"".join(
        [chunk.extracted_bytes for chunk in decompressed_out_chunks]
    )
    assert decompressed_result == orig_data


def test_file_splitter():
    text_chunks = [
        "<meta><page>\n",
        "<dummy>123\n</dumm",
        'y>\n<text inner_attribute="123">\nHere is some text',
        "</text></page>\n<another_dummy></another_dummy><page>\n<text>Here is some more text on another page\n</text><title>My Page Title</title></page>\n</meta>",
    ]
    chunk_inputs = [
        TextFileResults(link="data1", extracted_bytes=chunk) for chunk in text_chunks
    ]
    splitter = [*file_splitter(chunk_inputs)]
    assert splitter == [
        FileSplitResult(
            link="data1", article_title="", article_contents="\nHere is some text"
        ),
        FileSplitResult(
            link="data1",
            article_title="My Page Title",
            article_contents="Here is some more text on another page\n",
        ),
    ]


def test():
    test_downloader()
    test_unzipper()
    test_file_splitter()


if __name__ == "__main__":
    test()
