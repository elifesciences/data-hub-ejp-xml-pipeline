import logging
from zipfile import ZipFile
from datetime import datetime
from typing import List, Iterable
import re

# pylint: disable=no-name-in-module
from lxml.etree import Element, XMLParser

from ejp_xml_pipeline.utils.xml_transform_util\
    .timestamp import (
        parse_timestamp,
        format_to_iso_timestamp
    )
from ejp_xml_pipeline.utils.xml_transform_util.xml import (
    get_xml_text, parse_xml_and_show_error_line
)
from ejp_xml_pipeline.transform_zip_xml.parsed_document import ParsedDocument

from ejp_xml_pipeline.transform_zip_xml.ejp_xml import parse_xml

LOGGER = logging.getLogger(__name__)


class ZipManifest:
    def __init__(self, modified_timestamp: datetime, filenames: List[str]):
        self.modified_timestamp = modified_timestamp
        self.filenames = filenames


def parse_go_xml(xml_root: Element) -> ZipManifest:
    return ZipManifest(
        modified_timestamp=parse_timestamp(xml_root.attrib['create_date']),
        filenames=[
            get_xml_text(node)
            for node in xml_root.findall('file_nm')
        ]
    )


def parse_zip_xml_root(zip_file: ZipFile, name: str) -> Element:
    return parse_xml_and_show_error_line(
        lambda: zip_file.open(name, 'r'),
        parser=XMLParser(recover=True)
    ).getroot()


def join_zip_and_xml_filename(zip_filename, xml_filename):
    return '%s/%s' % (zip_filename, xml_filename)


def iter_parse_xml_in_zip(
        zip_file: ZipFile, zip_filename: str, xml_filename_exclusion_regex_pattern: str
) -> Iterable[ParsedDocument]:
    imported_timestamp_str = format_to_iso_timestamp(datetime.now())
    zip_manifest = parse_go_xml(parse_zip_xml_root(zip_file, 'go.xml'))
    filenames = zip_manifest.filenames
    for filename in filenames:
        if xml_filename_exclusion_regex_pattern:
            if re.match(xml_filename_exclusion_regex_pattern, filename):
                continue

        source_filename = join_zip_and_xml_filename(zip_filename, filename)
        provenance = {
            'source_filename': source_filename,
            'imported_timestamp': imported_timestamp_str
        }
        yield parse_xml(
            parse_zip_xml_root(zip_file, filename),
            modified_timestamp=zip_manifest.modified_timestamp,
            provenance=provenance
        )
