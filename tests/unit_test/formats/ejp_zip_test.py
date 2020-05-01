from datetime import datetime
from io import BytesIO
from zipfile import ZipFile
from unittest.mock import patch, MagicMock
from typing import Dict

import pytest
# pylint: disable=no-name-in-module
from lxml.builder import E
from lxml import etree

from ejp_xml_pipeline.utils.xml_transform_util.timestamp import (
    parse_timestamp, format_to_iso_timestamp
)

# pylint: disable=c-extension-no-member

import ejp_xml_pipeline.transform_zip_xml.ejp_zip
from ejp_xml_pipeline.transform_zip_xml.ejp_zip import (
    parse_go_xml,
    iter_parse_xml_in_zip,
    join_zip_and_xml_filename
)


TIMESTAMP_1 = '2018-01-01 03:04:05'

ZIP_FILE_1 = 'file1.zip'
XML_FILE_1 = 'file1.xml'
XML_FILE_2 = 'file2.xml'

PROVENANCE_1 = dict(source_filename='dummy.xml')


@pytest.fixture(name='parse_xml_mock')
def _parse_xml_mock():
    with patch.object(ejp_xml_pipeline.transform_zip_xml.ejp_zip, 'parse_xml') as mock:
        yield mock


@pytest.fixture(name='datetime_mock')
def _datetime_mock():
    with patch.object(ejp_xml_pipeline.transform_zip_xml.ejp_zip, 'datetime') as mock:
        yield mock


def _create_go_xml(create_date, filenames):
    return E.file_list(
        *[E.file_nm(filename) for filename in filenames],
        create_date=create_date
    )


def _create_zip_bytes(file_map: Dict[str, bytes]) -> bytes:
    out = BytesIO()
    with ZipFile(out, 'w') as zip_file:
        for filename, data in file_map.items():
            zip_file.writestr(filename, data)
    return out.getvalue()


class TestParseGoXml:
    def test_should_parse_timestamp(self):
        zip_manifest = parse_go_xml(_create_go_xml(
            create_date=TIMESTAMP_1,
            filenames=[]
        ))
        assert zip_manifest.modified_timestamp == parse_timestamp(TIMESTAMP_1)

    def test_should_parse_file_list(self):
        zip_manifest = parse_go_xml(_create_go_xml(
            create_date=TIMESTAMP_1,
            filenames=[XML_FILE_1, XML_FILE_2]
        ))
        assert zip_manifest.filenames == [XML_FILE_1, XML_FILE_2]


class TestIterParseXmlInZip:
    def test_should_parse_single_xml(
            self,
            parse_xml_mock: MagicMock,
            datetime_mock: MagicMock
    ):
        datetime_mock.now.return_value = datetime(2001, 2, 3, 4, 5, 6)
        go_xml = _create_go_xml(
            create_date=TIMESTAMP_1,
            filenames=[XML_FILE_1]
        )
        manuscript_xml = E.xml('dummy')
        zip_bytes = _create_zip_bytes({
            'go.xml': etree.tostring(go_xml),
            XML_FILE_1: etree.tostring(manuscript_xml)
        })
        with ZipFile(BytesIO(zip_bytes), 'r') as zip_file:
            parsed_documents = list(iter_parse_xml_in_zip(
                zip_file,
                zip_filename=ZIP_FILE_1
            ))
            assert parsed_documents == [
                parse_xml_mock.return_value
            ]
            parse_xml_mock.assert_called_once()
            (call_xml_root,), call_kwargs = parse_xml_mock.call_args
            assert etree.tostring(call_xml_root) == etree.tostring(manuscript_xml)
            assert call_kwargs == {
                'modified_timestamp': parse_timestamp(TIMESTAMP_1),
                'provenance': {
                    'source_filename': join_zip_and_xml_filename(
                        ZIP_FILE_1, XML_FILE_1
                    ),
                    'imported_timestamp': format_to_iso_timestamp(
                        datetime_mock.now.return_value
                    )
                }
            }
