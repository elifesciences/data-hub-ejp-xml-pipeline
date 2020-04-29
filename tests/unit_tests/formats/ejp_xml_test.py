from unittest.mock import patch, MagicMock

import pytest
from lxml.builder import E

from data_pipeline.utils.xml_transform_util.timestamp import parse_timestamp

from data_pipeline.transform_zip_xml import ejp_xml
from data_pipeline.transform_zip_xml.ejp_xml import parse_xml, ParseDocumentError


from .ejp_manuscript_xml_test import _manuscript_xml
from .ejp_person_xml_test import _person_xml


TIMESTAMP_1 = '2018-01-01T03:04:05Z'
FILENAME_1 = 'file.xml'

PARSED_TIMESTAMP_1 = parse_timestamp(TIMESTAMP_1)

PROVENANCE_1 = dict(source_filename=FILENAME_1)


@pytest.fixture(name='parse_manuscript_xml')
def _parse_manuscript_xml():
    with patch.object(ejp_xml, 'parse_manuscript_xml') as mock:
        yield mock


@pytest.fixture(name='parse_person_xml')
def _parse_person_xml():
    with patch.object(ejp_xml, 'parse_person_xml') as mock:
        yield mock


class TestParseXml:
    def test_should_call_parse_manuscript_xml_if_manuscript_xml(
            self, parse_manuscript_xml, parse_person_xml):
        root = _manuscript_xml()
        parse_xml(
            root,
            modified_timestamp=PARSED_TIMESTAMP_1,
            provenance=PROVENANCE_1
        )
        parse_manuscript_xml.assert_called_with(
            root, modified_timestamp=PARSED_TIMESTAMP_1, provenance=PROVENANCE_1
        )
        parse_person_xml.assert_not_called()

    def test_should_call_parse_person_xml_if_person_xml(
            self, parse_manuscript_xml, parse_person_xml):
        root = _person_xml()
        parse_xml(
            root,
            modified_timestamp=PARSED_TIMESTAMP_1,
            provenance=PROVENANCE_1
        )
        parse_person_xml.assert_called_with(
            root, modified_timestamp=PARSED_TIMESTAMP_1, provenance=PROVENANCE_1
        )
        parse_manuscript_xml.assert_not_called()

    @pytest.mark.parametrize("exception", [
        (KeyError('dummy')),
        (ValueError('dummy'))
    ])
    def test_should_raise_exception_parse_xml_error_on_key_error(
            self, parse_manuscript_xml: MagicMock, exception: Exception):
        parse_manuscript_xml.side_effect = exception
        with pytest.raises(ParseDocumentError):
            root = _manuscript_xml()
            parse_xml(
                root,
                modified_timestamp=PARSED_TIMESTAMP_1,
                provenance=PROVENANCE_1
            )

    def test_should_raise_error_if_unknown_xml(
            self):
        root = E.unknown()
        with pytest.raises(ParseDocumentError):
            parse_xml(
                root,
                modified_timestamp=PARSED_TIMESTAMP_1,
                provenance=PROVENANCE_1
            )
