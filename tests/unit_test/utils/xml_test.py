# pylint: disable=no-name-in-module
from lxml.builder import E

from ejp_xml_pipeline.utils.xml_transform_util.xml import (
    decode_html_entities,
    get_and_decode_xml_child_text
)


ENCODED_TEXT = '&apos;'
DECODED_TEXT = "'"


class TestDecodeHtmlEntities:
    def test_should_return_none_if_text_is_none(self):
        assert decode_html_entities(None) is None

    def test_should_return_unchanged_text_without_entities(self):
        assert decode_html_entities('test') == 'test'

    def test_should_decode_hex_entity(self):
        assert decode_html_entities('&#x00FC;') == 'ü'

    def test_should_decode_named_entity(self):
        assert decode_html_entities('&apos;') == "'"


class TestGetAndDecodeXmlChildText:
    def test_should_return_none_if_child_element_was_not_found(self):
        assert get_and_decode_xml_child_text(
            E.parent(E.other(ENCODED_TEXT)),
            'child'
        ) is None

    def test_should_return_empty_string_if_child_element_is_empty(self):
        assert get_and_decode_xml_child_text(
            E.parent(E.child('')),
            'child'
        ) == ''

    def test_should_decode_text(self):
        assert get_and_decode_xml_child_text(
            E.parent(E.child(ENCODED_TEXT)),
            'child'
        ) == DECODED_TEXT
