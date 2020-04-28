from datetime import datetime

from lxml.etree import Element

from data_pipeline.transform_zip_xml.parsed_document import ParsedDocument, ParseDocumentError

from data_pipeline.transform_zip_xml.ejp_manuscript_xml import (
    parse_xml as parse_manuscript_xml,
    is_manuscript_xml
)
from data_pipeline.transform_zip_xml.ejp_person_xml import (
    parse_xml as parse_person_xml,
    is_person_xml
)


def parse_xml(
        xml_root: Element, modified_timestamp: datetime,
        provenance: dict) -> ParsedDocument:
    try:
        if is_person_xml(xml_root):
            print("this is a person v2 xml")
            return parse_person_xml(
                xml_root, modified_timestamp=modified_timestamp, provenance=provenance
            )
        if is_manuscript_xml(xml_root):
            return parse_manuscript_xml(
                xml_root, modified_timestamp=modified_timestamp, provenance=provenance
            )
    except Exception as exception:
        raise ParseDocumentError(
            provenance,
            f'failed to process {provenance} due to {exception}'
        ) from exception
    raise ParseDocumentError(
        provenance,
        f'unrecognised xml tag {xml_root.tag} (filename: {provenance})'
    )
