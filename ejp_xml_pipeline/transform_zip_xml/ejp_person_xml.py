import logging
from datetime import datetime
from itertools import islice
from typing import List, Optional

# pylint: disable=no-name-in-module
from lxml import etree
from lxml.etree import Element

from ejp_xml_pipeline.utils.xml_transform_util.xml import (
    get_and_decode_xml_child_text, get_and_decode_xml_text
)
from ejp_xml_pipeline.utils.xml_transform_util.timestamp import (
    format_to_iso_timestamp
)
from ejp_xml_pipeline.model.entities import PersonV2

from ejp_xml_pipeline.transform_zip_xml.parsed_document import ParsedDocument
from ejp_xml_pipeline.utils.xml_transform_util.extract import (
    extract_list, format_optional_to_iso_timestamp
)

LOGGER = logging.getLogger(__name__)

GENERATED_PERSON_ID_PREFIX = 'generated-'


class ParsedPersonDocument(ParsedDocument):
    def __init__(self, provenance, persons):
        self.provenance = provenance
        self.persons = persons

    def get_entities(self):
        return self.persons


def is_person_xml(xml_root: Element):
    return xml_root.tag == 'persons'


def membership_node_to_dict(membership_node: Element) -> dict:
    return {
        'is_enabled': membership_node.attrib['active_ind'] == '1',
        'reference_type': membership_node.attrib['member_id_type_cde'],
        'reference_value': get_and_decode_xml_child_text(
            membership_node, 'member_id'
        ),
        'start_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(membership_node, 'start_dt')
        ),
        'end_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(membership_node, 'end_dt')
        ),
        'modified_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(membership_node, 'last_update_dt')
        ),
        'modified_by_person_id': get_and_decode_xml_child_text(
            membership_node, 'last_update_p_id'
        )
    }


def role_node_to_dict(role_node: Element) -> dict:
    return {
        'role_name': role_node.attrib['role_nm'],
        'is_enabled': role_node.attrib['active_ind'] == '1',
        'start_timestamp': format_optional_to_iso_timestamp(
            role_node.attrib['start_dt']
        ),
        'end_timestamp': format_optional_to_iso_timestamp(
            role_node.attrib['end_dt']
        ),
        'modified_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(role_node, 'update_dt')
        ),
        'modified_by_person_id': get_and_decode_xml_child_text(
            role_node, 'update_p_id'
        )
    }


def address_node_to_dict(address_node: Element) -> dict:
    return {
        'is_enabled': address_node.attrib['active_ind'] == '1',
        'address_type': address_node.attrib.get('addr_type'),
        'country': get_and_decode_xml_child_text(address_node, 'country'),
        'area': get_and_decode_xml_child_text(address_node, 'state'),
        'city': get_and_decode_xml_child_text(address_node, 'city'),
        'postal_code': get_and_decode_xml_child_text(address_node, 'zip'),
        'organization': get_and_decode_xml_child_text(
            address_node, 'organization'
        ),
        'department': get_and_decode_xml_child_text(
            address_node, 'department'
        ),
        'division': get_and_decode_xml_child_text(address_node, 'division'),
        'laboratory': get_and_decode_xml_child_text(
            address_node, 'laboratory'
        ),
        'job_title': get_and_decode_xml_child_text(address_node, 'job_title'),
        'email': get_and_decode_xml_child_text(address_node, 'e_mail'),
        'telephone': get_and_decode_xml_child_text(address_node, 'telephone'),
        'address_line_1': get_and_decode_xml_child_text(address_node, 'addr1'),
        'address_line_2': get_and_decode_xml_child_text(address_node, 'addr2'),
        'address_line_3': get_and_decode_xml_child_text(address_node, 'addr3'),
        'start_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(address_node, 'start_dt')
        ),
        'end_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(address_node, 'end_dt')
        )
    }


def dates_not_available_node_to_dict(
        dates_not_available_node: Element
) -> dict:
    return {
        'start_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(
                dates_not_available_node, 'dna-start-date'
            )
        ),
        'end_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(
                dates_not_available_node, 'dna-end-date'
            )
        )
    }


def organization_node_to_dict(
    organization_node: Element
) -> dict:
    return {
        'organization_id': get_and_decode_xml_child_text(organization_node, 'org-id'),
        'organization_name': get_and_decode_xml_child_text(organization_node, 'org-name'),
        'organization_type': get_and_decode_xml_child_text(organization_node, 'org-type')
    }


def generate_person_id(source_filename: str, node_index: int) -> str:
    return f'{GENERATED_PERSON_ID_PREFIX}{source_filename}-{node_index}'


def is_generated_person_id(person_id: str) -> bool:
    return person_id.startswith(GENERATED_PERSON_ID_PREFIX)


def has_generated_person_id(person: dict) -> bool:
    return is_generated_person_id(person['person_id'])


def person_node_to_dict(
        person_node: Element,
        node_index: int,
        modified_timestamp_str: str,
        provenance: dict) -> dict:
    source_filename = provenance['source_filename']
    person_id = get_and_decode_xml_child_text(person_node, 'person-id')
    if not person_id:
        person_id = generate_person_id(
            source_filename=source_filename, node_index=node_index
        )
    return {
        'provenance': {
            **provenance,
            'node_index': node_index
        },
        'person_id': person_id,
        'modified_timestamp': format_to_iso_timestamp(
            get_and_decode_xml_child_text(
                person_node, 'profile-modify-date'
            ) or
            modified_timestamp_str
        ),
        'status': get_and_decode_xml_child_text(person_node, 'status'),
        'title': get_and_decode_xml_child_text(person_node, 'title'),
        'first_name': get_and_decode_xml_child_text(person_node, 'first-name'),
        'middle_name': get_and_decode_xml_child_text(person_node, 'middle_nm'),
        'last_name': get_and_decode_xml_child_text(person_node, 'last-name'),
        'native_name': get_and_decode_xml_child_text(person_node, 'native_nm'),
        'institution': get_and_decode_xml_child_text(
            person_node, 'institution'
        ),
        'email': get_and_decode_xml_child_text(person_node, 'email'),
        'secondary_email': get_and_decode_xml_child_text(
            person_node, 'secondary-email'
        ),
        'external_references': extract_list(
            person_node, 'memberships/membership', membership_node_to_dict
        ),
        'addresses': extract_list(
            person_node, 'addresses/address', address_node_to_dict
        ),
        'organizations': extract_list(
            person_node, 'organizations/organization', organization_node_to_dict
        ),
        'roles': extract_list(
            person_node, 'roles/role', role_node_to_dict
        ),
        'dates_not_available': extract_list(
            person_node,
            'dates-not-available/dna', dates_not_available_node_to_dict
        ),
        'keywords': extract_list(
            person_node, 'keywords/keyword', get_and_decode_xml_text
        ),
        'person_tags': extract_list(
            person_node, 'person-tags/person-tag', get_and_decode_xml_text
        ),
        'merged_into_person_ids': extract_list(
            person_node,
            'merge-info/merged-into-person-id', get_and_decode_xml_text
        ),
        'research_organisms': extract_list(
            person_node,
            'subject-area-list[@name="Research Organism(s)"]/subject-area',
            get_and_decode_xml_text
        ),
        'subject_areas': extract_list(
            person_node,
            'subject-area-list[@name="Major Subject Area(s)"]/subject-area',
            get_and_decode_xml_text
        )
    }


def get_person_nodes_with_generated_person_ids(
        person_nodes: List[Element],
        person_list: List[dict]) -> List[Element]:
    return [
        person_node
        for person_node, person in zip(person_nodes, person_list)
        if has_generated_person_id(person)
    ]


def log_no_person_id_summary(
        person_nodes_with_no_person_id: List[Element],
        total_count: int):
    no_person_id_count = len(person_nodes_with_no_person_id)
    examples = list(islice(
        (
            # pylint: disable=c-extension-no-member
            etree.tostring(person_node)
            for person_node in person_nodes_with_no_person_id
        ),
        3
    ))
    no_person_id_percentage = 100 * no_person_id_count / total_count
    LOGGER.warning(
        'xml contains %s of %s (%s percent) person entries without person ids, e.g. %s',
        no_person_id_count, total_count, no_person_id_percentage, examples
    )


def parse_xml(
        # pylint: disable=unused-argument
        xml_root: Element, modified_timestamp: datetime,
        provenance: dict) -> ParsedDocument:
    LOGGER.debug('parse_xml person xml: %s', provenance)
    modified_timestamp_str = format_to_iso_timestamp(modified_timestamp)
    person_nodes = xml_root.xpath('person')
    person_list = [
        person_node_to_dict(
            person_node,
            node_index=node_index,
            modified_timestamp_str=modified_timestamp_str,
            provenance=provenance
        )
        for node_index, person_node in enumerate(person_nodes)
    ]
    person_nodes_with_no_person_id = (
        get_person_nodes_with_generated_person_ids(
            person_nodes, person_list
        )
    )
    if person_nodes_with_no_person_id:
        log_no_person_id_summary(
            person_nodes_with_no_person_id, total_count=len(person_nodes)
        )
    LOGGER.info('number of extracted person records: %d', len(person_list))

    return ParsedPersonDocument(
        provenance=provenance,
        persons=[
            PersonV2(person) for person in person_list
        ]
    )
