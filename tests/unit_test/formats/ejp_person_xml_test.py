import logging
from typing import List

# pylint: disable=no-name-in-module
from lxml import etree
from lxml.builder import E
from lxml.etree import Element

from ejp_xml_pipeline.utils.xml_transform_util.timestamp import parse_timestamp

from ejp_xml_pipeline.utils.xml_transform_util.extract import MemberTypes

from ejp_xml_pipeline.transform_zip_xml.ejp_person_xml import (
    generate_person_id,
    has_generated_person_id,
    parse_xml
)

from ..utils.dict_to_xml import dict_to_xml


LOGGER = logging.getLogger(__name__)

TIMESTAMP_1 = '2018-01-01T03:04:05Z'
TIMESTAMP_2 = '2018-02-02T03:04:05Z'
TIMESTAMP_3 = '2018-02-03T03:04:05Z'
FILENAME_1 = 'file.xml'

PERSON_ID_1 = 'person1'
PERSON_ID_2 = 'person2'

ORCID_1 = 'Orcid 1'

ROLE_NAME_1 = 'Role 1'

KEYWORD_1 = 'Keyword 1'
KEYWORD_2 = 'Keyword 2'

PERSON_TAG_1 = 'Person Tag 1'
PERSON_TAG_2 = 'Person Tag 2'

RESEARCH_ORGANISM_1 = 'Research Organism 1'
RESEARCH_ORGANISM_2 = 'Research Organism 2'

SUBJECT_AREA_1 = 'Subject Area 1'
SUBJECT_AREA_2 = 'Subject Area 2'

MEMBERSHIP_1 = {
    '@active_ind': '1',
    '@member_id_type_cde': MemberTypes.ORCID,
    'member_id': ORCID_1,
    'member_id_validated': '1',
    'start_dt': TIMESTAMP_1,
    'end_dt': TIMESTAMP_2,
    'last_update_dt': TIMESTAMP_3,
    'last_update_p_id': PERSON_ID_2
}

ROLE_1 = {
    '@role_nm': ROLE_NAME_1,
    '@active_ind': '1',
    '@start_dt': TIMESTAMP_1,
    '@end_dt': TIMESTAMP_2,
    'update_dt': TIMESTAMP_3,
    'update_p_id': PERSON_ID_2
}

ADDRESS_1 = {
    '@active_ind': '1',
    '@addr_type': 'Address Type 1',
    'country': 'Country 1',
    'state': 'Area 1',
    'city': 'City 1',
    'zip': 'Post Code 1',
    'addr1': 'Address Line 1',
    'addr2': 'Address Line 2',
    'addr3': 'Address Line 3',
    'organization': 'Organization 1',
    'department': 'Department 1',
    'division': 'Division 1',
    'laboratory': 'Laboratory 1',
    'job_title': 'Job Title 1',
    'e_mail': 'Email 1',
    'telephone': 'Telephone 1',
    'start_dt': TIMESTAMP_1,
    'end_dt': TIMESTAMP_2
}

ORGANIZATION_1 = {
    'org-id': 'ORG_ID_1',
    'org-name': 'ORG_NAME_1',
    'org-type': 'ORG_TYPE_1'
}

DATES_NOT_AVAILABLE_1 = {
    'dna-start-date': TIMESTAMP_1,
    'dna-end-date': TIMESTAMP_2
}

PERSON_1 = {
    'person-id': PERSON_ID_1,
    'status': 'Active',
    'profile-modify-date': TIMESTAMP_1,
    'title': 'Title1',
    'first-name': 'First1',
    'middle_nm': 'Middle1',
    'last-name': 'Last1',
    'native_nm': 'Native1',
    'institution': 'Institution1',
    'email': 'Email1',
    'secondary-email': 'SecondaryEmail1'
}

JSON_PERSON_1 = {
    'person_id': PERSON_ID_1,
    'modified_timestamp': PERSON_1['profile-modify-date'],
    'status': PERSON_1['status'],
    'title': PERSON_1['title'],
    'first_name': PERSON_1['first-name'],
    'middle_name': PERSON_1['middle_nm'],
    'last_name': PERSON_1['last-name'],
    'native_name': PERSON_1['native_nm'],
    'institution': PERSON_1['institution'],
    'email': PERSON_1['email'],
    'secondary_email': PERSON_1['secondary-email']
}


ENCODED_TEXT = '&apos;'
DECODED_TEXT = "'"


PROVENANCE_1 = dict(source_filename=FILENAME_1)

DEFAULT_PARSE_XML_KWARGS = dict(
    modified_timestamp=parse_timestamp(TIMESTAMP_2),
    provenance=PROVENANCE_1
)


def _parse_xml_with_defaults(*args, **kwargs):
    return parse_xml(*args, **{
        **DEFAULT_PARSE_XML_KWARGS,
        **kwargs
    })


def _person_xml(person_nodes: List[Element] = None):
    root = E.persons(*(person_nodes or []))
    # pylint: disable=c-extension-no-member
    LOGGER.debug('person xml: %s', etree.tostring(root))
    return root


def _person_node(person_props: dict) -> Element:
    return dict_to_xml(
        'person', person_props
    )


class TestGeneratePersonId:
    def test_should_generate_id(self):
        assert generate_person_id(source_filename=FILENAME_1, node_index=123) == (
            f'generated-{FILENAME_1}-123'
        )


class TestHasPersonId:
    def test_should_return_false_for_regular_person_id(self):
        assert not has_generated_person_id({'person_id': '12345'})

    def test_should_return_true_for_regular_person_id(self):
        person_id = generate_person_id(source_filename=FILENAME_1, node_index=123)
        assert has_generated_person_id({'person_id': person_id})


class TestParseXml:
    def test_should_extract_id_fields(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node(
                PERSON_1
            )])
        )
        assert len(result.persons) == 1
        assert result.persons[0].data['person_id'] == PERSON_ID_1

    def test_should_generate_person_id_if_blank(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'person-id': ''
            })])
        )
        assert [p.data['person_id'] for p in result.persons] == [generate_person_id(
            source_filename=FILENAME_1, node_index=0
        )]

    def test_should_include_source_filename_in_provenance_field(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node(
                PERSON_1
            )]),
            provenance=PROVENANCE_1
        )
        provenance = result.persons[0].data['provenance']
        assert provenance['source_filename'] == PROVENANCE_1['source_filename']

    def test_should_include_node_index_in_provenance_field(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[
                _person_node(PERSON_1),
                _person_node(PERSON_1)
            ])
        )
        assert (
            [person.data['provenance']['node_index'] for person in result.persons]
            == [0, 1]
        )

    def test_should_extract_basic_fields(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1
            })])
        )
        keys = JSON_PERSON_1.keys()
        assert [
            {k: v for k, v in p.data.items() if k in keys}
            for p in result.persons
        ] == [JSON_PERSON_1]

    def test_should_decode_html_entities(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'last-name': ENCODED_TEXT
            })])
        )
        assert (
            [p.data['last_name'] for p in result.persons]
            == [DECODED_TEXT]
        )

    def test_should_use_file_modified_timestamp_if_profile_modify_date_is_blank(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'profile-modify-date': ''
            })]),
            modified_timestamp=parse_timestamp(TIMESTAMP_1)
        )
        assert len(result.persons) == 1
        assert result.persons[0].data['person_id'] == PERSON_ID_1

    def test_should_extract_memberships(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'memberships/membership': [MEMBERSHIP_1]
            })])
        )
        external_reference = result.persons[0].data['external_references'][0]
        assert external_reference == {
            # use in combination with end date
            'is_enabled': MEMBERSHIP_1['@active_ind'] == '1',
            'reference_type': MEMBERSHIP_1['@member_id_type_cde'],
            'reference_value': MEMBERSHIP_1['member_id'],
            'start_timestamp': MEMBERSHIP_1['start_dt'],
            'end_timestamp': MEMBERSHIP_1['end_dt'],
            'modified_timestamp': MEMBERSHIP_1['last_update_dt'],
            'modified_by_person_id': MEMBERSHIP_1['last_update_p_id']
        }

    def test_should_extract_roles(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'roles/role': [ROLE_1]
            })])
        )
        assert [p.data['roles'] for p in result.persons] == [[{
            'role_name': ROLE_1['@role_nm'],
            # use in combination with end date
            'is_enabled': ROLE_1['@active_ind'] == '1',
            'start_timestamp': ROLE_1['@start_dt'],
            'end_timestamp': ROLE_1['@end_dt'],
            'modified_timestamp': ROLE_1['update_dt'],
            'modified_by_person_id': ROLE_1['update_p_id']
        }]]

    def test_should_extract_roles_with_blank_timestamps(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'roles/role': [{
                    **ROLE_1,
                    '@start_dt': '',
                    '@end_dt': '',
                    'update_dt': ''
                }]
            })])
        )
        role = result.persons[0].data['roles'][0]
        assert role['start_timestamp'] is None
        assert role['end_timestamp'] is None
        assert role['modified_timestamp'] is None

    def test_should_extract_roles_with_disabled_role(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'roles/role': [{
                    **ROLE_1,
                    '@active_ind': '0'
                }]
            })])
        )
        role = result.persons[0].data['roles'][0]
        assert not role['is_enabled']

    def test_should_extract_address(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'addresses/address': [ADDRESS_1]
            })])
        )
        assert len(result.persons) == 1
        assert len(result.persons[0].data['addresses']) == 1
        assert result.persons[0].data['addresses'][0] == {
            'is_enabled': True,
            'address_type': ADDRESS_1['@addr_type'],
            'country': ADDRESS_1['country'],
            'area': ADDRESS_1['state'],
            'city': ADDRESS_1['city'],
            'postal_code': ADDRESS_1['zip'],
            'organization': ADDRESS_1['organization'],
            'department': ADDRESS_1['department'],
            'division': ADDRESS_1['division'],
            'laboratory': ADDRESS_1['laboratory'],
            'job_title': ADDRESS_1['job_title'],
            'email': ADDRESS_1['e_mail'],
            'telephone': ADDRESS_1['telephone'],
            'address_line_1': ADDRESS_1['addr1'],
            'address_line_2': ADDRESS_1['addr2'],
            'address_line_3': ADDRESS_1['addr3'],
            'start_timestamp': ADDRESS_1['start_dt'],
            'end_timestamp': ADDRESS_1['end_dt']
        }

    def test_should_extract_address_with_blank_start_end_date(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'addresses/address': [{
                    **ADDRESS_1,
                    'start_dt': '',
                    'end_dt': ''
                }]
            })])
        )
        address = result.persons[0].data['addresses'][0]
        assert address['start_timestamp'] is None
        assert address['end_timestamp'] is None

    def test_should_extract_dates_not_available(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'dates-not-available/dna': [DATES_NOT_AVAILABLE_1]
            })])
        )
        assert [p.data['dates_not_available'] for p in result.persons] == [[{
            'start_timestamp': DATES_NOT_AVAILABLE_1['dna-start-date'],
            'end_timestamp': DATES_NOT_AVAILABLE_1['dna-end-date']
        }]]

    def test_should_extract_keywords(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'keywords/keyword': [KEYWORD_1, KEYWORD_2]
            })])
        )
        keywords = result.persons[0].data['keywords']
        assert keywords == [KEYWORD_1, KEYWORD_2]

    def test_should_extract_person_tags(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'person-tags/person-tag': [PERSON_TAG_1, PERSON_TAG_2]
            })])
        )
        keywords = result.persons[0].data['person_tags']
        assert keywords == [PERSON_TAG_1, PERSON_TAG_2]

    def test_should_extract_merged_with_person_ids(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'merge-info/merged-into-person-id': [PERSON_ID_1, PERSON_ID_2]
            })])
        )
        merged_into_person_ids = result.persons[0].data['merged_into_person_ids']
        assert merged_into_person_ids == [PERSON_ID_1, PERSON_ID_2]

    def test_should_extract_research_organisms(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'subject-area-list': {
                    '@name': 'Research Organism(s)',
                    'subject-area': [RESEARCH_ORGANISM_1, RESEARCH_ORGANISM_2]
                }
            })])
        )
        research_organisms = result.persons[0].data['research_organisms']
        assert research_organisms == [RESEARCH_ORGANISM_1, RESEARCH_ORGANISM_2]

    def test_should_extract_subject_areas(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'subject-area-list': {
                    '@name': 'Major Subject Area(s)',
                    'subject-area': [SUBJECT_AREA_1, SUBJECT_AREA_2]
                }
            })])
        )
        research_organisms = result.persons[0].data['subject_areas']
        assert research_organisms == [SUBJECT_AREA_1, SUBJECT_AREA_2]

    def test_should_extract_organizations(self):
        result = _parse_xml_with_defaults(
            _person_xml(person_nodes=[_person_node({
                **PERSON_1,
                'organizations/organization': [ORGANIZATION_1]
            })])
        )
        assert [p.data['organizations'] for p in result.persons] == [[{
            'organization_id': ORGANIZATION_1['org-id'],
            'organization_name': ORGANIZATION_1['org-name'],
            'organization_type': ORGANIZATION_1['org-type']
        }]]
