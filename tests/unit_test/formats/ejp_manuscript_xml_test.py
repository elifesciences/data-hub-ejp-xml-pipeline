# pylint: disable=too-many-public-methods

from typing import List
# pylint: disable=no-name-in-module
from lxml.builder import E
from lxml.etree import Element

from ejp_xml_pipeline.utils.xml_transform_util.timestamp import (
    parse_timestamp, to_default_tz_display_format
)

from ejp_xml_pipeline.utils.xml_transform_util.extract import MemberTypes
from ejp_xml_pipeline.transform_zip_xml.ejp_manuscript_xml import (
    derive_version_id_from_manuscript_id_and_created_timestamp,
    parse_xml,
    OverallStageNames,
    DecisionNames,
    INITIAL_SUBMISSION_TYPE_PREFIX
)

from ..utils.dict_to_xml import dict_to_xml


TIMESTAMP_1 = '2018-01-01T03:04:05Z'
TIMESTAMP_2 = '2018-02-02T03:04:05Z'

NON_ISO_TIMESTAMP_1 = to_default_tz_display_format(
    parse_timestamp(TIMESTAMP_1)
)

COUNTRY_1 = 'Country 1'
DOI_1 = '10.7554/test1'

STAGE_NAME_1 = 'stage1'
PERSON_ID_1 = 'person1'

ROLE_1 = 'Role 1'

ORCID_1 = 'Orcid 1'

MANUSCRIPT_TYPE_1 = 'Manuscript Type 1'

MANUSCRIPT_ID_1 = '12345'
MANUSCRIPT_NUMBER_1 = '01-02-2018-RA-eLife-%s' % MANUSCRIPT_ID_1
MANUSCRIPT_FILENAME_1 = '%s.xml' % MANUSCRIPT_NUMBER_1

FUNDING_TITLE_1 = 'Funding Title'
GRANT_REFERENCE_1 = 'Grant Reference 1'

SUBJECT_AREA_1 = 'Subject Area 1'
SUBJECT_AREA_2 = 'Subject Area 2'

RESEARCH_ORGANISM_1 = 'Research Organism 1'
RESEARCH_ORGANISM_2 = 'Research Organism 2'

KEYWORD_1 = 'Keyword 1'
KEYWORD_2 = 'Keyword 2'

ADDRESS_1 = {
    'address-type': 'Address Type 1',
    'address-country': 'Country 1',
    'address-state-province': 'Province 1',
    'address-city': 'City 1',
    'address-zip-postal-code': 'Postal Code 1',
    'address-street-address-1': 'Address Line 1',
    'address-street-address-2': 'Address Line 2',
    'address-department': 'Department 1',
    'address-start-date': TIMESTAMP_1,
    'address-end-date': TIMESTAMP_2
}

PERSON_1 = {
    'person-id': PERSON_ID_1,
    'profile-modify-date': TIMESTAMP_1,
    'title': 'Title1',
    'first-name': 'First1',
    'middle-name': 'Middle1',
    'last-name': 'Last1',
    'institution': 'Institution1',
    'email': 'Email1',
    'secondary_email': 'SecondaryEmail1'
}

JSON_PERSON_1 = {
    'person_id': PERSON_ID_1,
    'modified_timestamp': PERSON_1['profile-modify-date'],
    'title': PERSON_1['title'],
    'first_name': PERSON_1['first-name'],
    'middle_name': PERSON_1['middle-name'],
    'last_name': PERSON_1['last-name'],
    'institution': PERSON_1['institution'],
    'email': PERSON_1['email'],
    'secondary_email': PERSON_1['secondary_email']
}

STAGE_1 = {
    'start-date': TIMESTAMP_1,
    'stage-name': STAGE_NAME_1,
    'stage-affective-person-id': PERSON_ID_1
}

VERSION_1 = {
    'manuscript-number': MANUSCRIPT_NUMBER_1,
    'manuscript-type': MANUSCRIPT_TYPE_1,
    'stages': [STAGE_1]
}

LIST_AND_ITEM_TAG_NAME_BY_PROP = {
    'stages': ('history', 'stage'),
    'authors': ('authors', 'author'),
    'reviewers': ('referees', 'referee'),
    'reviewing-editors': ('editors', 'editor'),
    'senior-editors': ('senior-editors', 'senior-editor'),
    'author-funding': ('author-funding', 'author-funding'),
    'themes': ('themes', 'theme'),
    'subject-areas': ('subject-areas', 'subject-area'),
    'keywords': ('keywords', 'keywords'),  # child element is indeed "keywords"
    'emails': ('emails', 'email'),
    'memberships': ('memberships', 'membership'),
    'roles': ('roles', 'role')
}

PROVENANCE_1 = dict(source_filename=MANUSCRIPT_FILENAME_1)


DEFAULT_PARSE_XML_KWARGS = dict(
    modified_timestamp=parse_timestamp(TIMESTAMP_2),
    provenance=PROVENANCE_1
)


ENCODED_TEXT = '&apos;'
DECODED_TEXT = "'"


def _parse_xml_with_defaults(*args, **kwargs):
    return parse_xml(*args, **{
        **DEFAULT_PARSE_XML_KWARGS,
        **kwargs
    })


def _nested_node_object(tag_name: str, props: dict) -> Element:
    return dict_to_xml(
        tag_name, props,
        list_and_item_tag_name_by_prop=LIST_AND_ITEM_TAG_NAME_BY_PROP
    )


def _person_node(person_props: dict) -> Element:
    return _nested_node_object('person', person_props)


def _version_node(version_props: dict) -> Element:
    return _nested_node_object('version', version_props)


def _manuscript_xml(
        version_nodes: List[Element] = None,
        person_nodes: List[Element] = None,
        country: str = COUNTRY_1,
        doi: str = DOI_1):
    return E.xml(
        E.manuscript(
            E.country(country),
            E('production-data', E('production-data-doi', doi)),
            *version_nodes or []
        ),
        E.people(
            *person_nodes or []
        )
    )


def _versions_prop(versions, key):
    return [v.data[key] for v in versions]


class TestDeriveVersionIdFromManuscriptIdAndCreatedTimestamp:
    def test_should_combine_manuscript_id_and_created_timestamp(self):
        assert derive_version_id_from_manuscript_id_and_created_timestamp(
            MANUSCRIPT_ID_1, TIMESTAMP_1
        ) == '%s/%s' % (
            MANUSCRIPT_ID_1,
            TIMESTAMP_1
        )


class TestParseXml:
    class TestPerson:
        def test_should_extract_id_fields(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml(
                    person_nodes=[_person_node(
                        PERSON_1
                    )]
                )
            )
            assert len(result.persons) == 1
            assert result.persons[0].data['person_id'] == PERSON_ID_1

        def test_should_include_provenance_field(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml(person_nodes=[_person_node(
                    PERSON_1
                )]),
                provenance=PROVENANCE_1
            )
            assert result.persons[0].data['provenance'] == PROVENANCE_1

        def test_should_extract_basic_fields(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml(person_nodes=[_person_node({
                    **PERSON_1
                })])
            )
            keys = JSON_PERSON_1.keys()
            assert [
                {
                    key: value
                    for key, value in p.data.items()
                    if key in keys
                }
                for p in result.persons
            ] == [JSON_PERSON_1]

        def test_should_decode_html_entities(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml(
                    person_nodes=[_person_node({
                        **PERSON_1,
                        'last-name': ENCODED_TEXT
                    })]
                )
            )
            assert (
                [p.data['last_name'] for p in result.persons] ==
                [DECODED_TEXT]
            )

        def test_should_use_modified_timestamp_if_profile_modify_date_blank(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml(
                    person_nodes=[_person_node({
                        **PERSON_1,
                        'profile-modify-date': ''
                    })]
                ),
                modified_timestamp=parse_timestamp(TIMESTAMP_1)
            )
            assert len(result.persons) == 1
            assert result.persons[0].data['person_id'] == PERSON_ID_1

        def test_should_extract_roles(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml(person_nodes=[_person_node({
                    **PERSON_1,
                    'roles': [{
                        'role-type': ROLE_1
                    }]
                })])
            )
            assert [p.data['roles'] for p in result.persons] == [[{
                'role_name': ROLE_1
            }]]

        def test_should_extract_memberships(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml(person_nodes=[_person_node({
                    **PERSON_1,
                    'memberships': [{
                        'member-type': MemberTypes.ORCID,
                        'member-id': ORCID_1
                    }]
                })])
            )
            assert [p.data['external_references'] for p in result.persons] == [[{
                'reference_type': MemberTypes.ORCID,
                'reference_value': ORCID_1
            }]]

        def test_should_extract_address(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml(person_nodes=[_person_node({
                    **PERSON_1,
                    'addresses/address': [ADDRESS_1]
                })])
            )
            assert [p.data['addresses'] for p in result.persons] == [[{
                'address_type': ADDRESS_1['address-type'],
                'country': ADDRESS_1['address-country'],
                'area': ADDRESS_1['address-state-province'],
                'city': ADDRESS_1['address-city'],
                'postal_code': ADDRESS_1['address-zip-postal-code'],
                'department': ADDRESS_1['address-department'],
                'address_line_1': ADDRESS_1['address-street-address-1'],
                'address_line_2': ADDRESS_1['address-street-address-2'],
                'start_timestamp': ADDRESS_1['address-start-date'],
                'end_timestamp': ADDRESS_1['address-end-date']
            }]]

        def test_should_extract_address_with_blank_start_end_date(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml(person_nodes=[_person_node({
                    **PERSON_1,
                    'addresses/address': [{
                        **ADDRESS_1,
                        'address-start-date': '',
                        'address-end-date': ''
                    }]
                })])
            )
            address = result.persons[0].data['addresses'][0]
            assert address['end_timestamp'] is None
            assert address['start_timestamp'] is None

    class TestManuscript:
        def test_should_extract_country_and_doi(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml(country=COUNTRY_1, doi=DOI_1)
            )
            assert result.manuscript.data['country'] == COUNTRY_1
            assert result.manuscript.data['doi'] == DOI_1

        def test_should_include_source_filename_in_provenance_field(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml(),
                provenance=PROVENANCE_1
            )
            assert result.manuscript.data['provenance'] == PROVENANCE_1

    class TestManuscriptVersion:
        def test_should_convert_empty_manuscript(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([])
            )
            assert result.versions == []

        def test_should_convert_one_version_with_one_stage(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'manuscript-number': MANUSCRIPT_NUMBER_1,
                    'stages': [STAGE_1]
                })])
            )
            result_versions = result.versions
            assert len(result_versions) == 1
            assert result_versions[0].data['created_timestamp'] == TIMESTAMP_1
            assert result_versions[0].data['modified_timestamp'] == TIMESTAMP_2
            assert result_versions[0].data['stages'] == [{
                'stage_timestamp': TIMESTAMP_1,
                'stage_name': STAGE_NAME_1,
                'person_id': PERSON_ID_1
            }]

        def test_should_include_provenance_field(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node(VERSION_1)]),
                provenance=PROVENANCE_1
            )
            assert result.versions[0].data['provenance'] == PROVENANCE_1

        def test_should_add_version_id(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'manuscript-number': MANUSCRIPT_NUMBER_1,
                    'stages': [STAGE_1]
                })])
            )
            expected_version_id = (
                derive_version_id_from_manuscript_id_and_created_timestamp(
                    MANUSCRIPT_ID_1, TIMESTAMP_1
                )
            )
            assert result.versions[0].data['version_id'] == expected_version_id

        def test_should_convert_non_iso_stage_date_to_iso(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'manuscript-number': MANUSCRIPT_NUMBER_1,
                    'stages': [{
                        **STAGE_1,
                        'start-date': NON_ISO_TIMESTAMP_1
                    }]
                })])
            )
            assert (
                [stage['stage_timestamp'] for stage in result.versions[0].data['stages']]
                ==
                [TIMESTAMP_1]
            )
            assert result.versions[0].data['created_timestamp'] == TIMESTAMP_1

        def test_should_extract_manuscript_id_and_long_identifier(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'manuscript-number': MANUSCRIPT_NUMBER_1
                })])
            )
            assert _versions_prop(result.versions, 'manuscript_id') == [MANUSCRIPT_ID_1]
            assert _versions_prop(result.versions, 'long_manuscript_identifier') == [
                MANUSCRIPT_NUMBER_1
            ]

        def test_should_use_manuscript_id_from_filename_if_manuscript_number_empty(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'manuscript-number': ''
                })]),
                provenance={
                    **PROVENANCE_1,
                    'source_filename': MANUSCRIPT_FILENAME_1
                }
            )
            assert _versions_prop(result.versions, 'manuscript_id') == [MANUSCRIPT_ID_1]
            assert _versions_prop(result.versions, 'long_manuscript_identifier') == [
                MANUSCRIPT_NUMBER_1
            ]

        def test_should_extract_manuscript_type(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'manuscript-type': MANUSCRIPT_TYPE_1
                })])
            )
            assert (
                _versions_prop(result.versions, 'manuscript_type')
                == [MANUSCRIPT_TYPE_1]
            )

        def test_should_extract_overall_stage_and_shorted_type_from_init_submission(self):
            full_manuscript_type = (
                '%s %s' % (INITIAL_SUBMISSION_TYPE_PREFIX, MANUSCRIPT_TYPE_1)
            )
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'manuscript-type': full_manuscript_type
                })])
            )
            assert _versions_prop(result.versions, 'overall_stage') == [
                OverallStageNames.INITIAL_SUBMISSION
            ]
            assert (
                _versions_prop(result.versions, 'manuscript_type')
                == [MANUSCRIPT_TYPE_1]
            )
            assert (
                _versions_prop(result.versions, 'full_manuscript_type')
                == [full_manuscript_type]
            )

        def test_should_extract_overall_stage_and_type_from_full_submission(self):
            full_manuscript_type = MANUSCRIPT_TYPE_1
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'manuscript-type': full_manuscript_type
                })])
            )
            assert _versions_prop(result.versions, 'overall_stage') == [
                OverallStageNames.FULL_SUBMISSION
            ]
            assert (
                _versions_prop(result.versions, 'manuscript_type')
                == [MANUSCRIPT_TYPE_1]
            )
            assert (
                _versions_prop(result.versions, 'full_manuscript_type')
                == [MANUSCRIPT_TYPE_1]
            )

        def test_should_extract_accept_decision(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'decision': DecisionNames.ACCEPT_FULL_SUBMISSION
                })])
            )
            assert _versions_prop(result.versions, 'decision') == [
                DecisionNames.ACCEPT_FULL_SUBMISSION
            ]

        def test_should_extract_decision_date(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'decision': DecisionNames.ACCEPT_FULL_SUBMISSION,
                    'decision-date': NON_ISO_TIMESTAMP_1
                })])
            )
            assert _versions_prop(result.versions, 'decision_timestamp') == [
                TIMESTAMP_1
            ]

        def test_should_extract_single_author(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'authors': [{
                        'author-person-id': PERSON_ID_1,
                        'author-seq': '1',
                        'is-corr': 'true'
                    }]
                })])
            )
            assert _versions_prop(result.versions, 'authors') == [[{
                'person_id': PERSON_ID_1,
                'sequence': 1,
                'is_corresponding_author': True
            }]]

        def test_should_extract_empty_author_seq_as_none(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'authors': [{
                        'author-person-id': PERSON_ID_1,
                        'author-seq': '',
                        'is-corr': 'true'
                    }]
                })])
            )
            assert _versions_prop(result.versions, 'authors') == [[{
                'person_id': PERSON_ID_1,
                'sequence': None,
                'is_corresponding_author': True
            }]]

        def test_should_extract_single_reviewer(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'reviewers': [{
                        'referee-person-id': PERSON_ID_1,
                        'referee-sequence': '1',
                        'referee-started-date': NON_ISO_TIMESTAMP_1,
                        'referee-due-date': NON_ISO_TIMESTAMP_1,
                        'referee-next-chase-date': NON_ISO_TIMESTAMP_1,
                        'referee-received-date': NON_ISO_TIMESTAMP_1
                    }]
                })])
            )
            assert _versions_prop(result.versions, 'reviewers') == [[{
                'person_id': PERSON_ID_1,
                'sequence': 1,
                'started_timestamp': TIMESTAMP_1,
                'due_timestamp': TIMESTAMP_1,
                'next_chase_timestamp': TIMESTAMP_1,
                'received_timestamp': TIMESTAMP_1
            }]]

        def test_should_extract_single_reviewing_editor(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'reviewing-editors': [{
                        'editor-person-id': PERSON_ID_1,
                        'editor-assigned-date': NON_ISO_TIMESTAMP_1,
                        'editor-decision-due-date': NON_ISO_TIMESTAMP_1,
                    }]
                })])
            )
            assert _versions_prop(result.versions, 'reviewing_editors') == [[{
                'person_id': PERSON_ID_1,
                'assigned_timestamp': TIMESTAMP_1,
                'due_timestamp': TIMESTAMP_1
            }]]

        def test_should_extract_single_senior_editor(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'senior-editors': [{
                        'senior-editor-person-id': PERSON_ID_1,
                        'senior-editor-assigned-date': NON_ISO_TIMESTAMP_1
                    }]
                })])
            )
            assert _versions_prop(result.versions, 'senior_editors') == [[{
                'person_id': PERSON_ID_1,
                'assigned_timestamp': TIMESTAMP_1
            }]]

        def test_should_extract_single_potential_reviewer(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'potential-referees/potential-referee': [{
                        'potential-referee-person-id': PERSON_ID_1,
                        'potential-referee-suggested-to-exclude': 'yes',
                        'potential-referee-suggested-to-include': 'no'
                    }]
                })])
            )
            assert _versions_prop(result.versions, 'potential_reviewers') == [[{
                'person_id': PERSON_ID_1,
                'suggested_to_exclude': True,
                'suggested_to_include': False
            }]]

        def test_should_return_none_if_include_exclude_are_not_yes_or_no(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'potential-referees/potential-referee': [{
                        'potential-referee-person-id': PERSON_ID_1,
                        'potential-referee-suggested-to-exclude': 'other',
                        'potential-referee-suggested-to-include': 'other'
                    }]
                })])
            )
            assert _versions_prop(result.versions, 'potential_reviewers') == [[{
                'person_id': PERSON_ID_1,
                'suggested_to_exclude': None,
                'suggested_to_include': None
            }]]

        def test_should_extract_author_funding(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'author-funding': [{
                        'author-person-id': PERSON_ID_1,
                        'funding-seq': '1',
                        'funding-title': FUNDING_TITLE_1,
                        'grant-reference-number': GRANT_REFERENCE_1
                    }]
                })])
            )
            assert _versions_prop(result.versions, 'author_funding') == [[{
                'author_person_id': PERSON_ID_1,
                'sequence': 1,
                'funding_title': FUNDING_TITLE_1,
                'grant_reference': GRANT_REFERENCE_1
            }]]

        def test_should_extract_subject_areas(self):
            # subject areas use the XML element "themes"
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'themes': [{
                        'theme': SUBJECT_AREA_1
                    }, {
                        'theme': SUBJECT_AREA_2
                    }]
                })])
            )
            assert _versions_prop(result.versions, 'subject_areas') == [[{
                'subject_area_name': SUBJECT_AREA_1
            }, {
                'subject_area_name': SUBJECT_AREA_2
            }]]

        def test_should_extract_research_organism(self):
            # research organism use the XML element "subject-areas"
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'subject-areas': [{
                        'subject-area': RESEARCH_ORGANISM_1
                    }, {
                        'subject-area': RESEARCH_ORGANISM_2
                    }]
                })])
            )
            assert _versions_prop(result.versions, 'research_organisms') == [[{
                'research_organism_name': RESEARCH_ORGANISM_1
            }, {
                'research_organism_name': RESEARCH_ORGANISM_2
            }]]

        def test_should_extract_keywords(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'keywords': [{
                        'word': KEYWORD_1
                    }, {
                        'word': KEYWORD_2
                    }]
                })])
            )
            assert _versions_prop(result.versions, 'keywords') == [[{
                'keyword': KEYWORD_1
            }, {
                'keyword': KEYWORD_2
            }]]

        def test_should_extract_email_metadata(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'emails': [{
                        'email-from': 'from@email',
                        'email-to': 'to@email',
                        'email-cc': 'cc@email',
                        'email-bcc': 'bcc@email',
                        'email-date': NON_ISO_TIMESTAMP_1,
                        'email-draft': 'Sent',
                        'email-subject': 'Subject 1',
                        'email-sender-person-id': 'sender-person',
                        'email-recipient-person-id': 'recipient-person',
                        'email-triggered-by-person-id': 'triggered-person',
                        'email-message': 'Some text to be ignored for now',
                    }]
                })])
            )
            assert _versions_prop(result.versions, 'emails') == [[{
                'from_email': 'from@email',
                'to_email': 'to@email',
                'cc_email': 'cc@email',
                'bcc_email': 'bcc@email',
                'email_timestamp': TIMESTAMP_1,
                'email_status': 'Sent',
                'subject': 'Subject 1',
                'from_person_id': 'sender-person',
                'to_person_id': 'recipient-person',
                'triggered_by_person_id': 'triggered-person'
            }]]

        def test_should_extract_manuscript_title(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'title': 'Title 1'
                })])
            )
            assert _versions_prop(result.versions, 'manuscript_title') == ['Title 1']

        def test_should_extract_abstract(self):
            result = _parse_xml_with_defaults(
                _manuscript_xml([_version_node({
                    **VERSION_1,
                    'abstract': 'Abstract 1'
                })])
            )
            assert _versions_prop(result.versions, 'abstract') == ['Abstract 1']
