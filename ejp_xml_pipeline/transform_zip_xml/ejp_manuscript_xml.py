import os
import logging
from datetime import datetime
from functools import partial
import re
from typing import Tuple

# pylint: disable=no-name-in-module
from lxml.etree import Element

from ejp_xml_pipeline.utils.xml_transform_util.xml import (
    get_and_decode_xml_child_text
)
from ejp_xml_pipeline.utils.xml_transform_util.timestamp import (
    format_to_iso_timestamp
)
from ejp_xml_pipeline.transform_zip_xml.parsed_document import ParsedDocument

from ejp_xml_pipeline.model.entities import (
    Person,
    Manuscript,
    ManuscriptVersion
)

from ejp_xml_pipeline.utils.xml_transform_util.extract import (
    format_optional_to_iso_timestamp, extract_list
)

LOGGER = logging.getLogger(__name__)

INITIAL_SUBMISSION_TYPE_PREFIX = 'Initial Submission:'


class DecisionNames:
    ACCEPT_FULL_SUBMISSION = 'Accept Full Submission'
    REJECT_FULL_SUBMISSION = 'Reject Full Submission'
    REVISE_FULL_SUBMISSION = 'Revise Full Submission'


class OverallStageNames:
    INITIAL_SUBMISSION = 'Initial Submission'
    FULL_SUBMISSION = 'Full Submission'


class ParsedManuscriptDocument(ParsedDocument):
    def __init__(self, provenance, persons, manuscript, versions):
        self.provenance = provenance
        self.persons = persons
        self.manuscript = manuscript
        self.versions = versions

    def get_entities(self):
        return self.persons + [self.manuscript] + self.versions


MANUSCRIPT_NO_REGEX = re.compile(r'^.*\D-(\d{3,})\D?.*$')


def to_bool(bool_str):
    if bool_str == 'true':
        return True
    if bool_str == 'false':
        return False
    return None


def to_int(int_str):
    return int(int_str) if int_str else None


def manuscript_number_to_manuscript_id(manuscript_number: str) -> str:
    LOGGER.debug('manuscript_number: %s', manuscript_number)
    if not manuscript_number.strip():
        raise ValueError('manuscript number must not be empty')
    match = MANUSCRIPT_NO_REGEX.match(manuscript_number)
    if not match:
        LOGGER.warning(
            ' '.join([
                'unrecognised manuscript-number format: %s'
                '(falling back to full manuscript number)'
            ]),
            manuscript_number
        )
        return manuscript_number
    return match.group(1)


def filename_to_manuscript_number(filename):
    return os.path.splitext(os.path.basename(filename))[0]


def membership_node_to_dict(membership_node: Element) -> dict:
    return {
        'reference_type': get_and_decode_xml_child_text(
            membership_node, 'member-type'
        ),
        'reference_value': get_and_decode_xml_child_text(
            membership_node, 'member-id'
        ),
    }


def role_node_to_dict(role_node: Element) -> dict:
    return {
        'role_name': get_and_decode_xml_child_text(role_node, 'role-type')
    }


def address_node_to_dict(address_node: Element) -> dict:
    return {
        'address_type': get_and_decode_xml_child_text(
            address_node, 'address-type'
        ),
        'country': get_and_decode_xml_child_text(
            address_node, 'address-country'
        ),
        'area': get_and_decode_xml_child_text(
            address_node, 'address-state-province'
        ),
        'city': get_and_decode_xml_child_text(address_node, 'address-city'),
        'postal_code': get_and_decode_xml_child_text(
            address_node, 'address-zip-postal-code'
        ),
        'department': get_and_decode_xml_child_text(
            address_node, 'address-department'
        ),
        'address_line_1': get_and_decode_xml_child_text(
            address_node, 'address-street-address-1'
        ),
        'address_line_2': get_and_decode_xml_child_text(
            address_node, 'address-street-address-2'
        ),
        'start_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(address_node, 'address-start-date')
        ),
        'end_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(address_node, 'address-end-date')
        )
    }


def person_node_to_dict(
        person_node: Element,
        modified_timestamp_str: str,
        provenance: dict) -> dict:
    person_id = get_and_decode_xml_child_text(person_node, 'person-id')
    try:
        return {
            'person_id': person_id,
            'provenance': provenance,
            'modified_timestamp': format_to_iso_timestamp(
                get_and_decode_xml_child_text(
                    person_node, 'profile-modify-date'
                ) or modified_timestamp_str
            ),
            'title': get_and_decode_xml_child_text(person_node, 'title'),
            'first_name': get_and_decode_xml_child_text(
                person_node, 'first-name'
            ),
            'middle_name': get_and_decode_xml_child_text(
                person_node, 'middle-name'
            ),
            'last_name': get_and_decode_xml_child_text(
                person_node, 'last-name'
            ),
            'institution': get_and_decode_xml_child_text(
                person_node, 'institution'
            ),
            'email': get_and_decode_xml_child_text(person_node, 'email'),
            'secondary_email': get_and_decode_xml_child_text(
                person_node, 'secondary_email'
            ),
            'external_references': extract_list(
                person_node, 'memberships/membership', membership_node_to_dict
            ),
            'roles': extract_list(
                person_node, 'roles/role', role_node_to_dict
            ),
            'addresses': extract_list(
                person_node, 'addresses/address', address_node_to_dict
            )
        }
    except ValueError as exc:
        raise ValueError(
            f'failed to process person {person_id} due to {exc}'
        ) from exc


def manuscript_node_to_dict(
        manuscript_node: Element,
        modified_timestamp_str: str,
        provenance: dict,
        manuscript_id: str,
        long_manuscript_identifier: str) -> dict:
    return {
        'provenance': provenance,
        'manuscript_id': manuscript_id,
        'long_manuscript_identifier': long_manuscript_identifier,
        'modified_timestamp': modified_timestamp_str,
        'country': get_and_decode_xml_child_text(manuscript_node, 'country'),
        'doi': get_and_decode_xml_child_text(
            manuscript_node, 'production-data/production-data-doi'
        )
    }


def version_stage_node_to_dict(stage_node: Element) -> dict:
    return {
        'stage_timestamp': format_to_iso_timestamp(
            get_and_decode_xml_child_text(stage_node, 'start-date')
        ),
        'stage_name': get_and_decode_xml_child_text(stage_node, 'stage-name'),
        'person_id': get_and_decode_xml_child_text(
            stage_node, 'stage-affective-person-id'
        )
    }


def overall_stage_and_manuscript_type_from_full_manuscript_type(
        full_manuscript_type: str) -> Tuple[str, str]:
    if full_manuscript_type.startswith(INITIAL_SUBMISSION_TYPE_PREFIX):
        overall_stage = OverallStageNames.INITIAL_SUBMISSION
        manuscript_type = full_manuscript_type[
            len(INITIAL_SUBMISSION_TYPE_PREFIX):
        ].strip()
    else:
        overall_stage = OverallStageNames.FULL_SUBMISSION
        manuscript_type = full_manuscript_type
    return overall_stage, manuscript_type


def manuscript_id_and_number_from_version_node(
        version_node: Element,
        source_filename: str) -> Tuple[str, str]:
    manuscript_number = get_and_decode_xml_child_text(
        version_node, 'manuscript-number'
    )
    try:
        manuscript_id = manuscript_number_to_manuscript_id(
            manuscript_number
        )
    except ValueError:
        # fallback to filename
        manuscript_number = filename_to_manuscript_number(
            source_filename
        )
        manuscript_id = manuscript_number_to_manuscript_id(
            manuscript_number
        )
    return manuscript_id, manuscript_number


def author_node_to_dict(author_node: Element) -> dict:
    return {
        'person_id': get_and_decode_xml_child_text(
            author_node, 'author-person-id'
        ),
        'sequence': to_int(get_and_decode_xml_child_text(
            author_node, 'author-seq'
        )),
        'is_corresponding_author': to_bool(get_and_decode_xml_child_text(
            author_node, 'is-corr'
        ))
    }


def reviewer_node_to_dict(
        reviewer_node: Element,
        element_prefix: str) -> dict:
    return {
        'person_id': get_and_decode_xml_child_text(
            reviewer_node, element_prefix + 'person-id'
        ),
        'sequence': to_int(get_and_decode_xml_child_text(
            reviewer_node, element_prefix + 'sequence'
        )),
        'started_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(
                reviewer_node, element_prefix + 'started-date'
            )
        ),
        'due_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(
                reviewer_node, element_prefix + 'due-date'
            )
        ),
        'next_chase_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(
                reviewer_node, element_prefix + 'next-chase-date'
            )
        ),
        'received_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(
                reviewer_node, element_prefix + 'received-date'
            )
        )
    }


def reviewing_editor_node_to_dict(
        reviewing_editor_node: Element,
        element_prefix: str) -> dict:
    return {
        'person_id': get_and_decode_xml_child_text(
            reviewing_editor_node,
            element_prefix + 'person-id'
        ),
        'assigned_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(
                reviewing_editor_node,
                element_prefix + 'assigned-date'
            )
        ),
        'due_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(
                reviewing_editor_node,
                element_prefix + 'decision-due-date'
            )
        )
    }


def senior_editor_node_to_dict(senior_editor_node: Element) -> dict:
    return {
        'person_id': get_and_decode_xml_child_text(
            senior_editor_node, 'senior-editor-person-id'
        ),
        'assigned_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(
                senior_editor_node, 'senior-editor-assigned-date'
            )
        )
    }


def _parse_yes_no(yes_no: str) -> bool:
    if not yes_no:
        return None
    if yes_no.lower() == 'yes':
        return True
    if yes_no.lower() == 'no':
        return False
    return None


def potential_person_node_to_dict(
        potential_person_node: Element,
        element_prefix: str) -> dict:
    return {
        'person_id': get_and_decode_xml_child_text(
            potential_person_node, element_prefix + 'person-id'
        ),
        'suggested_to_include': _parse_yes_no(get_and_decode_xml_child_text(
            potential_person_node, element_prefix + 'suggested-to-include'
        )),
        'suggested_to_exclude': _parse_yes_no(get_and_decode_xml_child_text(
            potential_person_node, element_prefix + 'suggested-to-exclude'
        ))
    }


def author_funding_node_to_dict(author_funding_node: Element) -> dict:
    return {
        'author_person_id': get_and_decode_xml_child_text(
            author_funding_node, 'author-person-id'
        ),
        'sequence': to_int(
            get_and_decode_xml_child_text(author_funding_node, 'funding-seq')
        ),
        'funding_title': get_and_decode_xml_child_text(
            author_funding_node, 'funding-title'
        ),
        'grant_reference': get_and_decode_xml_child_text(
            author_funding_node, 'grant-reference-number'
        )
    }


def subject_area_node_to_dict(subject_area_node: Element) -> dict:
    return {
        'subject_area_name':
            get_and_decode_xml_child_text(subject_area_node, 'theme')
    }


def research_organism_node_to_dict(research_organism_node: Element) -> dict:
    return {
        'research_organism_name': get_and_decode_xml_child_text(
            research_organism_node, 'subject-area'
        )
    }


def keyword_node_to_dict(keyword_node: Element) -> dict:
    return {
        'keyword': get_and_decode_xml_child_text(keyword_node, 'word')
    }


def email_node_to_dict(email_node: Element) -> dict:
    return {
        'from_email': get_and_decode_xml_child_text(email_node, 'email-from'),
        'to_email': get_and_decode_xml_child_text(email_node, 'email-to'),
        'cc_email': get_and_decode_xml_child_text(email_node, 'email-cc'),
        'bcc_email': get_and_decode_xml_child_text(email_node, 'email-bcc'),
        'email_timestamp': format_optional_to_iso_timestamp(
            get_and_decode_xml_child_text(email_node, 'email-date')
        ),
        'email_status': get_and_decode_xml_child_text(
            email_node, 'email-draft'
        ),
        'subject': get_and_decode_xml_child_text(email_node, 'email-subject'),
        'from_person_id': get_and_decode_xml_child_text(
            email_node, 'email-sender-person-id'
        ),
        'to_person_id': get_and_decode_xml_child_text(
            email_node, 'email-recipient-person-id'
        ),
        'triggered_by_person_id': get_and_decode_xml_child_text(
            email_node, 'email-triggered-by-person-id'
        )
    }


def derive_version_id_from_manuscript_id_and_created_timestamp(
        manuscript_id: str,
        created_timestamp: str) -> str:
    if created_timestamp:
        return '%s/%s' % (manuscript_id, created_timestamp)
    else:
        return 'NotAcceptable %s/%s' % (manuscript_id, created_timestamp)


def version_node_to_dict(
        version_node: Element,
        modified_timestamp_str: str,
        provenance: dict) -> dict:
    stages = [
        version_stage_node_to_dict(stage_node)
        for stage_node in version_node.findall('history/stage')
    ]
    if stages:
        first_stage = stages[0]
        created_timestamp = first_stage['stage_timestamp']
    else:
        created_timestamp = None

    source_filename = provenance['source_filename']
    manuscript_id, manuscript_number = (
        manuscript_id_and_number_from_version_node(
            version_node, source_filename=source_filename
        )
    )

    full_manuscript_type = get_and_decode_xml_child_text(
        version_node, 'manuscript-type'
    )
    overall_stage, manuscript_type = (
        overall_stage_and_manuscript_type_from_full_manuscript_type(
            full_manuscript_type
        )
    )

    decision = get_and_decode_xml_child_text(version_node, 'decision')
    decision_timestamp_str = get_and_decode_xml_child_text(
        version_node, 'decision-date'
    )
    decision_timestamp = format_to_iso_timestamp(
        decision_timestamp_str
    ) if decision_timestamp_str else None

    return {
        'provenance': provenance,
        'created_timestamp': created_timestamp,
        'modified_timestamp': modified_timestamp_str,
        'manuscript_id': manuscript_id,
        'long_manuscript_identifier': manuscript_number,
        'full_manuscript_type': full_manuscript_type,
        'manuscript_type': manuscript_type,
        'version_id': (
            derive_version_id_from_manuscript_id_and_created_timestamp(
                manuscript_id, created_timestamp
            )
        ),
        'manuscript_title': get_and_decode_xml_child_text(
            version_node, 'title'
        ),
        'abstract': get_and_decode_xml_child_text(version_node, 'abstract'),
        'overall_stage': overall_stage,
        'decision': decision,
        'decision_timestamp': decision_timestamp,
        'stages': stages,
        'authors': extract_list(
            version_node, 'authors/author', author_node_to_dict
        ),
        'reviewers': extract_list(
            version_node, 'referees/referee',
            partial(reviewer_node_to_dict, element_prefix='referee-')
        ) + extract_list(
            version_node, 'reviewers/reviewer',
            partial(reviewer_node_to_dict, element_prefix='reviewer-')
        ),
        'reviewing_editors': extract_list(
            version_node, 'editors/editor',
            partial(reviewing_editor_node_to_dict, element_prefix='editor-')
        ) + extract_list(
            version_node, 'reviewing-editors/reviewing-editor',
            partial(reviewing_editor_node_to_dict, element_prefix='reviewing-editor-')
        ),
        'senior_editors': extract_list(
            version_node, 'senior-editors/senior-editor',
            senior_editor_node_to_dict
        ),
        'potential_reviewers': extract_list(
            version_node, 'potential-referees/potential-referee',
            partial(potential_person_node_to_dict, element_prefix='potential-referee-')
        ) + extract_list(
            version_node, 'potential-reviewers/potential-reviewer',
            partial(potential_person_node_to_dict, element_prefix='potential-reviewer-')
        ),
        'potential_reviewing_editors': extract_list(
            version_node, 'potential-reviewing-editors/potential-reviewing-editor',
            partial(potential_person_node_to_dict, element_prefix='potential-reviewing-editor-')
        ),
        'potential_senior_editors': extract_list(
            version_node, 'potential-senior-editors/potential-senior-editor',
            partial(potential_person_node_to_dict, element_prefix='potential-senior-editor-')
        ),
        'author_funding': extract_list(
            version_node, 'author-funding/author-funding',
            author_funding_node_to_dict
        ),
        'subject_areas': extract_list(
            version_node, 'themes/theme', subject_area_node_to_dict
        ),
        'research_organisms': extract_list(
            version_node, 'subject-areas/subject-area',
            research_organism_node_to_dict
        ),
        'keywords': extract_list(
            version_node, 'keywords/keywords', keyword_node_to_dict
        ),
        'emails': extract_list(
            version_node, 'emails/email', email_node_to_dict
        )
    }


def is_manuscript_xml(xml_root: Element):
    return xml_root.tag == 'xml'


def parse_xml(
        xml_root: Element, modified_timestamp: datetime,
        provenance: dict) -> ParsedManuscriptDocument:
    source_filename = provenance['source_filename']
    modified_timestamp_str = format_to_iso_timestamp(modified_timestamp)
    person_nodes = xml_root.xpath('people/person')
    manuscript_node = xml_root.find('manuscript')
    version_nodes = xml_root.xpath('manuscript/version')
    version_jsons = [
        version_node_to_dict(
            version_node,
            modified_timestamp_str=modified_timestamp_str,
            provenance=provenance
        )
        for version_node in version_nodes
    ]
    if version_jsons:
        manuscript_id = version_jsons[0]['manuscript_id']
        long_manuscript_identifier = (
            version_jsons[0]['long_manuscript_identifier']
        )
    else:
        long_manuscript_identifier = filename_to_manuscript_number(
            source_filename
        )
        manuscript_id = manuscript_number_to_manuscript_id(
            long_manuscript_identifier
        )
    return ParsedManuscriptDocument(
        provenance=provenance,
        persons=[
            Person(person_node_to_dict(
                person_node,
                modified_timestamp_str=modified_timestamp_str,
                provenance=provenance
            ))
            for person_node in person_nodes
        ],
        manuscript=Manuscript(manuscript_node_to_dict(
            manuscript_node,
            modified_timestamp_str=modified_timestamp_str,
            provenance=provenance,
            manuscript_id=manuscript_id,
            long_manuscript_identifier=long_manuscript_identifier
        )),
        versions=[
            ManuscriptVersion(version_json)
            for version_json in version_jsons
        ]
    )
