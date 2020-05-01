from typing import Callable, List
# pylint: disable=no-name-in-module
from lxml.etree import Element

from ejp_xml_pipeline.utils.xml_transform_util.timestamp import (
    format_to_iso_timestamp
)


class MemberTypes:
    ORCID = 'ORCID'


def format_optional_to_iso_timestamp(timestamp_str: str) -> str:
    return format_to_iso_timestamp(timestamp_str) if timestamp_str else None


def extract_list(
        parent_node: Element, xpath: str,
        transform_fn: Callable[[Element], dict]) -> List[dict]:
    return [
        transform_fn(node)
        for node in parent_node.xpath(xpath)
    ]
