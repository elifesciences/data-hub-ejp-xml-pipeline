import html

from lxml import etree
from lxml.etree import Element


def parse_xml_and_show_error_line(open_fn, **kwargs):
    try:
        with open_fn() as source:
            return etree.parse(source, **kwargs)
    except etree.XMLSyntaxError as exception:
        error_lineno = exception.lineno
        with open_fn() as source:
            for current_lineno, line in enumerate(source, 1):
                if current_lineno == error_lineno:
                    raise ValueError(
                        f'failed to parse xml line=[{line}] due to {exception}'
                    ) from exception
        raise exception


def decode_html_entities(text):
    return html.unescape(text) if text else text


def get_xml_text(node: Element, default_value=''):
    result = ''.join(node.itertext())
    return result if result else default_value


def get_and_decode_xml_text(node: Element, default_value=''):
    return decode_html_entities(get_xml_text(node, default_value=default_value))


def get_xml_child_text(parent_node: Element, child_name: str, default_value=None):
    child = parent_node.find(child_name)
    return (
        get_xml_text(child, default_value='')
        if child is not None
        else default_value
    )


def get_and_decode_xml_child_text(parent_node: Element, child_name: str, default_value=None):
    return decode_html_entities(
        get_xml_child_text(parent_node, child_name, default_value=default_value)
    )
