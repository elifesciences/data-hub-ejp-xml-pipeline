from typing import Dict, List, Optional, Tuple, Union, cast

# pylint: disable=no-name-in-module
from lxml.builder import E
from lxml.etree import Element


Props = Dict[str, str]
PropsOrText = Union[Props, str]
NestedProps = Dict[str, Union[str, List[PropsOrText]]]


def _element_props(props: NestedProps) -> NestedProps:
    return {k: v for k, v in props.items() if not k.startswith('@')}


def _attribute_props(props: NestedProps) -> NestedProps:
    return {k[1:]: v for k, v in props.items() if k.startswith('@')}


def _simple_object_node(tag_name: str, props_or_text: PropsOrText) -> Element:
    if isinstance(props_or_text, str):
        text = props_or_text
        return E(tag_name, text)
    props = cast(NestedProps, props_or_text)
    return E(
        tag_name,
        *[E(k, v) for k, v in _element_props(props).items()],
        _attribute_props(props)
    )


def dict_to_xml(
        tag_name: str,
        props: NestedProps,
        list_and_item_tag_name_by_prop: Optional[Dict[str, Tuple[str, str]]] = None) -> Element:
    if not list_and_item_tag_name_by_prop:
        list_and_item_tag_name_by_prop = {}
    node = E(tag_name, _attribute_props(props))
    for key, value in _element_props(props).items():
        if isinstance(value, dict):
            node.append(dict_to_xml(
                key, value, list_and_item_tag_name_by_prop=list_and_item_tag_name_by_prop
            ))
            continue
        list_tag_names = list_and_item_tag_name_by_prop.get(key)
        if not list_tag_names and isinstance(value, list):
            list_tag_names = cast(Tuple[str, str], key.split('/'))
        if list_tag_names:
            list_node = node
            for list_tag_name in list_tag_names[0:-1]:
                new_list_node = E(list_tag_name)
                list_node.append(new_list_node)
                list_node = new_list_node
            item_tag_name = list_tag_names[-1]
            for item_props in value:
                list_node.append(_simple_object_node(item_tag_name, item_props))
        else:
            node.append(E(key, value))
    return node
