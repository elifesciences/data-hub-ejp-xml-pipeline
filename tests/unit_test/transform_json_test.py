from ejp_xml_pipeline.transform_json import remove_key_with_null_value


class TestRemoveKeyWithNullValue:
    def test_should_remove_none_from_dict(self):
        assert remove_key_with_null_value({
            'key1': None,
            'other': 'value'
        }) == {'other': 'value'}

    def test_should_remove_empty_string_from_dict(self):
        assert remove_key_with_null_value({
            'key1': '',
            'other': 'value'
        }) == {'other': 'value'}

    def test_should_remove_not_remove_false_from_dict(self):
        assert remove_key_with_null_value({
            'key1': False,
            'other': 'value'
        }) == {'key1': False, 'other': 'value'}
