from datetime import datetime
from ejp_xml_pipeline.etl_state import (
    update_object_latest_dates
)


class TestUpdateObjectLatestDates:
    def test_should_add_key_if_not_existing_in_existing_state(self):
        existing_state = {
            'key_1_*': datetime.strptime(
                '2020-01-01 01:01', '%Y-%m-%d %H:%M'
            )
        }
        new_key = 'new_key_*'
        new_key_latest_datetime_string = datetime.strptime(
            '2020-02-01 00:00:00', '%Y-%m-%d %H:%M:%S'
        )
        new_state = update_object_latest_dates(
            existing_state, new_key, new_key_latest_datetime_string
        )
        expected_new_state = {
            'key_1_*': '2020-01-01 01:01:00',
            'new_key_*': '2020-02-01 00:00:00'
        }
        assert new_state == expected_new_state

    def test_should_update_key_if_key_in_existing_state(self):
        existing_state = {
            'key_1_*': datetime.strptime(
                '2020-01-01 01:01', '%Y-%m-%d %H:%M'
            )
        }
        new_key = 'key_1_*'
        new_key_latest_datetime_string = datetime.strptime(
            '2020-02-01 00:00:00', '%Y-%m-%d %H:%M:%S'
        )
        new_state = update_object_latest_dates(
            existing_state, new_key, new_key_latest_datetime_string
        )
        expected_new_state = {
            'key_1_*': '2020-02-01 00:00:00'
        }
        assert new_state == expected_new_state

    def test_should_add_key_if_key_existing_state_is_empty(self):
        existing_state = {}
        new_key = 'key_1_*'
        new_key_latest_datetime_string = datetime.strptime(
            '2020-02-01 00:00:00', '%Y-%m-%d %H:%M:%S'
        )
        new_state = update_object_latest_dates(
            existing_state, new_key, new_key_latest_datetime_string
        )
        expected_new_state = {
            'key_1_*': '2020-02-01 00:00:00'
        }
        assert new_state == expected_new_state
