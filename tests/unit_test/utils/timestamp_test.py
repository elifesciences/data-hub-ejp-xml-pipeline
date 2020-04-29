from datetime import datetime, timezone

from data_pipeline.utils.xml_transform_util.timestamp import (
    parse_timestamp,
    to_timestamp,
    format_to_iso_timestamp,
    to_default_tz_display_format
)


ISO_TIMESTAMP_1 = '2018-01-02T03:04:05Z'


class TestParseTimestamp:
    def test_should_parse_iso_date_and_time_space_separated_and_assume_us_eastern_tz(
            self
    ):
        assert parse_timestamp(
            '2018-01-02 03:04:05'
        ) == datetime(2018, 1, 2, 8, 4, 5, tzinfo=timezone.utc)

    def test_should_parse_iso_timestamp(self):
        assert parse_timestamp(
            '2018-01-02T03:04:05Z'
        ) == datetime(2018, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    def test_should_parse_display_date_and_assume_us_eastern_tz(self):
        assert parse_timestamp(
            '2nd Jan 18  03:04:05'
        ) == datetime(2018, 1, 2, 8, 4, 5, tzinfo=timezone.utc)


class TestToTimestamp:
    def test_should_return_parsed_timestamp_if_str(self):
        assert to_timestamp(ISO_TIMESTAMP_1) == parse_timestamp(
            ISO_TIMESTAMP_1
        )

    def test_should_return_passed_in_timestamp_if_datetime(self):
        parsed_timestamp = parse_timestamp(ISO_TIMESTAMP_1)
        assert to_timestamp(parsed_timestamp) == parsed_timestamp


class TestFormatToIsoTimestamp:
    def test_should_return_passed_in_timestamp_if_iso_timestamp(self):
        assert format_to_iso_timestamp(ISO_TIMESTAMP_1) == ISO_TIMESTAMP_1

    def test_should_return_passed_in_timestamp_if_datetime(self):
        parsed_timestamp = parse_timestamp(ISO_TIMESTAMP_1)
        assert format_to_iso_timestamp(parsed_timestamp) == ISO_TIMESTAMP_1


class TestToDefaultTzDisplayFormat:
    def test_should_format(self):
        assert to_default_tz_display_format(parse_timestamp(
            '2018-01-02T03:04:05Z'
        )) == '01 Jan 18  22:04:05'
