from datetime import datetime, timezone

import dateutil.parser
import pytz


DEFAULT_TIMEZONE = pytz.timezone('US/Eastern')


def parse_timestamp(timestr: str) -> datetime:
    timestamp = dateutil.parser.parse(timestr)
    if timestamp.tzinfo is None:
        timestamp = DEFAULT_TIMEZONE.localize(
            timestamp
        ).astimezone(timezone.utc)
    return timestamp


def to_timestamp(timestamp_or_timestr) -> datetime:
    return (
        timestamp_or_timestr
        if isinstance(timestamp_or_timestr, datetime)
        else parse_timestamp(timestamp_or_timestr)
    )


def format_to_iso_timestamp(timestamp_or_timestr) -> str:
    return to_timestamp(timestamp_or_timestr).isoformat().replace('+00:00', 'Z')


def to_default_tz_display_format(timestamp: datetime) -> str:
    return timestamp.astimezone(DEFAULT_TIMEZONE).strftime(
        r'%d %b %y  %H:%M:%S'
    )


def convert_datetime_to_string(dtobj, dt_format="%Y-%m-%d %H:%M:%S"):
    return dtobj.strftime(dt_format)


def convert_datetime_string_to_datetime(
        datetime_as_string: str,
        time_format: str = "%Y-%m-%d %H:%M:%S"
) -> datetime:
    tz_unaware = datetime.strptime(datetime_as_string.strip(), time_format)
    tz_aware = tz_unaware.replace(tzinfo=tz.tzlocal())

    return tz_aware
