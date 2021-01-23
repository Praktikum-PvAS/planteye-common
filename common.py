from datetime import timezone, datetime


def utc_to_timestamp(utc_dt: datetime) -> int:
    """
    Converts received OPC UA server source timestamp (UTC) into local timestamp
    :param utc_dt: OPC UA server source timestamp (UTC) as datetime
    :return: Timestamp in local timezone as integer value
    """
    return int(datetime.timestamp(utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)) * 1000)
