
from datetime import datetime
from decimal import Decimal

__all__ = ['json_serializer']


def json_serializer(obj):
    """Function describing how things should be serialized in JSON.
    Datetime objects are saved with datetime.isoformat(), Parameter class objects use clean_dict()
    while all others use __dict__ """

    if isinstance(obj, datetime):
        return obj.isoformat()

    if isinstance(obj, Decimal):
        return float(obj)

    return obj.__dict__

