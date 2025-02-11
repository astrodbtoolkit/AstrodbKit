# Testing for utils

import json
from datetime import datetime
from decimal import Decimal
from io import StringIO

import pytest
from astropy.table import Table

from astrodbkit.utils import _name_formatter, datetime_json_parser, get_simbad_names, json_serializer

try:
    import mock
except ImportError:
    from unittest import mock


@pytest.mark.parametrize('test_input, expected', [
    ('TWA  27', 'TWA 27'),
    ('HIDDEN A', None),
    ('V* V4046 Sgr', 'V4046 Sgr'),
    ('** CVN   12A', 'CVN 12A')
])
def test_name_formatter(test_input, expected):
    assert _name_formatter(test_input) == expected


def test_json_serializer():
    data = {'date': datetime(2018, 12, 6, 12, 30, 0),
            'value': Decimal(2.3),
            'integer': 4,
            'bytes': b'byte'}

    json_text = json.dumps(data, indent=4, default=json_serializer)
    with StringIO(json_text) as f:
        new_data = json.load(f)

    assert new_data['date'] == '2018-12-06T12:30:00'
    assert new_data['value'] == pytest.approx(2.3)
    assert new_data['integer'] == 4
    assert new_data['bytes'] == 'byte'


def test_datetime_json_parser():
    json_dict = {'date': '2018-12-06T12:30:00', 'name': 'TWA 23', 'number': 4.5}
    new_dict = datetime_json_parser(json_dict)
    assert new_dict['date'] == datetime.fromisoformat('2018-12-06T12:30:00')
    assert isinstance(new_dict['date'], datetime)
    assert isinstance(new_dict['name'], str)
    assert isinstance(new_dict['number'], float)


@mock.patch('astrodbkit.utils.Simbad.query_objectids')
def test_get_simbad_names(mock_simbad):
    mock_simbad.return_value = Table({'id': ['name 1', 'name 2', 'V* name 3', 'HIDDEN name']})
    t = get_simbad_names('twa 27')
    assert len(t) == 3
    assert t[2] == 'name 3'

    # Example with no Simbad match
    mock_simbad.return_value = None
    t = get_simbad_names('WISEU J005559.88+594745.0')
    assert len(t) == 1
    assert t[0] == 'WISEU J005559.88+594745.0'


def test_get_simbad_names_live():
    """Unmocked version of the call to Simbad to catch API changes"""
    t = get_simbad_names("TWA 27")
    assert "TIC 102076870" in t
