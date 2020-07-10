# Utility functions for Astrodbkit2

import re
from datetime import datetime
from decimal import Decimal
from astroquery.simbad import Simbad

__all__ = ['json_serializer', 'get_simbad_names']


def json_serializer(obj):
    """Function describing how things should be serialized in JSON.
    Datetime objects are saved with datetime.isoformat(), Parameter class objects use clean_dict()
    while all others use __dict__ """

    if isinstance(obj, datetime):
        return obj.isoformat()

    if isinstance(obj, Decimal):
        return float(obj)

    return obj.__dict__


def _name_formatter(name):
    """
    Clean up names of spurious formatting (extra spaces, some special characters)

    Parameters
    ----------
    name : str
        Name to clean up

    Returns
    -------
    Cleaned up name
    """

    # Clean up multiple spaces
    name = re.sub("\s\s+", " ", name)

    # Clean up Simbad types
    strings_to_delete = ['V* ', 'EM* ', 'NAME ', '** ', 'Cl* ', '* ']
    for pattern in strings_to_delete:
        name = name.replace(pattern, '')

    name = name.strip()

    # Clean up 'hidden' names from Simbad
    if 'HIDDEN' in name.upper():
        name = None

    return name


def get_simbad_names(name):
    """
    Get list of alternate names from Simbad

    Parameters
    ----------
    name : str
        Name to resolve

    Returns
    -------
    List of names
    """

    t = Simbad.query_objectids(name)
    temp = [_name_formatter(s) for s in t['ID'].tolist()]
    return [s for s in temp if s is not None and s != '']
