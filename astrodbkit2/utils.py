# Utility functions for Astrodbkit2

import re
import numpy as np
import functools
import warnings
from datetime import datetime
from decimal import Decimal
from astroquery.simbad import Simbad

__all__ = ['json_serializer', 'get_simbad_names']


def deprecated_alias(**aliases):
    """
    Decorator from StackOverflow
    https://stackoverflow.com/questions/49802412/how-to-implement-deprecation-in-python-with-argument-alias
    in order to handle deprecation of renamed columns
    To use: add @deprecated_alias(old_name='new_name')
    """
    def deco(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            rename_kwargs(f.__name__, kwargs, aliases)
            return f(*args, **kwargs)
        return wrapper
    return deco


def rename_kwargs(func_name, kwargs, aliases):
    """Helper function used be deprecated_alias"""
    for alias, new in aliases.items():
        if alias in kwargs:
            if new in kwargs:
                raise TypeError('{} received both {} and {}'.format(
                    func_name, alias, new))
            warnings.warn('{} is deprecated; use {}'.format(alias, new),
                          DeprecationWarning)
            kwargs[new] = kwargs.pop(alias)


def json_serializer(obj):
    """Function describing how things should be serialized in JSON.
    Datetime objects are saved with datetime.isoformat(), Parameter class objects use clean_dict()
    while all others use __dict__ """

    if isinstance(obj, datetime):
        return obj.isoformat()

    if isinstance(obj, Decimal):
        return float(obj)

    if isinstance(obj, bytes):
        return obj.decode('utf-8')

    return obj.__dict__


def datetime_json_parser(json_dict):
    """Function to convert JSON dictionary objects to datetime when possible.
    This is required to get datetime objects into the database.
    Adapted from: https://stackoverflow.com/questions/8793448/how-to-convert-to-a-python-datetime-object-with-json-loads
    """
    for (key, value) in json_dict.items():
        if isinstance(value, str):
            try:
                json_dict[key] = datetime.fromisoformat(value)
            except (ValueError, AttributeError):
                pass
        else:
            pass
    return json_dict


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
    name = re.sub(r"\s\s+", " ", name)

    # Clean up Simbad types
    strings_to_delete = ['V* ', 'EM* ', 'NAME ', '** ', 'Cl* ', '* ']
    for pattern in strings_to_delete:
        name = name.replace(pattern, '')

    name = name.strip()

    # Clean up 'hidden' names from Simbad
    if 'HIDDEN' in name.upper():
        name = None

    return name


def get_simbad_names(name, verbose=False):
    """
    Get list of alternate names from Simbad

    Parameters
    ----------
    name : str
        Name to resolve
    verbose : bool
        Verbosity flag

    Returns
    -------
    List of names
    """

    t = Simbad.query_objectids(name)
    if t is not None and len(t) > 0:
        temp = [_name_formatter(s) for s in t['ID'].tolist()]
        return [s for s in temp if s is not None and s != '']
    else:
        if verbose:
            print(f'No Simbad match for {name}')
        return [name]
