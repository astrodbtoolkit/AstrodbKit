# Licensed under a 3-clause BSD style license - see LICENSE.rst

# Packages may add whatever they like to this file, but
# should keep this content at the top.
# ----------------------------------------------------------------------------
# from ._astropy_init import *   # noqa
# ----------------------------------------------------------------------------

__all__ = ['__version__']

# from .example_mod import *   # noqa
# Then you can be explicit to control what ends up in the namespace,
# __all__ += ['do_primes']   # noqa
# or you can keep everything from the subpackage with the following instead
# __all__ += example_mod.__all__

try:
    from .version import version as __version__
except ImportError:
    __version__ = ''

# Global variables

# These describe the various database tables and their links
REFERENCE_TABLES = ['Publications', 'Telescopes', 'Instruments', 'Modes', 'Filters', 'PhotometryFilters',
                    'Citations', 'References', 'Versions', 'Parameters']
# REFERENCE_TABLES is a list of tables that do not link to the primary table.
# These are treated separately from the other data tables that are all assumed to be linked to the primary table.
PRIMARY_TABLE = 'Sources'  # the primary table used for storing objects
PRIMARY_TABLE_KEY = 'source'  # the name of the primary key in the primary table; this is used for joining tables
FOREIGN_KEY = 'source'  # the name of the foreign key in other tables that refer back to the primary
