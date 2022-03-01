# Main database handler code

__all__ = ['__version__', 'Database', 'or_', 'and_', 'create_database']

import os
import json
import numpy as np
import pandas as pd
from astropy.table import Table as AstropyTable
from astropy.units.quantity import Quantity
from astropy.coordinates import SkyCoord
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import Engine
import sqlalchemy.types as sqlalchemy_types
from sqlalchemy import event, create_engine, Table
from sqlalchemy import or_, and_
import sqlite3
from tqdm import tqdm
from . import REFERENCE_TABLES, PRIMARY_TABLE, PRIMARY_TABLE_KEY, FOREIGN_KEY
from .utils import json_serializer, get_simbad_names, deprecated_alias, datetime_json_parser
from .spectra import load_spectrum

try:
    from .version import version as __version__
except ImportError:
    __version__ = ''

# For SQLAlchemy ORM Declarative mapping
# User created schema should import and use astrodb.Base so that create_database can properly handle them
Base = declarative_base()


class AstrodbQuery(Query):
    # Subclassing the Query class to add more functionality.
    # See: https://stackoverflow.com/questions/15936111/sqlalchemy-can-you-add-custom-methods-to-the-query-object
    def _make_astropy(self):
        temp = self.all()
        if len(temp) > 0:
            t = AstropyTable(rows=temp, names=temp[0].keys())
        else:
            t = AstropyTable(temp)
        return t

    def astropy(self, spectra=None, spectra_format=None, **kwargs):
        """
        Allow SQLAlchemy query output to be formatted as an astropy Table

        Parameters
        ----------
        spectra : str or list
            List of columns to process as spectra
        spectra_format : str
            Format to apply for all spectra. Default: None means specutils will attempt to find the best one.

        Returns
        -------
        t : astropy.Table
            Table output of query
        """

        t = self._make_astropy()

        # Apply spectra conversion
        if spectra is not None:
            if not isinstance(spectra, (list, tuple)):
                spectra = [spectra]
            for col in spectra:
                if col in t.colnames:
                    t[col] = [load_spectrum(x, spectra_format=spectra_format) for x in t[col]]

        return t

    def table(self, *args, **kwargs):
        # Alternative for getting astropy Table
        return self.astropy(*args, **kwargs)

    def pandas(self, spectra=None, spectra_format=None, **kwargs):
        """
        Allow SQLAlchemy query output to be formatted as a pandas DataFrame

        Parameters
        ----------
        spectra : str or list
            List of columns to process as spectra
        spectra_format : str
            Format to apply for all spectra. Default: None means specutils will attempt to find the best one.

        Returns
        -------
        df : pandas.DataFrame
            DataFrame output of query
        """

        # Relying on astropy to convert to pandas for simplicity as that handles the column names
        df = self._make_astropy().to_pandas()

        # Apply spectra conversion
        if spectra is not None:
            if not isinstance(spectra, (list, tuple)):
                spectra = [spectra]
            for col in spectra:
                if col in df.columns.to_list():
                    df[col] = df[col].apply(lambda x: load_spectrum(x, spectra_format=spectra_format))

        return df

    def spectra(self, spectra=['spectrum'], fmt='astropy', **kwargs):
        """
        Convenience method fo that uses default column name for spectra conversion

        Parameters
        ----------
        spectra : str or list
            List of columns to process as spectra
        fmt : str
            Output format (Default: astropy)
        """
        if fmt == 'pandas':
            return self.pandas(spectra=spectra, **kwargs)
        else:
            return self.astropy(spectra=spectra, **kwargs)


def load_connection(connection_string, sqlite_foreign=True, base=None, connection_arguments={}):
    """Return session, base, and engine objects for connecting to the database.

    Parameters
    ----------
    connection_string : str
        The connection string to connect to the database. The
        connection string should take the form:
        ``dialect+driver://username:password@host:port/database``
    sqlite_foreign : bool
        Flag to enable foreign key checks for SQLite. Default: True
    base : SQLAlchemy base object
        Use an existing base class. Default: None (ie, creates a new one)
    connection_arguments : dict
        Additional connection arguments, like {'check_same_thread': False}

    Returns
    -------
    session : session object
        Provides a holding zone for all objects loaded or associated
        with the database.
    base : base object
        Provides a base class for declarative class definitions.
    engine : engine object
        Provides a source of database connectivity and behavior.
    """

    engine = create_engine(connection_string, connect_args=connection_arguments)
    if not base:
       base = declarative_base()
    base.metadata.bind = engine
    Session = sessionmaker(bind=engine, query_cls=AstrodbQuery)
    session = Session()

    # Enable foreign key checks in SQLite
    if 'sqlite' in connection_string and sqlite_foreign:
        set_sqlite()
    # elif 'postgresql' in connection_string:
    #     # Set up schema in postgres (must be lower case?)
    #     from sqlalchemy import DDL
    #     event.listen(Base.metadata, 'before_create', DDL("CREATE SCHEMA IF NOT EXISTS ivoa"))
    #     event.listen(Base.metadata, 'before_create', DDL("CREATE SCHEMA IF NOT EXISTS tap_schema"))

    return session, base, engine


def set_sqlite():
    # Special overrides when using SQLite
    @event.listens_for(Engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        # Enable foreign key checking in SQLite
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


def create_database(connection_string, drop_tables=False):
    """
    Create a database from a schema that utilizes the `astrodbkit2.astrodb.Base` class.
    Some databases, eg Postgres, must already exist but any tables should be dropped.

    Parameters
    ----------
    connection_string : str
        Connection string to database
    drop_tables : bool
        Flag to drop existing tables. This is needed when the schema changes. (Default: False)
    """

    session, base, engine = load_connection(connection_string, base=Base)
    if drop_tables:
        base.metadata.drop_all()
    base.metadata.create_all()  # this explicitly creates the database
    return session, base, engine


def copy_database_schema(source_connection_string, destination_connection_string, sqlite_foreign=False,
                         ignore_tables=[], copy_data=False):
    """
    Copy a database schema (ie, all tables and columns) from one database to another
    Adapted from https://gist.github.com/pawl/9935333

    Parameters
    ----------
    source_connection_string : str
        Connection string to source database
    destination_connection_string : str
        Connection string to destination database
    sqlite_foreign : bool
        Flag to enable foreign key checks for SQLite; passed to `load_connection`. Default: False
    ignore_tables : list
        List of tables to not copy
    copy_data : bool
        Flag to enable copying data to the new database. Default: False
    """

    src_session, src_base, src_engine = load_connection(source_connection_string, sqlite_foreign=sqlite_foreign)
    src_metadata = src_base.metadata
    src_metadata.reflect(bind=src_engine)

    dest_session, dest_base, dest_engine = load_connection(destination_connection_string, sqlite_foreign=sqlite_foreign)
    dest_metadata = dest_base.metadata
    dest_metadata.reflect(bind=dest_engine)

    for table in src_metadata.sorted_tables:
        if table.name in ignore_tables:
            continue

        dest_table = Table(table.name, dest_metadata)

        # Copy schema and create newTable from oldTable
        for column in src_metadata.tables[table.name].columns:
            dest_table.append_column(column.copy())
        dest_table.create()

        # Copy data, row by row
        if copy_data:
            table_data = src_session.query(src_metadata.tables[table.name]).all()
            for row in table_data:
                dest_session.execute(dest_table.insert(row))
            dest_session.commit()

    # Explicitly close sessions/engines
    src_session.close()
    dest_session.close()
    src_engine.dispose()
    dest_engine.dispose()


class Database:
    def __init__(self, connection_string,
                 reference_tables=REFERENCE_TABLES,
                 primary_table=PRIMARY_TABLE,
                 primary_table_key=PRIMARY_TABLE_KEY,
                 foreign_key=FOREIGN_KEY,
                 column_type_overrides={},
                 sqlite_foreign=True,
                 connection_arguments={}):
        """
        Wrapper for database calls and utility functions

        Parameters
        ----------
        connection_string : str
            Connection string to establish a database connection
        reference_tables : list
            List of reference tables; these are treated separately from data tables.
            Default: ['Publications', 'Telescopes', 'Instruments']
        primary_table : str
            Name of the primary source table. Default: Sources
        primary_table_key : str
            Name of the primary key in the sources table. This is meant to be unique and used to join tables.
            Default: source
        foreign_key : str
            Name of the foreign key in other tables that refer back to the primary table. Default: source
        column_type_overrides : dict
            Dictionary with table.column type overrides. For example, {'spectra.spectrum': sqlalchemy.types.TEXT()}
            will set the table spectra, column spectrum to be of type TEXT()
        sqlite_foreign : bool
            Flag to enable/disable use of foreign keys with SQLite. Default: True
        connection_arguments : dict
            Additional connection arguments, like {'check_same_thread': False}. Default: {}
        """

        if connection_string == 'sqlite://':
            self.session, self.base, self.engine = create_database(connection_string)
        else:
            self.session, self.base, self.engine = load_connection(connection_string, sqlite_foreign=sqlite_foreign,
                                                                   connection_arguments=connection_arguments)

        # Convenience methods
        self.query = self.session.query
        self.save_db = self.save_database
        self.load_db = self.load_database

        # Prep the tables
        self.metadata = self.base.metadata
        self.metadata.reflect(bind=self.engine)

        self._reference_tables = reference_tables
        self._primary_table = primary_table
        self._primary_table_key = primary_table_key
        self._foreign_key = foreign_key

        if len(self.metadata.tables) == 0:
            print('Database empty. Import schema (eg, from astrodbkit.schema_example import *) '
                  'and then run create_database()')
            raise RuntimeError('Create database first.')

        # Set tables as explicit attributes of this class
        for table in self.metadata.tables:
            self.__setattr__(table, self.metadata.tables[table])

        # If column overrides are provided, this will set the types to whatever the user provided
        if len(column_type_overrides) > 0:
            for k, v in column_type_overrides.items():
                tab, col = k.split('.')
                self.metadata.tables[tab].columns[col].type = v

    # Generic methods
    @staticmethod
    def _handle_format(temp, fmt):
        # Internal method to handle SQLAlchemy output and format it
        if fmt.lower() in ('astropy', 'table'):
            if len(temp) > 0:
                results = AstropyTable(rows=temp, names=temp[0].keys())
            else:
                results = AstropyTable(temp)
        elif fmt.lower() == 'pandas':
            if len(temp) > 0:
                results = pd.DataFrame(temp, columns=temp[0].keys())
            else:
                results = pd.DataFrame(temp)
        else:
            results = temp

        return results

    # Inventory related methods
    def _row_cleanup(self, row):
        """
        Handler method to convert a result row to a dictionary but remove the foreign key column
        as defined in the database initialization. Used internally by `Database._inventory_query`.

        Parameters
        ----------
        row :
            SQLAlchemy row object

        Returns
        -------
        row_dict : dict
            Dictionary version of the row object
        """

        row_dict = row._asdict()
        del row_dict[self._foreign_key]
        return row_dict

    def _inventory_query(self, data_dict, table_name, source_name):
        """
        Handler method to query database contents for the specified source.
        Table results are stored as new keys in `data_dict`. Used internally by `Database.inventory`.

        Parameters
        ----------
        data_dict : dict
            Dictionary of data to update.
        table_name : str
            Table to query
        source_name : str
            Source to query on
        """

        table = self.metadata.tables[table_name]

        if table_name == self._primary_table:
            column = table.columns[self._primary_table_key]
        else:
            column = table.columns[self._foreign_key]

        results = self.session.query(table).filter(column == source_name).all()

        if results and table_name == self._primary_table:
            data_dict[table_name] = [row._asdict() for row in results]
        elif results:
            data_dict[table_name] = [self._row_cleanup(row) for row in results]

    def inventory(self, name, pretty_print=False):
        """
        Method to return a dictionary of all information for a given source, matched by name.
        Each table is a key of this dictionary.

        Parameters
        ----------
        name : str
            Name of the source to search for
        pretty_print : bool
            Optionally print out the dictionary contents on screen. Default: False

        Returns
        -------
        data_dict : dict
            Dictionary of all information for the given source.
        """

        data_dict = {}
        # Loop over tables (not reference tables) and gather the information. Start with the primary table, though
        self._inventory_query(data_dict, self._primary_table, name)
        for table in self.metadata.tables:
            if table in self._reference_tables + [self._primary_table]:
                continue
            self._inventory_query(data_dict, table, name)

        if pretty_print:
            print(json.dumps(data_dict, indent=4, default=json_serializer))

        return data_dict

    # Text query methods
    @deprecated_alias(format='fmt')
    def search_object(self, name, output_table=None, resolve_simbad=False,
                      table_names={'Sources': ['source', 'shortname'], 'Names': ['other_name']},
                      fmt='table', fuzzy_search=True, verbose=True):
        """
        Query the database for the object specified. By default will return the primary table,
        but this can be specified. Users can also request to resolve the object name via Simbad and query against
        all Simbad names.

        Parameters
        ----------
        name : str or list
            Object name(s) to match
        output_table : str
            Name of table to match. Default: primary table (eg, Sources)
        resolve_simbad : bool
            Get additional names from Simbad. Default: False
        table_names : dict
            Dictionary of tables to search for name information. Should be of the form table name: column name list.
            Default: {'Sources': ['source', 'shortname'], 'Names': 'other_name'}
        fmt : str
            Format to return results in (pandas, astropy/table, default). Default is astropy table
        fuzzy_search : bool
            Flag to perform partial searches on provided names (default: True)
        verbose : bool
            Output some extra messages (default: True)

        Returns
        -------
        List of SQLAlchemy results
        """

        # Set table to output and verify it exists
        if output_table is None:
            output_table = self._primary_table
        if output_table not in self.metadata.tables:
            raise RuntimeError(f'Table {output_table} is not in the database')

        match_column = self._foreign_key
        if output_table == self._primary_table:
            match_column = self._primary_table_key

        # Query Simbad to get additional names and join them to list to search
        if resolve_simbad:
            simbad_names = get_simbad_names(name, verbose=verbose)
            name = list(set(simbad_names + [name]))
            if verbose:
                print(f'Including Simbad names, searching for: {name}')

        # Turn name into a list
        if not isinstance(name, list):
            name = [name]

        # Verify provided tables exist in database
        for k in table_names.keys():
            if k not in self.metadata.tables:
                raise RuntimeError(f'Table {k} is not in the database')

        # Get source for objects that match the provided names
        # The following will build the filters required to query all specified tables
        # approximately by case-insensitive names.
        # This is not really optimized as it does separate DB calls,
        # but is the simpler setup and at our scale is sufficient
        matched_names = []
        for k, col_list in table_names.items():
            for v in col_list:
                if fuzzy_search:
                    filters = [self.metadata.tables[k].columns[v].ilike(f'%{n}%')
                               for n in name]
                else:
                    filters = [self.metadata.tables[k].columns[v].ilike(f'{n}')
                               for n in name]

                # Column to be returned
                if k == self._primary_table:
                    output_to_match = self.metadata.tables[k].columns[self._primary_table_key]
                else:
                    output_to_match = self.metadata.tables[k].columns[self._foreign_key]

                temp = self.query(output_to_match).\
                    filter(or_(*filters)).\
                    distinct().\
                    all()
                matched_names += [s[0] for s in temp]

        # Join the matched sources with the desired table
        temp = self.query(self.metadata.tables[output_table]).\
            filter(self.metadata.tables[output_table].columns[match_column].in_(matched_names)).\
            all()

        results = self._handle_format(temp, fmt)

        return results

    def search_string(self, value, fmt='table', fuzzy_search=True, verbose=True):
        """
        Search an abitrary string across all string columns in the full database

        Parameters
        ----------
        value : str
            String to search for
        fmt : str
            Format to return results in (pandas, astropy/table, default). Default is astropy table
        fuzzy_search : bool
            Flag to perform partial searches on provided names (default: True)
        verbose : bool
            Output results to screen in addition to dictionary (default: True)

        Returns
        -------
        Dictionary of results, with each key being the matched table names
        """

        # Loop over all tables to build the results
        output_dict = {}
        for table in self.metadata.tables:
            # Gather only string-type columns
            columns = self.metadata.tables[table].columns
            col_list = [c for c in columns
                        if isinstance(c.type, sqlalchemy_types.String)
                        or isinstance(c.type, sqlalchemy_types.Text)
                        or isinstance(c.type, sqlalchemy_types.Unicode)]

            # Construct filters to query for each string column
            filters = []
            for c in col_list:
                if fuzzy_search:
                    filters += [c.ilike(f'%{value}%')]
                else:
                    filters += [c.ilike(f'{value}')]

            # Perform the actual query
            temp = self.query(self.metadata.tables[table]). \
                filter(or_(*filters)). \
                distinct(). \
                all()

            # Append results to dictionary output in specified format
            if len(temp) > 0:
                results = self._handle_format(temp, fmt)
                if verbose:
                    print(table)
                    print(results)
                output_dict[table] = results

        return output_dict

    # General query methods
    @deprecated_alias(format='fmt')
    def sql_query(self, query, fmt='default'):
        """
        Wrapper for a direct SQL query.

        Parameters
        ----------
        query : str
            Query to be performed
        fmt : str
            Format in which to return the results (pandas, astropy/table, default)

        Returns
        -------
        List of SQLAlchemy results
        """

        temp = self.engine.execute(query).fetchall()

        return self._handle_format(temp, fmt)

    def query_region(self, target_coords, radius=Quantity(10, unit='arcsec'), output_table=None, fmt='table',
                    coordinate_table=None, ra_col='ra', dec_col='dec', frame='icrs', unit='deg'):
        """
        Perform a cone search of the given coordinates and return the specified output table.

        Parameters
        ----------
        target_coords : SkyCoord
            Astropy SkyCoord object of coordinates to search around
        radius : Quantity or float
            Radius as an astropy Quantity object in which to search for objects.
            If not a Quantity will convert to one assuming units are arcseconds. Default: 10 arcseconds
        output_table : str
            Name of table to match. Default: primary table (eg, Sources)
        fmt : str
            Format to return results in (pandas, astropy/table, default). Default is astropy table
        coordinate_table : str
            Table to use for coordinates. Default: primary table (eg, Sources)
        ra_col : str
            Name of column to use for RA values. Default: ra
        dec_col : str
            Name of column to use for Dec values. Default: dec
        frame : str
            Coordinate frame for objects in the database. Default: icrs
        unit : str or tuple of Unit or str
            Unit of ra/dec (or equivalent) in database. Default: deg

        Returns
        -------
        List of SQLAlchemy results
        """

        # Set table to output and verify it exists
        if output_table is None:
            output_table = self._primary_table
        if output_table not in self.metadata.tables:
            raise RuntimeError(f'Table {output_table} is not in the database')

        # Radius conversion
        if not isinstance(radius, Quantity):
            radius = Quantity(radius, unit='arcsec')

        # Get the column name to use for matching
        match_column = self._foreign_key
        if output_table == self._primary_table:
            match_column = self._primary_table_key

        # Grab the specified coordinate table (Sources by default) to construct SkyCoord objects
        if coordinate_table is None:
            coordinate_table = self._primary_table
        if coordinate_table not in self.metadata.tables:
            raise RuntimeError(f'Table {coordinate_table} is not in the database')
        coordinate_match_column = self._foreign_key
        if coordinate_table == self._primary_table:
            coordinate_match_column = self._primary_table_key

        # This is adapted from the original astrodbkit code
        df = self.query(self.metadata.tables[coordinate_table]).pandas()
        df[['ra', 'dec']] = df[[ra_col, dec_col]].apply(pd.to_numeric)  # convert everything to floats
        mask = df['ra'].isnull()
        df = df[~mask]

        # Native use of astropy SkyCoord objects here
        coord_list = SkyCoord(df['ra'].tolist(), df['dec'].tolist(), frame=frame, unit=unit)
        sep_list = coord_list.separation(target_coords)  # sky separations for each db object against target position
        good = sep_list <= radius

        if sum(good) > 0:
            matched_list = df[coordinate_match_column][good]
        else:
            matched_list = []

        # Join the matched sources with the desired table
        temp = self.query(self.metadata.tables[output_table]). \
            filter(self.metadata.tables[output_table].columns[match_column].in_(matched_list)). \
            all()
        results = self._handle_format(temp, fmt)

        return results

    # Object output methods
    def save_json(self, name, directory):
        """
        Output database contents as JSON data for matched source into specified directory

        Parameters
        ----------
        name : str
            Name of source to match by primary key.
            Alternatively can also be a row from a query against the source table.
        directory : str
            Name of directory in which to save the output JSON
        """

        if isinstance(name, str):
            source_name = str(name)
            data = self.inventory(name)
        else:
            source_name = str(name.__getattribute__(self._primary_table_key))
            data = self.inventory(name.__getattribute__(self._primary_table_key))

        # Clean up spaces and other special characters
        filename = source_name.lower().replace(' ', '_').replace('*', '').strip() + '.json'
        with open(os.path.join(directory, filename), 'w') as f:
            f.write(json.dumps(data, indent=4, default=json_serializer))

    def save_reference_table(self, table, directory):
        """

        Parameters
        ----------
        table : str
            Name of reference table to output
        directory : str
            Name of directory in which to save the output JSON
        """

        results = self.session.query(self.metadata.tables[table]).all()
        data = [row._asdict() for row in results]
        filename = table + '.json'
        if len(data) > 0:
            with open(os.path.join(directory, filename), 'w') as f:
                f.write(json.dumps(data, indent=4, default=json_serializer))

    def save_database(self, directory, clear_first=True):
        """
        Output contents of the database into the specified directory as JSON files.
        Source objects have individual JSON files with all data for that object.
        Reference tables have a single JSON for all contents in the table.

        Parameters
        ----------
        directory : str
            Name of directory in which to save the output JSON
        clear_first : bool
            First clear the directory of all existing JSON (useful to capture DB deletions). Default: True
        """

        # Clear existing files first from that directory
        if clear_first:
            print('Clearing existing JSON files...')
            for filename in os.listdir(directory):
                os.remove(os.path.join(directory, filename))

        # Output reference tables
        for table in self._reference_tables:
            # Skip reference tables that are not actually in the database
            if table not in self.metadata.tables.keys():
                continue

            self.save_reference_table(table, directory)

        # Output primary objects
        for row in tqdm(self.query(self.metadata.tables[self._primary_table])):
            self.save_json(row, directory)

    # Object input methods
    def add_table_data(self, data, table, fmt='csv'):
        """
        Method to insert data into the database. Column names in the file must match those of the database table.
        Additional columns in the supplied table are ignored.
        Format options include:

         - csv
         - astropy
         - pandas


        Parameters
        ----------
        data : str or astropy.Table or pandas.DataFrame
            Name of file or Table or DataFrame to load
        table : str
            Name of table to insert records into
        fmt : str
            Data format. Default: csv
        """

        if fmt.lower() == 'csv':
            df = pd.read_csv(data)
        elif fmt.lower() == 'astropy':
            df = data.to_pandas()
        elif fmt.lower() == 'pandas':
            df = data.copy()
        else:
            raise RuntimeError(f'Unrecognized format {fmt}')

        # Foreign key constraints will prevent inserts of missing sources,
        # but for clarity we'll check first and exit if there are missing sources
        if table != self._primary_table:
            source_list = df[self._foreign_key].to_list()
            primary_column = self.metadata.tables[self._primary_table].columns[self._primary_table_key]
            matched_sources = self.query(primary_column).filter(primary_column.in_(source_list)).all()
            missing_sources = np.setdiff1d(source_list, matched_sources)
            if len(missing_sources) > 0:
                print(f'{len(missing_sources)} missing source(s):')
                print(missing_sources)
                raise RuntimeError(f'There are missing entries in {self._primary_table} table. These must exist first.')

        # Convert format for SQLAlchemy
        data = [row.to_dict() for _, row in df.iterrows()]

        # Load into specified table
        self.metadata.tables[table].insert().execute(data)

    def load_table(self, table, directory, verbose=False):
        """
        Load a reference table to the database, expects there to be a file of the form [table].json

        Parameters
        ----------
        table : str
            Name of table to load. Table must already exist in the schema.
        directory : str
            Name of directory containing the JSON file
        verbose : bool
            Flag to enable diagnostic messages
        """

        filename = os.path.join(directory, table+'.json')
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                data = json.load(f)
                self.metadata.tables[table].insert().execute(data)
        else:
            if verbose: print(f'{table}.json not found.')

    def load_json(self, filename):
        """
        Load single source JSON into the database

        Parameters
        ----------
        filename : str
            Name of directory containing the JSON file
        """

        with open(filename, 'r') as f:
            data = json.load(f, object_hook=datetime_json_parser)

        # Loop through the dictionary, adding data to the database.
        # Ensure that Sources is added first
        source = data[self._primary_table][0][self._primary_table_key]
        self.metadata.tables[self._primary_table].insert().execute(data[self._primary_table])
        for key, value in data.items():
            if key == self._primary_table:
                continue

            # Loop over multiple values (eg, Photometry)
            for v in value:
                temp_dict = v
                temp_dict[self._foreign_key] = source
                self.metadata.tables[key].insert().execute(temp_dict)

    def load_database(self, directory, verbose=False):
        """
        Reload entire database from a directory of JSON files.
        Note that this will first clear existing tables.

        Parameters
        ----------
        directory : str
            Name of directory containing the JSON files
        verbose : bool
            Flag to enable diagnostic messages
        """

        # Clear existing database contents
        # reversed(sorted_tables) can help ensure that foreign key dependencies are taken care of first
        for table in reversed(self.metadata.sorted_tables):
            if verbose: print(f'Deleting {table.name} table')
            self.metadata.tables[table.name].delete().execute()

        # Load reference tables first
        for table in self._reference_tables:
            if verbose: print(f'Loading {table} table')
            self.load_table(table, directory, verbose=verbose)

        # Load object data
        if verbose: print('Loading object tables')
        for file in tqdm(os.listdir(directory)):
            # Skip reference tables
            core_name = file.replace('.json', '')
            if core_name in self._reference_tables:
                continue

            # Skip non-JSON files or hidden files
            if not file.endswith('.json') or file.startswith('.'):
                continue

            self.load_json(os.path.join(directory, file))

    def dump_sqlite(self, database_name):
        if self.engine.url.drivername == 'sqlite':
            destconn = sqlite3.connect(database_name)
            self.engine.raw_connection().backup(destconn)
        else:
            print('AstrodbKit2: dump_sqlite not available for non-sqlite databases')
