# Main database handler code

__all__ = ['__version__', 'Database', 'or_', 'and_', 'create_database']

import os
import json
import pandas as pd
from astropy.table import Table as AstropyTable
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import Engine
from sqlalchemy import event, create_engine, Table
from sqlalchemy import or_, and_
from .utils import json_serializer, get_simbad_names

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
    def astropy(self):
        # Allow SQLAlchemy query output to be formatted as an astropy Table
        temp = self.all()
        if len(temp) > 0:
            t = AstropyTable(rows=temp, names=temp[0].keys())
        else:
            t = AstropyTable(temp)
        return t

    def table(self):
        # Alternative for getting astropy Table
        return self.astropy()

    def pandas(self):
        return pd.DataFrame(self.all())


def load_connection(connection_string, sqlite_foreign=True, base=None):
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

    engine = create_engine(connection_string)
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


def create_database(connection_string):
    """
    Create a database from a schema that utilizes the `astrodbkit2.astrodb.Base` class.
    Some databases, eg Postgres, must already exist but any tables should be dropped.

    Parameters
    ----------
    connection_string : str
        Connection string to database
    """

    session, base, engine = load_connection(connection_string, base=Base)
    # base.metadata.drop_all()  # drop all the tables
    base.metadata.create_all()  # this explicitly creates the database


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


class Database:
    def __init__(self, connection_string,
                 reference_tables=['Publications', 'Telescopes', 'Instruments'],
                 primary_table='Sources',
                 primary_table_key='source',
                 foreign_key='source',
                 column_type_overrides={},
                 sqlite_foreign=True):
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
        """

        self.session, self.base, self.engine = load_connection(connection_string, sqlite_foreign=sqlite_foreign)

        # Convenience methods
        self.query = self.session.query

        self.save_database = self.save_db
        self.load_db = self.load_database

        # Prep the tables
        self.metadata = self.base.metadata
        self.metadata.reflect(bind=self.engine)

        self._reference_tables = reference_tables
        self._primary_table = primary_table
        self._primary_table_key = primary_table_key
        self._foreign_key = foreign_key

        if len(self.metadata.tables) == 0:
            print('Database empty. Import schema (eg, from astrodbkit.schema import *) '
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
    def _handle_format(self, temp, format):
        # Internal method to handle SQLAlchemy output and format it
        if format.lower() in ('astropy', 'table'):
            if len(temp) > 0:
                results = AstropyTable(rows=temp, names=temp[0].keys())
            else:
                results = AstropyTable(temp)
        elif format.lower() == 'pandas':
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
    def search_object(self, name, output_table=None, resolve_simbad=False,
                      table_names={'Sources': 'source', 'Names': 'other_name', 'Sources': 'shortname'},
                      format='default'):
        """
        Query the database for the object specified. By default will return the primary table,
        but this can be specified. Users can also request to resolve the object name via Simbad and query against
        all Simbad names.

        Parameters
        ----------
        name : str
            Object name to match
        output_table : str
            Name of table to match. Default: primary table (eg, Sources)
        resolve_simbad : bool
            Get additional names from Simbad. Default: False
        table_names : dict
            Dictionary of tables to search for name information. Should be of the form table name: column name.
            Default: {'Sources': 'source', 'Names': 'other_name'}
        format : str
            Format to return results in (pandas, astropy/table, default)

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
            simbad_names = get_simbad_names(name)
            name = list(set(simbad_names + [name]))
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
        for k, v in table_names.items():
            filters = [self.metadata.tables[k].columns[v].ilike(f'%{n}%')
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

        results = self._handle_format(temp, format)

        return results

    # General query methods
    def sql_query(self, query, format='default'):
        """
        Wrapper for a direct SQL query.

        Parameters
        ----------
        query : str
            Query to be performed
        format : str
            Format in which to return the results (pandas, astropy/table, default)

        Returns
        -------
        List of SQLAlchemy results
        """

        temp = self.engine.execute(query).fetchall()

        return self._handle_format(temp, format)

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

    def save_db(self, directory, clear_first=True):
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
            for filename in os.listdir(directory):
                os.remove(os.path.join(directory, filename))

        # Output reference tables
        for table in self._reference_tables:
            results = self.session.query(self.metadata.tables[table]).all()
            data = [row._asdict() for row in results]
            filename = table + '.json'
            if len(data) > 0:
                with open(os.path.join(directory, filename), 'w') as f:
                    f.write(json.dumps(data, indent=4, default=json_serializer))

        # Output primary objects
        for row in self.query(self.metadata.tables[self._primary_table]):
            self.save_json(row, directory)

    # Object input methods
    def load_table(self, table, directory):
        """
        Load a reference table to the database, expects there to be a file of the form [table].json

        Parameters
        ----------
        table : str
            Name of table to load. Table must already exist in the schema.
        directory : str
            Name of directory containing the JSON file
        """

        filename = os.path.join(directory, table+'.json')
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                data = json.load(f)
                self.metadata.tables[table].insert().execute(data)
        else:
            print(f'{table}.json not found.')

    def load_json(self, filename):
        """
        Load single source JSON into the database

        Parameters
        ----------
        filename : str
            Name of directory containing the JSON file
        """

        with open(filename, 'r') as f:
            data = json.load(f)

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
            self.load_table(table, directory)

        # Load object data
        if verbose: print('Loading object tables')
        for file in os.listdir(directory):
            # Skip reference tables
            core_name = file.replace('.json', '')
            if core_name in self._reference_tables:
                continue

            self.load_json(os.path.join(directory, file))

