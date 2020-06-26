# Main database handler code

__all__ = ['__version__', 'Database', 'or_', 'and_', 'create_database']

import os
import json
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import Engine
from sqlalchemy import event, create_engine, Table, MetaData
from sqlalchemy import or_, and_
from .utils import json_serializer

try:
    from .version import version as __version__
except ImportError:
    __version__ = ''

Base = declarative_base()  # For SQLAlchemy handling


def load_connection(connection_string, sqlite_foreign=True, base=None):
    """Return session, base, and engine objects for connecting to the database.

    Parameters
    ----------
    connection_string : str
        The connection string to connect to the database. The
        connection string should take the form:
        ``dialect+driver://username:password@host:port/database``
    base : base object
        Use an existing base class

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
    Session = sessionmaker(bind=engine)
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
    session, base, engine = load_connection(connection_string, base=Base)
    # base.metadata.drop_all()  # drop all the tables
    base.metadata.create_all()  # this explicitly create the SQLite file


def copy_database_schema(source_connection_string, destination_connection_string, sqlite_foreign=False):
    """
    Copy a database schema (ie, all tables and columns) from one database to another
    Adapted from https://gist.github.com/pawl/9935333

    Parameters
    ----------
    source_connection_string : str
        Connection string to source database
    destination_connection_string : str
        Connection string to destination database
    """

    session, srcBase, srcEngine = load_connection(source_connection_string, sqlite_foreign=sqlite_foreign)
    # srcEngine._metadata = MetaData(bind=srcEngine)
    # srcEngine._metadata.reflect(srcEngine)  # get columns from existing table

    srcMetadata = srcBase.metadata
    srcMetadata.reflect(bind=srcEngine)

    session, base, destEngine = load_connection(destination_connection_string, sqlite_foreign=sqlite_foreign)
    destEngine._metadata = MetaData(bind=destEngine)

    for table in srcMetadata.tables:
        destTable = Table(table, destEngine._metadata)

        # copy schema and create newTable from oldTable
        for column in srcMetadata.tables[table].columns:
            destTable.append_column(column.copy())
        destTable.create()


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
            Flag to enable/disable use of foreign keys with SQLite
        """

        self.session, self.base, self.engine = load_connection(connection_string, sqlite_foreign=sqlite_foreign)

        # Convenience method
        self.query = self.session.query

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

    # Inventory related methods
    def _row_cleanup(self, row):
        row_dict = row._asdict()
        del row_dict[self._foreign_key]
        return row_dict

    def _inventory_query(self, data_dict, table_name, source_name):
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

    # General query methods
    def sql_query(self, query):
        # Direct SQL query
        return self.engine.execute(query).fetchall()

    # Object output methods
    def save_json(self, name, directory):
        # Output database contents as JSON data into specified directory
        if isinstance(name, str):
            source_name = str(name)
            data = self.inventory(name)
        else:
            source_name = str(name.__getattribute__(self._primary_table_key))
            data = self.inventory(name.__getattribute__(self._primary_table_key))

        filename = source_name.lower().replace(' ', '_') + '_data.json'
        with open(os.path.join(directory, filename), 'w') as f:
            f.write(json.dumps(data, indent=4, default=json_serializer))

    def save_db(self, directory):
        # Output reference tables
        for table in self._reference_tables:
            results = self.session.query(self.metadata.tables[table]).all()
            data = [row._asdict() for row in results]
            filename = table + '_data.json'
            if len(data) > 0:
                with open(os.path.join(directory, filename), 'w') as f:
                    f.write(json.dumps(data, indent=4, default=json_serializer))

        for row in self.query(self.metadata.tables[self._primary_table]):
            self.save_json(row, directory)

    # Object input methods
    def load_table(self, table, directory):
        # Load a reference table, expects there to be a file of the form [table]_data.json
        filename = os.path.join(directory, table+'_data.json')
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                data = json.load(f)
                self.metadata.tables[table].insert().execute(data)
        else:
            print(f'{table}_data.json not found.')

    def load_json(self, filename):
        # Load a single object
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
        # From a directory, reload the database

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
            core_name = file.replace('_data.json', '')
            if core_name in self._reference_tables:
                continue

            self.load_json(os.path.join(directory, file))

