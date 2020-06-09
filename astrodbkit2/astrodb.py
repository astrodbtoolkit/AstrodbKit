# Main database handler code

__all__ = ['__version__', 'Database', 'load_connection']

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import Engine
from sqlalchemy import event
from sqlalchemy import create_engine

try:
    from .version import version as __version__
except ImportError:
    __version__ = ''

Base = declarative_base()


def load_connection(connection_string):
    """Return session, base, and engine objects for connecting to the database.

    Parameters
    ----------
    connection_string : str
        The connection string to connect to the database. The
        connection string should take the form:
        ``dialect+driver://username:password@host:port/database``

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
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine)
    session = Session()

    # Enable foreign key checks in SQLite
    if 'sqlite' in connection_string:
        set_sqlite()
    # elif 'postgresql' in connection_string:
    #     # Set up schema in postgres (must be lower case?)
    #     from sqlalchemy import DDL
    #     event.listen(Base.metadata, 'before_create', DDL("CREATE SCHEMA IF NOT EXISTS ivoa"))
    #     event.listen(Base.metadata, 'before_create', DDL("CREATE SCHEMA IF NOT EXISTS tap_schema"))

    return session, Base, engine


def set_sqlite():
    @event.listens_for(Engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        # Enable foreign key checking in SQLite
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


class Database:
    """
    Wrapper for database calls and utility functions
    """

    def __init__(self, connection_string):
        self.session, self.base, self.engine = load_connection(connection_string)

        # Convenience method
        self.query = self.session.query

        # Prep the tables
        self.metadata = self.base.metadata
        self.metadata.reflect(bind=self.engine)

        if len(self.metadata.tables) > 0:
            self._prepare_tables()
        else:
            print('Database empty. Import schema (eg, from astrodbkit.schema import *) '
                  'and then run the create_database() here')

    def _prepare_tables(self):
        self.Sources = self.metadata.tables['Sources']
        self.Names = self.metadata.tables['Names']
        self.Publications = self.metadata.tables['Publications']
        self.Photometry = self.metadata.tables['Photometry']

    def create_database(self):
        # self.base.metadata.drop_all()  # drop all the tables
        self.base.metadata.create_all()  # this explicitly create the SQLite file

        self._prepare_tables()

    def _inventory_query(self, data_dict, table, table_name, source_name):
        results = self.session.query(table).filter(table.c.source == source_name).all()

        if results and table_name == 'Sources':
            data_dict[table_name] = [row._asdict() for row in results]
        elif results:
            data_dict[table_name] = [self._row_cleanup(row) for row in results]

    @staticmethod
    def _row_cleanup(row):
        row_dict = row._asdict()
        del row_dict['source']
        return row_dict

    def inventory(self, name):
        data_dict = {}
        self._inventory_query(data_dict, self.Sources, 'Sources', name)
        self._inventory_query(data_dict, self.Names, 'Names', name)
        self._inventory_query(data_dict, self.Photometry, 'Photometry', name)

        return data_dict

    def sql_query(self, query):
        # Direct SQL query
        return self.engine.execute(query).fetchall()

    def save(self, directory='SIMPLE/data'):
        # Output database contents as JSON data into specified directory
        for row in self.query(self.Sources):
            name = row.source.lower()
            data = self.inventory(row.source)
