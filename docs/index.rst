*************************
AstrodbKit Documentation
*************************

**AstrodbKit** is an astronomical database handler code built on top of SQLAlchemy.
The goal behind this code is to provide SQLAlchemy's
powerful `Object Relational Mapping (ORM) <https://docs.sqlalchemy.org/en/13/orm/>`_
infrastructure to access astronomical database contents regardless of the underlying architecture.

Introduction
============

Astronomical databases tend to focus on targets or observations with ancillary data that support them.
**AstrodbKit** is designed to work with these types of databases and adopts several principles
for some of its more advanced features. These are:

 - There exists a primary table with object identifiers
 - There exists any number of supplementary tables that either refer back to the primary table or exist independently

For example, the `SIMPLE database <https://github.com/SIMPLE-AstroDB/SIMPLE-db>`_
(which was the initial design for **AstrodbKit**) contains:

 - the primary Sources table, with coordinate information for each target
 - several object data tables, like Photometry, Spectra, etc, that contain information for each target
 - reference tables, like Publications, Telescopes, etc, that list other information that is used throughout the database, but doesn't refer to a particular target

The goal of **AstrodbKit** is to link together the object tables together in order
to express them as a single entity, while still retaining the information for other reference tables.
**AstrodbKit** can read and write out an entire target's data as a single JSON file for ease of transport and version
control. Reference tables are also written as JSON files, but organized differently-
a single file per table with multiple records.
An **AstrodbKit**-supported database can thus be exported to two types of JSON files:
individual target files and reference table files
If your database is constructed in a similar fashion, it will work well with **AstrodbKit**.
Other databases can still benefit from some of the functionality of **AstrodbKit**,
but they might not work properly if attempting to use the save/load methods.

Getting Started
===============

To install **AstrodbKit**, do::

    pip install astrodbkit

or directly via the Github repo::

    pip install git+https://github.com/dr-rodriguez/AstrodbKit

Creating a Database
===================

To create a database from scratch users will need a database schema coded with the SQLAlchemy ORM.
An example schema is provided (see schema_example.py),
but users can also refer to the `SIMPLE schema <https://github.com/SIMPLE-AstroDB/SIMPLE-db/blob/main/simple/schema.py>`_.
With that on hand, users should import their schema and prepare the database::

    from astrodbkit.astrodb import create_database
    from simple.schema import *

    connection_string = 'sqlite:///SIMPLE.db'  # connection string for a SQLite database named SIMPLE.db
    create_database(connection_string)

Creating a Database with Felis schema
-------------------------------------

The `LSST Felis package <https://felis.lsst.io/index.html>`_ provides a way of writing a database schema file. 
An example yaml file is provided in this repo (see schema_example_felis.yaml). 
Users that want to use Felis will need to install that package (lsst-felis), which you can do alongside astrodbkit with `pip install astrodbkit[felis]`.
Note that Python 3.11 or higher is required to use Felis.

With a Felis schema file, the creation call can happen either through Felis's examples (see their docs) or with something like::

    from astrodbkit.astrodb import create_database

    connection_string = 'sqlite:///SIMPLE.db'  # connection string for a SQLite database named SIMPLE.db
    felis_schema = "path/to/felis/schema.yaml"
    create_database(connection_string, felis_schema=felis_schema)

Accessing the Database
======================

To start using the database, launch Python, import the module,
then initialize the database with the :py:class:`astrodbkit.astrodb.Database()` class like so::

    from astrodbkit.astrodb import Database

    connection_string = 'sqlite:///SIMPLE.db'  # SQLite connection string
    db = Database(connection_string)

The database is now read to be used. If the database is empty, see below how to populate it.

.. note:: The :py:class:`astrodbkit.astrodb.Database()` class has many parameters that can be set to
          control the names of primary/reference tables. By default, these match the SIMPLE database, but users can
          configure them for their own needs and can pass them here or modify their __init__.py file.


When using PostgreSQL databases, it may be useful to pass along connection_arguments that specify the schema to use. For example::

    CONNECTION_STRING = "postgresql+psycopg2://user:password@server:port/database"
    REFERENCE_TABLES = [
        "Publications",
        "Surveys",
    ]

    db = Database(CONNECTION_STRING, 
        reference_tables=REFERENCE_TABLES, 
        connection_arguments={'options': '-csearch_path=my_schema'}
        )
    # This will use my_schema as the default schema for this connection

Loading the Database
--------------------

**Astrodbkit2** contains methods to output the full contents of the database as a list of JSON files.
It can likewise read in a directory of these files to populate the database. 
By default, reference tables (eg, Publications, Telescopes, etc) and source tables are respectively stored in `reference/` and `source/` sub-directories of `data/`.
This is how SIMPLE is currently version controlled. 

To load a database of this form, do the following::

    from astrodbkit.astrodb import Database

    connection_string = 'sqlite:///SIMPLE.db'  # SQLite connection string
    db_dir = 'data'  # directory where JSON files are located
    db = Database(connection_string)
    db.load_database(directory=db_dir, reference_directory="reference")

.. note:: Database contents are cleared when loading from JSON files to ensure that the database only contains
          sources from on-disk files. We describe later how to use the :py:meth:`~astrodbkit.astrodb.Database.save_db` method
          to produce JSON files from the existing database contents.

Loading SQLite databases with Windows
-------------------------------------

Large databases may significantly slow down when attempted to load to a SQLite binary file under Windows.
To avoid this, one can create the load database purely in memory and then connect to it when it's ready.
For example::

    from astrodbkit.astrodb import Database

    connection_string = 'sqlite:///SIMPLE.db'  # SQLite connection string
    db_dir = 'data'  # directory where JSON files are located

    # Create a temporary in-memory database and load to it
    db = Database('sqlite://')
    db.load_database(db_dir)
    # Dump in-memory database to file
    db.dump_sqlite('SIMPLE.db')

    # Connect to the newly created database as usual
    db = Database(connection_string)

Querying the Database
=====================

Upon connecting to a database, **AstrodbKit** creates methods for each table defined in the schema.
This allows for a more pythonic approach to writing queries. There are also methods to perform specialized queries.

Exploring the Schema
--------------------

The database schema is accessible via the :py:attr:`~astrodbkit.astrodb.Database.metadata` attribute.

For example, to see the available tables users can do::

    for table in db.metadata.tables:
        print(table)

And users can also examine column information for an existing table::

    for c in db.metadata.tables['Sources'].columns:
        print(c.name, c.type, c.primary_key, c.foreign_keys, c.nullable)

    # Example output
    source VARCHAR(100) True set() False
    ra FLOAT False set() True
    dec FLOAT False set() True
    shortname VARCHAR(30) False set() True
    reference VARCHAR(30) False {ForeignKey('Publications.name')} False
    comments VARCHAR(1000) False set() True

Specialized Searches
--------------------

Identifier (name) Search
~~~~~~~~~~~~~~~~~~~~~~~~

To search for an object by name, users can use the :py:meth:`~astrodbkit.astrodb.Database.search_object`
method to do fuzzy searches on the provided name, output results from any table,
and also include alternate Simbad names for their source. Refer to the API documentation for full details.

Search for TWA 27 and return default results in Astropy Table format::

    db.search_object('twa 27', fmt='astropy')

Search for TWA 27 and any of its alternate designations from Simbad and return results from the Names table::

    db.search_object('twa 27', resolve_simbad=True, output_table='Names')

Search for any source with 1357+1428 in its name and return results from the Photometry table in pandas Dataframe format::

    db.search_object('1357+1428', output_table='Photometry', fmt='astropy')

Inventory Search
~~~~~~~~~~~~~~~~

**AstrodbKit**  also contains an :py:meth:`~astrodbkit.astrodb.Database.inventory` method to return all data for a source by its name::

    data = db.inventory('2MASS J13571237+1428398')
    print(data)  # output as a dictionary, with individual tables as results

The pretty_print parameter can be passed to print out results to the screen in an easier to read format::

    db.inventory('2MASS J13571237+1428398', pretty_print=True)

    # Partial output:
    {
        "Sources": [
            {
                "source": "2MASS J13571237+1428398",
                "ra": 209.301675,
                "dec": 14.477722,
                "shortname": "1357+1428",
                "reference": "Schm10",
                "comments": null
            }
        ],
        "Names": [
            {
                "other_name": "2MASS J13571237+1428398"
            },
            {
                "other_name": "SDSS J135712.40+142839.8"
            }
        ],
        "Photometry": [
            {
                "band": "WISE_W1",
                "ucd": null,
                "magnitude": 13.348,
                "magnitude_error": 0.025,
                "telescope": "WISE",
                "instrument": null,
                "epoch": null,
                "comments": null,
                "reference": "Cutr12"
            },
            ...
        ]
    }

Region (spatial) Search
~~~~~~~~~~~~~~~~~~~~~~~

Another query method available in **AstrodbKit**  is :py:meth:`~astrodbkit.astrodb.Database.query_region`.
This performs a cone search around a given location for sources in the database.
It expects astropy SkyCoord and Quantity objects for the position and radius::

    db.query_region(SkyCoord(209.301675, 14.477722, frame='icrs', unit='deg'), radius=Quantity(60., unit='arcsec'))

Similar to :py:meth:`~astrodbkit.astrodb.Database.search_object`, a variety of options can be passed to control the output.
If the table with coordinate information is not the primary table, it can be specifed as well::

    db.query_region(SkyCoord(209., 14., frame='icrs', unit='deg'), output_table='Photometry')  # returning Photometry results for this search
    db.query_region(SkyCoord(209., 14., frame='icrs', unit='deg'), fmt='pandas')  # returning as a pandas DataFrame
    db.query_region(SkyCoord(209., 14., frame='icrs', unit='deg'), coordinate_table='Sources', ra_col='ra', dec_col='dec')  # specifying the name of the table with coordinate information

Full String Search
~~~~~~~~~~~~~~~~~~~~~~~

Similar to the Identifier Search above, one can perform a case-insensitive search for
any string against every string column in the database with :py:meth:`~astrodbkit.astrodb.Database.search_string`.
The output is a dictionary with keys for each table that matched results.
This can be useful to find all results matching a particular reference regardless of table::

    db.search_string('twa')  # search for any records with 'twa' anywhere in the database
    db.search_string('Cruz18', fuzzy_search=False)  # search for strings exactly matching Cruz19 anywhere in the database
    db.search_string('Cruz18', fuzzy_search=False, fmt='pandas')  # as above, but have each table as a pandas dataframe

General Queries
--------------------

Frequently, users may wish to perform specialized queries against the full database.
This can be used with the SQLAlchemy ORM and a convenience method, :py:attr:`~astrodbkit.astrodb.Database.query`, exists for this.
For more details on how to use SQLAlchemy, refer to `their documentation <https://docs.sqlalchemy.org/en/13/orm/>`_.
Here are a few examples.

Query all columns for the table Sources and output in a variety of formats::

    db.query(db.Sources).all()      # default SQLAlchemy output (list of named tuples)
    db.query(db.Sources).astropy()  # Astropy Table output
    db.query(db.Sources).table()    # equivalent to astropy
    db.query(db.Sources).pandas()   # Pandas DataFrame

Example query for sources with declinations larger than 0::

    db.query(db.Sources).filter(db.Sources.c.dec > 0).table()

Example query for rows with missing data::

    db.query(db.Publications).filter(db.Publications.c.doi.is_(None)).table()

Example query returning just a single column (source) and sorting sources by declination::

    db.query(db.Sources.c.source).order_by(db.Sources.c.dec).table()

Example query joining Sources and Publications tables and return just several of the columns::

    db.query(db.Sources.c.source, db.Sources.c.reference, db.Publications.c.name)\
            .join(db.Publications, db.Sources.c.reference == db.Publications.c.name)\
            .table()

Example queries showing how to perform ANDs and ORs::

    # Query with AND
    db.query(db.Sources).filter(and_(db.Sources.c.dec > 0, db.Sources.c.ra > 200)).all()

    # Query with OR
    db.query(db.Sources).filter(or_(db.Sources.c.dec < 0, db.Sources.c.ra > 200)).all()

In addition to using the ORM, it is useful to note that a :py:meth:`~astrodbkit.astrodb.Database.sql_query` method exists
to pass direct SQL queries to the database for users who may wish to write their own SQL statements::

    results = db.sql_query('select * from sources', fmt='astropy')
    print(results)

General Queries with Transformations
------------------------------------

**AstrodbKit** can convert columns to special types.
Currently, spectra transformations are implemented and the specified column would be converted to a `Spectrum1D` object
using the `specutils package <https://specutils.readthedocs.io/en/stable/>`_.
To call this, users can supply the name of the column to convert
(by default, none is converted, though .spectra assumes the column name is *spectrum*)::

    db.query(db.Spectra).astropy(spectra='spectrum')
    db.query(db.Spectra).pandas(spectra=['spectrum'])
    db.query(db.Spectra).spectra(fmt='astropy')

These three calls will return results from the Spectra table and will attempt to convert the *spectrum*
column to a Spectrum1D object for each row. Multiple columns to convert can also be passed as a list.
The parameter `spectra_format` can be specified if **specutils** is having trouble determining the type of spectrum.

Spectra need to be specified as either URL or paths relative to an environment variable,
for example `$ASTRODB_SPECTRA/infrared/myfile.fits`.
**AstrodbKit** would examine the environment variable `$ASTRODB_SPECTRA` and use that as
part of the absolute path to the file.

Working With Views
------------------

If your database contains views, they will not be included in the inventory methods or in the output JSON files. 
However, you can still work with and query data in them. 
The best way to define them is in the schema file, for example, with something like::

    SampleView = view(
        "SampleView",
        Base.metadata,
        sa.select(
            Sources.source.label("source"),
            Sources.ra.label("s_ra"),
            Sources.dec.label("s_dec"),
            SpectralTypes.spectral_type.label("spectral_type"),
        ).select_from(Sources).join(SpectralTypes, Sources.source == SpectralTypes.source)
        )

When created, the database will contain these views and users can reflect them to access them::
    
    import sqlalchemy as sa

    # Inspect the database to get a list of available views
    insp = sa.inspect(db.engine)
    print(insp.get_view_names())

    # Reflect a view and query it
    SampleView = sa.Table('SampleView', sa.MetaData())  
    insp.reflect_table(SampleView, include_columns=None)
    db.query(SampleView).table()

It is important that `sa.MetaData()` be used in the `sa.Table()` call as you don't want to modify 
the metadata of the actual database. 
If you don't do this and instead use `db.metadata`, you may end up with errors in other parts of AstrodbKit 
functionality as the view will be treated as a physical table.

Modifying Data
==============

As a wrapper against standard SQLAlchemy calls, data can be added fairly simply.

.. note:: Primary and Foreign keys, if present in the database, are verified when modifying data.
          This can prevent duplicated keys from being created and can propagate deletes or updates as specified
          in the database schema.

Adding Data
-----------

The simplest way to add data to an existing database is to construct a list of dictionaries and insert it to a table::

    sources_data = [{'ra': 209.301675, 'dec': 14.477722,
                     'source': '2MASS J13571237+1428398',
                     'reference': 'Schm10',
                     'shortname': '1357+1428'}]
    with db.engine.connect() as conn:
        conn.execute(db.Sources.insert().values(sources_data))
        conn.commit()  # sqlalchemy 2.0 does not autocommit

As a convenience method, users can use the :py:meth:`~astrodbkit.astrodb.Database.add_table_data` method
to load user-supplied tables into database tables. If not loading the primary table, the code will first check for
missing sources and print those out for the user to correct them. Column names should match those in the database, but
extra columns in the supplied table are ignored.
Currently, csv-formatted data, astropy Tables, and pandas DataFrames are supported.
For example::

    db.add_table_data('my_file.csv', table='Photometry', fmt='csv')

Updating Data
-------------

Similarly, rows can be updated with standard SQLAlchemy calls. 
For simplicity, here we use SQLAlchemy's "Begin Once" style which commits/rollbacks the transaction as needed; 
see https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#begin-once.
This example sets the shortname for a row that matches a Source with source name of 2MASS J13571237+1428398::

    with db.engine.begin() as conn:
        conn.execute(db.Sources.update()\
                .where(db.Sources.c.source == '2MASS J13571237+1428398')\
                .values(shortname='1357+1428'))

Deleting Data
-------------

Deleting rows can also be done. Here's an example that deletes all photometry with band name of WISE_W1::

    with db.engine.begin() as conn:
        conn.execute(db.Photometry.delete().where(db.Photometry.c.band == 'WISE_W1'))

Saving the Database
===================

If users perform changes to a database, they will want to output this to disk to be version controlled.
**Astrodbkit** provides methods to save an individual source or reference table as well as all of the data stored in the database. 
By default, reference tables are stored in a sub-directory of `data/` called "reference"; this can be overwritten by 
supplying a `reference_directory` variable into `save_database` or `save_reference_table`. 
Similarly, source/object tables are stored in a sub-directory of `data/` called "source" which can be overwritten by supplying  a `source_directory` variable.

We recommend using `save_database` as that outputs the entire database contents to disk::

    # Save single object
    db.save_json('2MASS J13571237+1428398', 'data')

    # Save single reference table
    db.save_reference_table('Publications', 'data')

    # Save entire database to directory 'data/' with 'reference/' and 'source/' subdirectories.
    db.save_database(directory='data', reference_directory='reference', source_directory='source')

.. note:: To properly capture database deletes, the contents of the specified directory is first cleared before
          creating JSON files representing the current state of the database.

Using the SQLAlchemy ORM
========================

The SQLAlchemy ORM (Object Relational Mapping) can be used for many of the examples provided above. 
This also allows for adding extra functionality to your schema, such as validation.

For example, the schema of your sources table could be written to validate RA/Dec as follows::

    from sqlalchemy import Column, Float, String
    from sqlalchemy.orm import validates

    class Sources(Base):
        """ORM for the sources table. This stores the main identifiers for our objects along with ra and dec"""

        __tablename__ = "Sources"
        source = Column(String(100), primary_key=True, nullable=False)
        ra = Column(Float)
        dec = Column(Float)

        @validates("ra")
        def validate_ra(self, key, value):
            if value > 360 or value < 0:
                raise ValueError("RA not in allowed range (0..360)")
            return value
        
        @validates("dec")
        def validate_dec(self, key, value):
            if value > 90 or value < -90:
                raise ValueError("Dec not in allowed range (-90..90)")
            return value

In your scripts, you can then create objects and populate them accordingly.
For example::

    from astrodbkit.astrodb import Database
    from schema import Sources

    db = Database(connection_string)

    # Create a new object and insert to database
    s = Sources(source="V4046 Sgr", ra=273.54, dec=-32.79)
    with db.session as session:
        session.add(s)
        session.commit()

If the RA or Dec fail the validation checks, the creation of the object will raise a ValueError exception. 

One can also use `session.add_all()` to insert a list of table entries and `session.delete()` to delete a single one. 
These options can facilitate the creation of robust ingest scripts that are both intuitive (ie, instantiating objects in a pythonic way) 
and that can take care of validating input values before they get to the database.


Advanced Database Considerations
================================

Here we provide useful tips or guidance when working with **AstrodbKit**.

Handling Relationships Between Object Tables
--------------------------------------------

Becuase **AstrodbKit** expects a single primary table, object tables that point back to it, and any number of reference tables, it can be difficult to handle relationships between object tables. 

As an example, consider the scenario where you want to store companion information to your sources, such as a table to store the relationship with orbital separation and a separate one to store general parameters. 
You may be calling these CompanionRelationship and CompanionParameters, respectively. 
If you want to link them together, you might decide to specify that the companion in the CompanionRelationship table should be a foreign key to the CompanionParameters table.
**However**, this will run into issues on database saving/loading as the output JSON files will no necessarily load tables in the order you expect. 
You might find it attempting to load CompanionParameters only to find that CompanionRelationship hasn't been loaded yet all the foreign keys are broken.

The better approach is to define a lookup table that will store the companion identifiers which will be used as the foreign keys. 
For example, a CompanionList table that both CompanionParameters and CompanionRelationship can refer to. 
This would be a reference table, similar to Telescopes or Publications, while CompanionParameters and CompanionRelationship would be object tables that require tying back to a specific source in the Sources table.
Essentially, this is normalizing the database a bit further and serves to avoid some common issues with foreign keys.

Reference/API
=============

.. toctree::
   :maxdepth: 2

   astrodb.rst
   utils.rst
   spectra.rst

Indices and tables

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
