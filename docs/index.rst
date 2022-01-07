*************************
AstrodbKit2 Documentation
*************************

**AstrodbKit2** is an astronomical database handler code built on top of SQLAlchemy.
The goal behind this code is to provide SQLAlchemy's
powerful `Object Relational Mapping (ORM) <https://docs.sqlalchemy.org/en/13/orm/>`_
infrastructure to access astronomical database contents regardless of the underlying architecture.

**Astrodbkit2** is inspired from the original **astrodbkit**, which is hardcoded for the SQLite BDNYC database.

Introduction
============

Astronomical databases tend to focus on targets or observations with ancillary data that support them.
**Astrodbkit2** is designed to work with these types of databases and adopts several principles
for some of its more advanced features. These are:

 - There exists a primary table with object identifiers
 - There exists any number of supplementary tables that either refer back to the primary table or exist independently

For example, the `SIMPLE database <https://github.com/SIMPLE-AstroDB/SIMPLE-db>`_
(which was the initial design for **Astrodbkit2**) contains:

 - the primary Sources table, with coordinate information for each target
 - several object data tables, like Photometry, Spectra, etc, that contain information for each target
 - reference tables, like Publications, Telescopes, etc, that list other information that is used throughout the database, but doesn't refer to a particular target

The goal of **Astrodbkit2** is to link together the object tables together in order
to express them as a single entity, while still retaining the information for other reference tables.
**Astrodbkit2** can read and write out an entire target's data as a single JSON file for ease of transport and version
control. Reference tables are also written as JSON files, but organized differently-
a single file per table with multiple records.
An **Astrodbkit2**-supported database can thus be exported to two types of JSON files:
individual target files and reference table files
If your database is constructed in a similar fashion, it will work well with **Astrodbkit2**.
Other databases can still benefit from some of the functionality of **Astrodbkit2**,
but they might not work properly if attempting to use the save/load methods.

Getting Started
===============

To install **Astrodbkit2**, do::

    pip install astrodbkit2

or directly via the Github repo::

    pip install git+https://github.com/dr-rodriguez/AstrodbKit2

Creating a Database
===================

To create a database from scratch users will need a database schema coded with the SQLAlchemy ORM.
An example schema is provided (see schema_example.py),
but users can also refer to the `SIMPLE schema <https://github.com/SIMPLE-AstroDB/SIMPLE-db/blob/main/simple/schema.py>`_.
With that on hand, users should import their schema and prepare the database::

    from astrodbkit2.astrodb import create_database
    from simple.schema import *

    connection_string = 'sqlite:///SIMPLE.db'  # connection string for a SQLite database named SIMPLE.db
    create_database(connection_string)

Accessing the Database
======================

To start using the database, launch Python, import the module,
then initialize the database with the :py:class:`astrodbkit2.astrodb.Database()` class like so::

    from astrodbkit2.astrodb import Database

    connection_string = 'sqlite:///SIMPLE.db'  # SQLite connection string
    db = Database(connection_string)

The database is now read to be used. If the database is empty, see below how to populate it.

.. note:: The :py:class:`astrodbkit2.astrodb.Database()` class has many parameters that can be set to
          control the names of primary/reference tables. By default, these match the SIMPLE database, but users can
          configure them for their own needs and can pass them here or modify their __init__.py file.

Loading the Database
--------------------

**Astrodbkit2** contains methods to output the full contents of the database as a list of JSON files.
It can likewise read in a directory of these files to populate the database.
This is how SIMPLE is currently version controlled. To load a database of this form, do the following::

    from astrodbkit2.astrodb import Database

    connection_string = 'sqlite:///SIMPLE.db'  # SQLite connection string
    db_dir = 'data'  # directory where JSON files are located
    db = Database(connection_string)
    db.load_database(db_dir)

.. note:: Database contents are cleared when loading from JSON files to ensure that the database only contains
          sources from on-disk files. We describe later how to use the :py:meth:`~astrodbkit2.astrodb.Database.save_db` method
          to produce JSON files from the existing database contents.

Loading SQLite databases with Windows
-------------------------------------

Large databases may significantly slow down when attempted to load to a SQLite binary file under Windows.
To avoid this, one can create the load database purely in memory and then connect to it when it's ready.
For example::

    from astrodbkit2.astrodb import Database

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

Upon connecting to a database, **Astrodbkit2** creates methods for each table defined in the schema.
This allows for a more pythonic approach to writing queries. There are also methods to perform specialized queries.

Exploring the Schema
--------------------

The database schema is accessible via the :py:attr:`~astrodbkit2.astrodb.Database.metadata` attribute.

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

To search for an object by name, users can use the :py:meth:`~astrodbkit2.astrodb.Database.search_object`
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

**Astrodbkit2**  also contains an :py:meth:`~astrodbkit2.astrodb.Database.inventory` method to return all data for a source by its name::

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

Another query method available in **Astrodbkit2**  is :py:meth:`~astrodbkit2.astrodb.Database.query_region`.
This performs a cone search around a given location for sources in the database.
It expects astropy SkyCoord and Quantity objects for the position and radius::

    db.query_region(SkyCoord(209.301675, 14.477722, frame='icrs', unit='deg'), radius=Quantity(60., unit='arcsec'))

Similar to :py:meth:`~astrodbkit2.astrodb.Database.search_object`, a variety of options can be passed to control the output.
If the table with coordinate information is not the primary table, it can be specifed as well::

    db.query_region(SkyCoord(209., 14., frame='icrs', unit='deg'), output_table='Photometry')  # returning Photometry results for this search
    db.query_region(SkyCoord(209., 14., frame='icrs', unit='deg'), fmt='pandas')  # returning as a pandas DataFrame
    db.query_region(SkyCoord(209., 14., frame='icrs', unit='deg'), coordinate_table='Sources', ra_col='ra', dec_col='dec')  # specifying the name of the table with coordinate information

Full String Search
~~~~~~~~~~~~~~~~~~~~~~~

Similar to the Identifier Search above, one can perform a case-insensitive search for
any string against every string column in the database with :py:meth:`~astrodbkit2.astrodb.Database.search_string`.
The output is a dictionary with keys for each table that matched results.
This can be useful to find all results matching a particular reference regardless of table::

    db.search_string('twa')  # search for any records with 'twa' anywhere in the database
    db.search_string('Cruz18', fuzzy_search=False)  # search for strings exactly matching Cruz19 anywhere in the database
    db.search_string('Cruz18', fuzzy_search=False, fmt='pandas')  # as above, but have each table as a pandas dataframe

General Queries
--------------------

Frequently, users may wish to perform specialized queries against the full database.
This can be used with the SQLAlchemy ORM and a convenience method, :py:attr:`~astrodbkit2.astrodb.Database.query`, exists for this.
For more details on how to use SQLAlchemy, refer to `their documentation <https://docs.sqlalchemy.org/en/13/orm/>`_.
Here are a few examples.

Query all columns for the table Sources and output in a variety of formats::

    db.query(db.Sources).all()      # default SQLAlchemy output (list of named tuples)
    db.query(db.Sources).astropy()  # Astropy Table output
    db.query(db.Sources).table()    # equivalent to astropy
    db.query(db.Sources).pandas()   # Pandas DataFrame

Example query for sources with declinations larger than 0::

    db.query(db.Sources).filter(db.Sources.c.dec > 0).table()

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

In addition to using the ORM, it is useful to note that a :py:meth:`~astrodbkit2.astrodb.Database.sql_query` method exists
to pass direct SQL queries to the database for users who may wish to write their own SQL statements::

    results = db.sql_query('select * from sources', fmt='astropy')
    print(results)

General Queries with Transformations
------------------------------------

**Astrodbkit2** can convert columns to special types.
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
**AstrodbKit2** would examine the environment variable `$ASTRODB_SPECTRA` and use that as
part of the absolute path to the file.

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
    db.Sources.insert().execute(sources_data)

As a convenience method, users can use the :py:meth:`~astrodbkit2.astrodb.Database.add_table_data` method
to load user-supplied tables into database tables. If not loading the primary table, the code will first check for
missing sources and print those out for the user to correct them. Column names should match those in the database, but
extra columns in the supplied table are ignored.
Currently, csv-formatted data, astropy Tables, and pandas DataFrames are supported.
For example::

    db.add_table_data('my_file.csv', table='Photometry', fmt='csv')

Updating Data
-------------

Similarly, rows can be updated with standard SQLAlchemy calls.
This example sets the shortname for a row that matches a Source with source name of 2MASS J13571237+1428398::

    stmt = db.Sources.update()\
             .where(db.Sources.c.source == '2MASS J13571237+1428398')\
             .values(shortname='1357+1428')
    db.engine.execute(stmt)

Deleting Data
-------------

Deleting rows can also be done. Here's an example that deletes all photometry with band name of WISE_W1::

    db.Photometry.delete().where(db.Photometry.c.band == 'WISE_W1').execute()

Saving the Database
===================

If users perform changes to a database, they will want to output this to disk to be version controlled.
**Astrodbkit2** provides methods to save an individual source or reference table as well as the entire data.
We recommend the later to output the entire contents to disk::

    # Save single object
    db.save_json('2MASS J13571237+1428398', 'data')

    # Save single reference table
    db.save_reference_table('Publications', 'data')

    # Save entire database to directory 'data'
    db.save_database('data')

.. note:: To properly capture database deletes, the contents of the specified directory is first cleared before
          creating JSON files representing the current state of the database.

Reference/API
=============

.. toctree::
   :maxdepth: 2

   astrodb.rst
   utils.rst

Indices and tables

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
