# Testing for astrodb

import os
import json
import pytest
import io
import pandas as pd
from sqlalchemy.exc import IntegrityError
from astropy.table import Table
from astropy.coordinates import SkyCoord
from astropy.units.quantity import Quantity
from astropy.io import ascii
from astrodbkit2.astrodb import Database, create_database, Base, copy_database_schema
from astrodbkit2.schema_example import *
try:
    import mock
except ImportError:
    from unittest import mock


DB_PATH = 'temp.db'


def test_nodatabase():
    connection_string = 'sqlite:///:memory:'
    with pytest.raises(RuntimeError, match='Create database'):
        db = Database(connection_string)


@pytest.fixture(scope="module")
def db_dir(tmpdir_factory):
    return tmpdir_factory.mktemp("data")


@pytest.fixture(scope="module")
def db():
    # Create a fresh temporary database and assert it exists
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    connection_string = 'sqlite:///' + DB_PATH
    create_database(connection_string)
    assert os.path.exists(DB_PATH)

    # Connect to the new database and confirm it has the Sources table
    db = Database(connection_string)
    assert db
    assert 'source' in [c.name for c in db.Sources.columns]

    return db


def test_add_data(db):
    # Load example data to the database
    publications_data = [{'name': 'Schm10',
                          'bibcode': '2010AJ....139.1808S',
                          'doi': '10.1088/0004-6256/139/5/1808',
                          'description': 'Colors and Kinematics of L Dwarfs From the Sloan Digital Sky Survey'},
                         {'name': 'Cutr12',
                          'bibcode': '2012yCat.2311....0C',
                          'doi': None,
                          'description': 'WISE All-Sky Data Release'}]
    db.Publications.insert().execute(publications_data)

    # Add telescope
    db.Telescopes.insert().execute([{'name': 'WISE'}])

    # Add source
    sources_data = [{'ra': 209.301675, 'dec': 14.477722,
                     'source': '2MASS J13571237+1428398',
                     'reference': 'Schm10',
                     'shortname': '1357+1428'},
                    {'ra': 123, 'dec': -32, 'source': 'FAKE', 'reference': 'Schm10', 'shortname': 'FAKE'}]
    db.Sources.insert().execute(sources_data)

    # Additional names
    names_data = [{'source': '2MASS J13571237+1428398',
                   'other_name': 'SDSS J135712.40+142839.8'},
                  {'source': '2MASS J13571237+1428398',
                   'other_name': '2MASS J13571237+1428398'},
                  {'source': 'FAKE', 'other_name': 'Penguin'}
                  ]
    db.Names.insert().execute(names_data)

    # Add Photometry
    phot_data = [{'source': '2MASS J13571237+1428398',
                  'band': 'WISE_W1',
                  'magnitude': 13.348,
                  'magnitude_error': 0.025,
                  'telescope': 'WISE',
                  'reference': 'Cutr12'
                  },
                 {'source': '2MASS J13571237+1428398',
                  'band': 'WISE_W2',
                  'magnitude': 12.990,
                  'magnitude_error': 0.028,
                  'telescope': 'WISE',
                  'reference': 'Cutr12'
                  }]
    db.Photometry.insert().execute(phot_data)

    # Add SpectralType
    spt_data = [{'source': '2MASS J13571237+1428398',
                 'spectral_type': 13,
                 'spectral_type_error': None,
                 'regime': 'fake',
                 'best': 1,
                 'reference': 'Cutr12'
                 }]
    # First try with an incorrect regime value
    with pytest.raises(IntegrityError):
        db.SpectralTypes.insert().execute(spt_data)
    # Then with an accpeted regime value
    spt_data[0]['regime'] = 'infrared'
    db.SpectralTypes.insert().execute(spt_data)

    # Adding source with no ra/dec to test cone search
    sources_data = [{'source': 'Third star',
                     'reference': 'Schm10'}]
    db.Sources.insert().execute(sources_data)


def test_add_table_data(db):
    # Test the add_table_data method
    file = io.StringIO("""source,band,magnitude,telescope,reference
2MASS J13571237+1428398,WISE_W3,12.48,WISE,Cutr12
2MASS J13571237+1428398,WISE_W4,9.56,WISE,Cutr12
Not in DB,WISE_W4,0,WISE,Cutr12
""")
    with pytest.raises(RuntimeError):
        db.add_table_data(file, 'Photometry')

    # Actual data to load
    string_data = """source,band,magnitude,telescope,reference,extra column
2MASS J13571237+1428398,WISE_W3,12.48,WISE,Cutr12,blah blah
"""

    # Load as mocked CSV file
    file = io.StringIO(string_data)
    db.add_table_data(file, 'Photometry')

    # Delete and re-add as pandas DataFrame
    db.Photometry.delete().where(db.Photometry.c.band == 'WISE_W3').execute()
    file = io.StringIO(string_data)
    data = pd.read_csv(file)
    db.add_table_data(data, 'Photometry', fmt='pandas')

    # Delete and re-add as astropy Table
    db.Photometry.delete().where(db.Photometry.c.band == 'WISE_W3').execute()
    data = ascii.read(string_data, format='csv')
    db.add_table_data(data, 'Photometry', fmt='astropy')


def test_query_data(db):
    # Perform some example queries and confirm the results
    assert db.query(db.Publications).count() == 2
    assert db.query(db.Photometry).count() == 3
    assert db.query(db.Sources).count() == 3
    assert db.query(db.Sources.c.source).limit(1).all()[0][0] == '2MASS J13571237+1428398'


@mock.patch('astrodbkit2.astrodb.get_simbad_names', return_value=['fake'])
def test_search_object(mock_simbad, db):
    # Use the search_object method to do partial string searching

    t = db.search_object('nothing')
    assert len(t) == 0
    t = db.search_object('engu')
    assert len(t) == 1
    t = db.search_object('engu', fuzzy_search=False)
    assert len(t) == 0

    # Test pandas conversion
    t = db.search_object('engu', fmt='pandas')
    assert isinstance(t, pd.DataFrame)
    assert 'source' in t.columns  # check column names

    # Search but only consider the Sources.source column
    t = db.search_object('penguin', table_names={'Sources': ['source']})
    assert len(t) == 0
    # As before, but now resolve names with Simbad which will allow me to match 'fake'
    t = db.search_object('penguin', resolve_simbad=True, table_names={'Sources': ['source']})
    assert len(t) == 1

    # Search but return Photometry
    t = db.search_object('1357', output_table='Photometry')
    assert len(t) == 3

    # Two searches providing tables that do not exist
    with pytest.raises(RuntimeError):
        t = db.search_object('fake', output_table='NOTABLE')
    with pytest.raises(RuntimeError):
        t = db.search_object('fake', table_names={'NOTABLE': ['nocolumn']})


def test_search_string(db):
    d = db.search_string('fake')
    assert len(d['Sources']) > 0
    assert d['Sources']['source'] == 'FAKE'
    d = db.search_string('2mass', fuzzy_search=True)
    assert len(d) > 0
    d = db.search_string('2mass', fuzzy_search=False)
    assert len(d) == 0


def test_query_region(db):
    t = db.query_region(SkyCoord(0, 0, frame='icrs', unit='deg'))
    assert len(t) == 0, 'Found source around 0,0 when there should be none'

    t = db.query_region(SkyCoord(209.301675, 14.477722, frame='icrs', unit='deg'))
    assert len(t) == 1
    assert t['source'][0] == '2MASS J13571237+1428398', 'Did not find correct source'

    t = db.query_region(SkyCoord(209.301675, 14.477722, frame='icrs', unit='deg'), radius=Quantity(20, unit='arcsec'))
    assert len(t) == 1
    assert t['source'][0] == '2MASS J13571237+1428398', 'Did not find correct source'

    t = db.query_region(SkyCoord(209.302, 14.478, frame='icrs', unit='deg'), radius=60.)
    print(t)
    assert len(t) == 1, 'Did not find correct source in 1 arcmin search'

    t = db.query_region(SkyCoord(209.302, 14.478, frame='icrs', unit='deg'), radius=Quantity(1., unit='arcmin'))
    print(t)
    assert len(t) == 1, 'Did not find correct source in 1 arcmin search'

    t = db.query_region(SkyCoord(209.301675, 14.477722, frame='icrs', unit='deg'), output_table='Photometry')
    assert len(t) == 3, 'Did not return 3 photometry values'

    # Two searches providing tables that do not exist
    with pytest.raises(RuntimeError):
        t = db.query_region(SkyCoord(209, 14, frame='icrs', unit='deg'), output_table='NOTABLE')
    with pytest.raises(RuntimeError):
        t = db.query_region(SkyCoord(209, 14, frame='icrs', unit='deg'), coordinate_table='NOTABLE')


def test_sql_query(db):
    # Perform direct SQLite queries
    # Includes testing of _handle_format implicitly
    t = db.sql_query('SELECT * FROM Sources', fmt='default')
    assert len(t) == 3
    assert isinstance(t, list)
    t = db.sql_query('SELECT * FROM Sources', fmt='astropy')
    assert isinstance(t, Table)
    t = db.sql_query('SELECT * FROM Sources', fmt='table')
    assert isinstance(t, Table)
    t = db.sql_query('SELECT * FROM Sources', fmt='pandas')
    assert isinstance(t, pd.DataFrame)
    t = db.sql_query('SELECT * FROM Instruments', fmt='astropy')
    assert len(t) == 0
    assert isinstance(t, Table)
    with pytest.warns(DeprecationWarning):
        _ = db.sql_query('SELECT * FROM Sources', format='pandas')
    with pytest.raises(TypeError):
        _ = db.sql_query('SELECT * FROM Sources', format='pandas', fmt='pandas')


def test_query_formats(db):
    # Check that the query subclass is working properly
    t = db.query(db.Sources).astropy()
    assert len(t) == 3
    assert isinstance(t, Table)
    t = db.query(db.Sources).table()
    assert isinstance(t, Table)
    t = db.query(db.Instruments).table()
    assert len(t) == 0
    assert isinstance(t, Table)
    t = db.query(db.Sources).pandas()
    assert len(t) == 3
    assert isinstance(t, pd.DataFrame)
    t = db.query(db.Instruments).pandas()
    assert len(t) == 0
    assert isinstance(t, pd.DataFrame)


@mock.patch('astrodbkit2.astrodb.load_spectrum')
def test_query_spectra(mock_spectrum, db):
    # Test special conversions in query methods
    def fake_loader(x, spectra_format=None):
        if spectra_format is None:
            return f'SPECTRA {x}'
        else:
            return f'SPECTRA {x} with format {spectra_format}'
    mock_spectrum.side_effect = fake_loader

    t = db.query(db.Sources).pandas(spectra=['fake', 'second fake'])
    assert t['ra'][0] == 209.301675
    t = db.query(db.Sources).pandas(spectra=['ra'])
    assert t['ra'][0] == 'SPECTRA 209.301675'
    t = db.query(db.Sources).pandas(spectra='ra')
    assert t['ra'][0] == 'SPECTRA 209.301675'
    t = db.query(db.Sources).astropy(spectra='ra')
    assert t['ra'][0] == 'SPECTRA 209.301675'
    t = db.query(db.Sources).spectra(spectra='ra', fmt='pandas')
    assert t['ra'][0] == 'SPECTRA 209.301675'
    t = db.query(db.Sources).spectra(spectra='ra')
    assert t['ra'][0] == 'SPECTRA 209.301675'
    t = db.query(db.Sources).spectra(spectra='ra', spectra_format='SpeX')
    assert t['ra'][0] == 'SPECTRA 209.301675 with format SpeX'
    t = db.query(db.Sources).spectra(spectra='ra', spectra_format='SpeX', fmt='pandas')
    assert t['ra'][0] == 'SPECTRA 209.301675 with format SpeX'
    t = db.query(db.Instruments).table(spectra='name')
    assert len(t) == 0


def test_inventory(db):
    # Test the inventory method
    test_dict = {'Sources': [{'source': '2MASS J13571237+1428398',
                              'ra': 209.301675, 'dec': 14.477722,
                              'shortname': '1357+1428', 'reference': 'Schm10',
                              'comments': None}],
                 'Names': [{'other_name': '2MASS J13571237+1428398'},
                           {'other_name': 'SDSS J135712.40+142839.8'}],
                 'Photometry': [{'band': 'WISE_W1', 'ucd': None, 'magnitude': 13.348,
                                 'magnitude_error': 0.025, 'telescope': 'WISE', 'instrument': None,
                                 'epoch': None, 'comments': None, 'reference': 'Cutr12'},
                                {'band': 'WISE_W2', 'ucd': None, 'magnitude': 12.99,
                                 'magnitude_error': 0.028, 'telescope': 'WISE', 'instrument': None,
                                 'epoch': None, 'comments': None, 'reference': 'Cutr12'},
                                {'band': 'WISE_W3', 'ucd': None, 'magnitude': 12.48,
                                 'magnitude_error': None, 'telescope': 'WISE', 'instrument': None,
                                 'epoch': None, 'comments': None, 'reference': 'Cutr12'}],
                 'SpectralTypes': [{'spectral_type': 13,
                                    'spectral_type_error': None,
                                    'regime': 'infrared',
                                    'best': 1,
                                    'comments': None,
                                    'reference': 'Cutr12'}]
                 }

    assert db.inventory('2MASS J13571237+1428398') == test_dict


def test_save_reference_table(db, db_dir):
    # Test saving a reference table
    if os.path.exists(os.path.join(db_dir, 'Publications.json')):
        os.remove(os.path.join(db_dir, 'Publications.json'))
    db.save_reference_table('Publications', db_dir)
    assert os.path.exists(os.path.join(db_dir, 'Publications.json'))
    os.remove(os.path.join(db_dir, 'Publications.json'))  # explicitly removing so that the next step will get verified


def test_save_database(db, db_dir):
    # Test saving the database to JSON files

    # Clear temporary directory first
    # if not os.path.exists(DB_DIR):
    #     os.mkdir(DB_DIR)
    for file in os.listdir(db_dir):
        os.remove(os.path.join(db_dir, file))

    db.save_database(db_dir)

    # Check JSON data
    assert os.path.exists(os.path.join(db_dir, 'Publications.json'))
    assert os.path.exists(os.path.join(db_dir, '2mass_j13571237+1428398.json'))
    assert not os.path.exists(os.path.join(db_dir, '2mass_j13571237+1428398 2.json'))

    # Load source and confirm it is the same
    with open(os.path.join(db_dir, '2mass_j13571237+1428398.json'), 'r') as f:
        data = json.load(f)
    assert data == db.inventory('2MASS J13571237+1428398')


def test_load_database(db, db_dir):
    # Test loading database from JSON files

    # First clear some of the tables
    db.Publications.delete().execute()
    db.Sources.delete().execute()
    assert db.query(db.Publications).count() == 0
    assert db.query(db.Sources).count() == 0

    # Reload the database and check DB contents
    assert os.path.exists(db_dir)
    assert os.path.exists(os.path.join(db_dir, 'Publications.json'))
    db.load_database(db_dir, verbose=True)
    assert db.query(db.Publications).count() == 2
    assert db.query(db.Photometry).count() == 3
    assert db.query(db.Sources).count() == 3
    assert db.query(db.Sources.c.source).limit(1).all()[0][0] == '2MASS J13571237+1428398'

    # Clear temporary directory and files
    for file in os.listdir(db_dir):
        os.remove(os.path.join(db_dir, file))


def test_copy_database_schema():
    connection_1 = 'sqlite:///' + DB_PATH
    connection_2 = 'sqlite:///second.db'
    if os.path.exists('second.db'):
        os.remove('second.db')

    copy_database_schema(connection_1, connection_2, copy_data=True)

    db2 = Database(connection_2)
    assert db2
    assert 'source' in [c.name for c in db2.Sources.columns]
    assert db2.query(db2.Sources).count() == 3
    assert db2.query(db2.Publications).count() == 2
    assert db2.query(db2.Sources.c.source).limit(1).all()[0][0] == '2MASS J13571237+1428398'

    # Close the database and delete the temporary secondary file
    db2.session.close()
    db2.engine.dispose()
    if os.path.exists('second.db'):
        os.remove('second.db')


def test_remove_database(db):
    db.session.close()
    db.engine.dispose()
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
