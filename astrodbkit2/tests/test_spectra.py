# Tests for spectra functions

import pytest
import numpy as np
from astropy.io import fits
from astropy.units import Unit
from astrodbkit2.spectra import identify_spex_prism, _identify_spex, load_spectrum, load_spex_prism
try:
    import mock
except ImportError:
    from unittest import mock


@pytest.fixture(scope="module")
def good_spex_file():
    n = np.empty((564, 3))
    hdr = fits.Header()
    hdr['TELESCOP'] = 'NASA IRTF'
    hdr['INSTRUME'] = 'SPeX, IRTF Spectrograph'
    hdr['GRAT'] = 'LowRes15 '
    hdr['XUNITS'] = 'Microns '
    hdr['YUNITS'] = 'ergs s-1 cm-2 A-1'
    hdu1 = fits.PrimaryHDU(n, header=hdr)
    return fits.HDUList([hdu1])


@pytest.fixture(scope="module")
def bad_spex_file():
    n = np.empty((564, 3))
    hdr = fits.Header()
    hdr['TELESCOP'] = 'MISSING'
    hdr['INSTRUME'] = 'MISSING'
    hdr['GRAT'] = 'MISSING'
    hdr['XUNITS'] = 'UNKNOWN'
    hdu1 = fits.PrimaryHDU(n, header=hdr)
    return fits.HDUList([hdu1])


@mock.patch('astrodbkit2.spectra.fits.open')
def test_identify_spex_prism(mock_fits_open, good_spex_file):
    mock_fits_open.return_value = good_spex_file

    filename = 'https://s3.amazonaws.com/bdnyc/SpeX/Prism/U10013_SpeX.fits'
    assert identify_spex_prism('read', filename)
    filename = 'I am not a valid spex prism file'
    assert not identify_spex_prism('read', filename)


@mock.patch('astrodbkit2.spectra.fits.open')
def test_identify_spex(mock_fits_open, good_spex_file, bad_spex_file):
    mock_fits_open.return_value = good_spex_file
    assert _identify_spex('filename')
    mock_fits_open.return_value = bad_spex_file
    assert not _identify_spex('filename')


@mock.patch('astrodbkit2.spectra.fits.open')
def test_load_spex_prism(mock_fits_open, good_spex_file, bad_spex_file):
    # Test good example
    mock_fits_open.return_value = good_spex_file
    spectrum = load_spex_prism('filename')
    assert spectrum.unit == Unit('erg / (A cm2 s)')
    # Test bad example
    mock_fits_open.return_value = bad_spex_file
    spectrum = load_spex_prism('filename')
    assert spectrum.unit == Unit('erg')


@mock.patch('astrodbkit2.spectra.Spectrum1D.read')
def test_load_spectrum(mock_spectrum1d, monkeypatch):
    _ = load_spectrum('fake_file.txt')
    mock_spectrum1d.assert_called_with('fake_file.txt')
    _ = load_spectrum('fake_file.txt', spectra_format='SpeX')
    mock_spectrum1d.assert_called_with('fake_file.txt', format='SpeX')

    # Testing user-set environment variable
    monkeypatch.setenv('FAKE_ENV', '/User/path')
    _ = load_spectrum('$FAKE_ENV/to/my/fake_file.txt')
    mock_spectrum1d.assert_called_with('/User/path/to/my/fake_file.txt')
