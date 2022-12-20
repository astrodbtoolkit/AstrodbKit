# Tests for spectra functions

import pytest
import numpy as np
from astropy.io import fits
from astropy.units import Unit
from astrodbkit2.spectra import identify_spex_prism, _identify_spex, load_spectrum, spex_prism_loader, identify_wcs1d_multispec, wcs1d_multispec_loader
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


@pytest.fixture(scope="module")
def good_wcs1dmultispec():
    n = np.empty((2141, 1, 4))
    hdr = fits.Header()
    hdr['WCSDIM'] = 3
    hdr['NAXIS'] = 3
    hdr['WAT0_001'] = 'system=equispec'
    hdr['WAT1_001'] = 'wtype=linear label=Wavelength units=angstroms'
    hdr['WAT2_001'] = 'wtype=linear'
    hdr['BANDID1'] = 'spectrum - background fit, weights variance, clean no'               
    hdr['BANDID2'] = 'raw - background fit, weights none, clean no'                        
    hdr['BANDID3'] = 'background - background fit'                                         
    hdr['BANDID4'] = 'sigma - background fit, weights variance, clean no'  
    hdr['CTYPE1'] = 'LINEAR  '
    hdr['BUNIT']   = 'erg/cm2/s/A'
    hdu1 = fits.PrimaryHDU(n, header=hdr)
    return fits.HDUList([hdu1])

@pytest.fixture(scope="module")
def alt_wcs1dmultispec():
    n = np.empty((2141,))
    hdr = fits.Header()
    hdr['WCSDIM'] = 1
    hdr['NAXIS'] = 1
    hdr['WAT0_001'] = 'system=equispec'
    hdr['WAT1_001'] = 'wtype=linear label=Wavelength units=angstroms'
    hdr['WAT2_001'] = 'wtype=linear'
    hdr['BANDID1'] = 'spectrum - background fit, weights variance, clean no'               
    hdr['BANDID2'] = 'raw - background fit, weights none, clean no'                        
    hdr['BANDID3'] = 'background - background fit'                                         
    hdr['BANDID4'] = 'sigma - background fit, weights variance, clean no'  
    hdr['CTYPE1'] = 'LINEAR  '
    hdr['BUNIT']   = 'erg/cm2/s/A'
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
    spectrum = spex_prism_loader('filename')
    assert spectrum.unit == Unit('erg / (A cm2 s)')
    # Test bad example
    mock_fits_open.return_value = bad_spex_file
    spectrum = spex_prism_loader('filename')
    assert spectrum.unit == Unit('erg')


@mock.patch('astrodbkit2.spectra.read_fileobj_or_hdulist')
def test_identify_wcs1d_multispec(mock_fits_open, good_wcs1dmultispec):
    mock_fits_open.return_value = good_wcs1dmultispec

    filename = 'https://s3.amazonaws.com/bdnyc/optical_spectra/U10929.fits'
    assert identify_wcs1d_multispec('read', filename)


@mock.patch('astrodbkit2.spectra.read_fileobj_or_hdulist')
def test_wcs1d_multispec_loader(mock_fits_open, good_wcs1dmultispec, alt_wcs1dmultispec):
    mock_fits_open.return_value = good_wcs1dmultispec

    spectrum = wcs1d_multispec_loader('filename')
    assert spectrum.unit == Unit('erg / (Angstrom cm2 s)')
    assert spectrum.wavelength.unit == Unit('Angstrom')

    # Check flux_unit is converted correctly
    spectrum = wcs1d_multispec_loader('filename', flux_unit=Unit('erg / (um cm2 s)'))
    assert spectrum.unit == Unit('erg / (um cm2 s)')

    # NAXIS=1 example
    mock_fits_open.return_value = alt_wcs1dmultispec
    spectrum = wcs1d_multispec_loader('filename')
    assert spectrum.unit == Unit('erg / (Angstrom cm2 s)')
    assert spectrum.wavelength.unit == Unit('Angstrom')


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
