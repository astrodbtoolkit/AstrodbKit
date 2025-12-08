# Tests for spectra functions

from unittest import mock

import numpy as np
import pytest
from astropy.io import fits
from astropy.units import Unit

from astrodbkit.spectra import (
    _identify_spex,
    identify_spex_prism,
    load_spectrum,
    spex_prism_loader,
)


@pytest.fixture(scope="module")
def good_spex_file():
    fake_data = np.arange(1, 10, 1)
    n = np.array([fake_data, fake_data, fake_data])
    hdr = fits.Header()
    hdr["TELESCOP"] = "NASA IRTF"
    hdr["INSTRUME"] = "SPeX, IRTF Spectrograph"
    hdr["GRAT"] = "LowRes15 "
    hdr["XUNITS"] = "Microns "
    hdr["YUNITS"] = "ergs s-1 cm-2 A-1"
    hdu1 = fits.PrimaryHDU(n, header=hdr)
    return fits.HDUList([hdu1])


@pytest.fixture(scope="module")
def bad_spex_file():
    fake_data = np.arange(1, 10, 1)
    n = np.array([fake_data, fake_data, fake_data])
    hdr = fits.Header()
    hdr["TELESCOP"] = "MISSING"
    hdr["INSTRUME"] = "MISSING"
    hdr["GRAT"] = "MISSING"
    hdr["XUNITS"] = "UNKNOWN"
    hdu1 = fits.PrimaryHDU(n, header=hdr)
    return fits.HDUList([hdu1])


@mock.patch("astrodbkit.spectra.fits.open")
def test_identify_spex_prism(mock_fits_open, good_spex_file):
    mock_fits_open.return_value = good_spex_file

    filename = "https://s3.amazonaws.com/bdnyc/SpeX/Prism/U10013_SpeX.fits"
    assert identify_spex_prism("read", filename)
    filename = "I am not a valid spex prism file"
    assert not identify_spex_prism("read", filename)


@mock.patch("astrodbkit.spectra.fits.open")
def test_identify_spex(mock_fits_open, good_spex_file, bad_spex_file):
    mock_fits_open.return_value = good_spex_file
    assert _identify_spex("filename")
    mock_fits_open.return_value = bad_spex_file
    assert not _identify_spex("filename")


@mock.patch("astrodbkit.spectra.fits.open")
def test_load_spex_prism(mock_fits_open, good_spex_file, bad_spex_file):
    # Test good example
    mock_fits_open.return_value = good_spex_file
    spectrum = spex_prism_loader("filename")
    assert spectrum.unit == Unit("erg / (A cm2 s)")
    # Test bad example
    mock_fits_open.return_value = bad_spex_file
    spectrum = spex_prism_loader("filename")
    assert spectrum.unit == Unit("erg")


@mock.patch("astrodbkit.spectra.Spectrum.read")
def test_load_spectrum(mock_spectrum1d, monkeypatch):
    _ = load_spectrum("fake_file.txt")
    mock_spectrum1d.assert_called_with("fake_file.txt")
    _ = load_spectrum("fake_file.txt", spectra_format="SpeX")
    mock_spectrum1d.assert_called_with("fake_file.txt", format="SpeX")

    # Testing user-set environment variable
    monkeypatch.setenv("FAKE_ENV", "/User/path")
    _ = load_spectrum("$FAKE_ENV/to/my/fake_file.txt")
    mock_spectrum1d.assert_called_with("/User/path/to/my/fake_file.txt")


def test_load_spectrum_error():
    # Test error handling
    with pytest.raises(TypeError):
        _ = load_spectrum("fake_file.fits", raise_error=True)
