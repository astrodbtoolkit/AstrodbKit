# Tests for spectra functions

from astrodbkit2.spectra import identify_spex_prism


def test_identify_spex_prism():
    filename = 'https://s3.amazonaws.com/bdnyc/SpeX/Prism/U10013_SpeX.fits'
    assert identify_spex_prism('read', filename)
    filename = 'I am not a valid spex prism file'
    assert not identify_spex_prism('read', filename)
