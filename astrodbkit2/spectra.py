# Functions to handle loading of spectrum objects

import os
from specutils.io.registers import data_loader
from specutils import Spectrum1D
from astropy.io import fits
from astropy.nddata import StdDevUncertainty
from astropy.units import Unit


def identify_spex_prism(origin, *args, **kwargs):
    # TODO: Needs to be more specific, too many others match so format has to be explicitly provided
    return (isinstance(args[0], str) and
            'spex' in args[0].lower and
            os.path.splitext(args[0].lower())[1] == '.fits')


@data_loader("Spex Prism", identifier=identify_spex_prism, extensions=['fits'])
def load_spex(filename, **kwargs):
    # Open a SpeX Prism file and convert it to a Spectrum1D object

    with fits.open(filename, **kwargs) as hdulist:
        header = hdulist[0].header

        tab = hdulist[0].data
        flux_unit = header['YUNITS'].replace('ergs', 'erg')
        wave_unit = header['XUNITS'].replace('Microns', 'um')
        wave, data = tab[0] * Unit(wave_unit), tab[1] * Unit(flux_unit)
        if tab.shape[0] == 3:
            uncertainty = StdDevUncertainty(tab[2])
        else:
            uncertainty = None

        meta = {'header': header}

    return Spectrum1D(flux=data, spectral_axis=wave, uncertainty=uncertainty, meta=meta)


def load_spectrum(filename):
    if 'spex' in filename.lower():
        spec1d = Spectrum1D.read(filename, format='Spex Prism')
    else:
        spec1d = Spectrum1D.read(filename)
    return spec1d
