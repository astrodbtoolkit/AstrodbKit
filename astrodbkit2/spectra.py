# Functions to handle loading of spectrum objects

import os
from specutils.io.registers import data_loader
from specutils import Spectrum1D
from astropy.io import fits
from astropy.nddata import StdDevUncertainty
from astropy.units import Unit


def _identify_spex(filename):
    """
    Check whether the given file is a SpeX data product.
    """
    try:
        with fits.open(filename, memmap=False) as hdulist:
            return 'spex' in hdulist[0].header['INSTRUME'].lower() and \
                   'irtf' in hdulist[0].header['TELESCOP'].lower()
    except Exception:
        return False


def identify_spex_prism(origin, *args, **kwargs):
    """
    Confirm this is a SpeX Prism FITS file.
    See FITS keyword reference at http://irtfweb.ifa.hawaii.edu/~spex/observer/
    Notes: GRAT has values of: ShortXD, Prism, LXD_long, LXD_short, SO_long, SO_short
    """
    is_spex = _identify_spex(args[0])
    if is_spex:
        with fits.open(args[0], memmap=False) as hdulist:
            return (isinstance(args[0], str) and
                    os.path.splitext(args[0].lower())[1] == '.fits' and
                    is_spex
                    and ('lowres' in hdulist[0].header['GRAT'].lower() or
                         'prism' in hdulist[0].header['GRAT'].lower())
                    )
    else:
        return is_spex


@data_loader("Spex Prism", identifier=identify_spex_prism, extensions=['fits'], dtype=Spectrum1D)
def load_spex_prism(filename, **kwargs):
    # Open a SpeX Prism file and convert it to a Spectrum1D object

    with fits.open(filename, **kwargs) as hdulist:
        header = hdulist[0].header

        tab = hdulist[0].data

        # Handle missing/incorrect units
        try:
            flux_unit = header['YUNITS'].replace('ergs', 'erg ').strip()
            wave_unit = header['XUNITS'].replace('Microns', 'um')
        except (KeyError, ValueError):
            # For now, assume some default units
            flux_unit = 'erg'
            wave_unit = 'um'

        wave, data = tab[0] * Unit(wave_unit), tab[1] * Unit(flux_unit)

        if tab.shape[0] == 3:
            uncertainty = StdDevUncertainty(tab[2])
        else:
            uncertainty = None

        meta = {'header': header}

    return Spectrum1D(flux=data, spectral_axis=wave, uncertainty=uncertainty, meta=meta)


def load_spectrum(filename, spectra_format=None):
    # Attempt to load the filename as a spectrum object

    # Convert filename if using environment variables
    if filename.startswith('$'):
        partial_path, _ = os.path.split(filename)
        while partial_path != '':
            partial_path, envvar_name = os.path.split(partial_path)
        abs_path = os.getenv(envvar_name[1:])
        if abs_path is not None:
            filename = filename.replace(envvar_name, abs_path)
        else:
            print(f'Could not find environment variable {envvar_name}')

    try:
        if spectra_format is not None:
            spec1d = Spectrum1D.read(filename, format=spectra_format)
        else:
            spec1d = Spectrum1D.read(filename)
    except Exception as e:
        print(f'Error loading {filename}: {e}')
        spec1d = filename

    return spec1d
