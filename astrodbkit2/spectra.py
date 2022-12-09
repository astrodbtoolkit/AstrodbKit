"""Functions to handle loading of spectrum objects"""

import os
import numpy as np
import astropy.units as u
from astropy.wcs import WCS
from astropy.io import fits
from astropy.nddata import StdDevUncertainty
from astropy.units import Unit
from specutils import Spectrum1D
from specutils.io.registers import data_loader
from specutils.io.parsing_utils import read_fileobj_or_hdulist

# pylint: disable=no-member, unused-argument

def _identify_spex(filename):
    """
    Check whether the given file is a SpeX data product.
    """
    try:
        with fits.open(filename, memmap=False) as hdulist:
            return 'spex' in hdulist[0].header['INSTRUME'].lower() and \
                   'irtf' in hdulist[0].header['TELESCOP'].lower()
    except Exception:  # pylint: disable=broad-except,
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
def spex_prism_loader(filename, **kwargs):
    """Open a SpeX Prism file and convert it to a Spectrum1D object"""

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


def identify_wcs1d_multispec(origin, *args, **kwargs):
    """
    Identifier for WCS1D multispec
    """
    hdu = kwargs.get('hdu', 0)

    # Check if number of axes is one and dimension of WCS is greater than one
    with read_fileobj_or_hdulist(*args, **kwargs) as hdulist:
        return (hdulist[hdu].header.get('WCSDIM', 1) > 1 and
                hdulist[hdu].header['NAXIS'] > 1  and
                'WAT0_001' in hdulist[hdu].header and
                hdulist[hdu].header.get('WCSDIM', 1) == hdulist[hdu].header['NAXIS'] and
                'LINEAR' in hdulist[hdu].header.get('CTYPE1', ''))


@data_loader("wcs1d-multispec", identifier=identify_wcs1d_multispec, extensions=['fits'],
             dtype=Spectrum1D, priority=10)
def wcs1d_multispec_loader(file_obj, flux_unit=None,
                      hdu=0, verbose=False, **kwargs):
    """
    Loader for multiextension spectra as wcs1d. Adapted from wcs1d_fits_loader

    Parameters
    ----------
    file_obj : str, file-like or HDUList
        FITS file name, object (provided from name by Astropy I/O Registry),
        or HDUList (as resulting from astropy.io.fits.open()).
    flux_unit : :class:`~astropy.units.Unit` or str, optional
        Units of the flux for this spectrum. If not given (or None), the unit
        will be inferred from the BUNIT keyword in the header. Note that this
        unit will attempt to convert from BUNIT if BUNIT is present.
    hdu : int
        The index of the HDU to load into this spectrum.
    verbose : bool
        Print extra info.
    **kwargs
        Extra keywords for :func:`~specutils.io.parsing_utils.read_fileobj_or_hdulist`.

    Returns
    -------
    :class:`~specutils.Spectrum1D`
    """

    with read_fileobj_or_hdulist(file_obj, **kwargs) as hdulist:
        header = hdulist[hdu].header
        wcs = WCS(header)

        # Load data, convert units if BUNIT and flux_unit is provided and not the same
        if 'BUNIT' in header:
            data = u.Quantity(hdulist[hdu].data, unit=header['BUNIT'])
            if u.A in data.unit.bases:
                data = data * u.A/u.AA # convert ampere to Angroms
            if flux_unit is not None:
                data = data.to(flux_unit)
        else:
            data = u.Quantity(hdulist[hdu].data, unit=flux_unit)

    if wcs.wcs.cunit[0] == '' and 'WAT1_001' in header:
        # Try to extract from IRAF-style card or use Angstrom as default.
        wat_dict = dict((rec.split('=') for rec in header['WAT1_001'].split()))
        unit = wat_dict.get('units', 'Angstrom')
        if hasattr(u, unit):
            wcs.wcs.cunit[0] = unit
        else:  # try with unit name stripped of excess plural 's'...
            wcs.wcs.cunit[0] = unit.rstrip('s')
        if verbose:
            print(f"Extracted spectral axis unit '{unit}' from 'WAT1_001'")
    elif wcs.wcs.cunit[0] == '':
        wcs.wcs.cunit[0] = 'Angstrom'

    # Compatibility attribute for lookup_table (gwcs) WCS
    wcs.unit = tuple(wcs.wcs.cunit)

    # Identify the correct parts of the data to store
    if len(data.shape) > 1:
        flux_data = data[0]
    else:
        flux_data = data
    uncertainty = None
    if 'NAXIS3' in header:
        for i in range(header['NAXIS3']):
            if 'spectrum' in header.get(f'BANDID{i+1}', ''):
                flux_data = data[i]
            if 'sigma' in header.get(f'BANDID{i+1}', ''):
                uncertainty = data[i]

    # Reshape arrays if needed
    if len(flux_data) == 1 and len(flux_data.shape) > 1:
        flux_data = np.reshape(flux_data, -1)
        if uncertainty is not None:
            uncertainty = np.reshape(uncertainty, -1)

    # Convert uncertainty to StdDevUncertainty array
    if uncertainty is not None:
        uncertainty = StdDevUncertainty(uncertainty)

    # Manually generate spectral axis
    pixels = [[i] + [0]*(wcs.naxis-1) for i in range(wcs.pixel_shape[0])]
    spectral_axis = [i[0] for i in wcs.all_pix2world(pixels, 0)] * wcs.wcs.cunit[0]

    # Store header as metadata information
    meta = {'header': header}

    return Spectrum1D(flux=flux_data, spectral_axis=spectral_axis, uncertainty=uncertainty,
                      meta=meta)


def load_spectrum(filename, spectra_format=None):
    """Attempt to load the filename as a spectrum object"""

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
    except Exception as e:  # pylint: disable=broad-except, invalid-name
        print(f'Error loading {filename}: {e}')
        spec1d = filename

    return spec1d
