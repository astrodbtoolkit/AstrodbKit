import warnings

import pytest
import requests
from astroquery.simbad import Simbad
from astroquery.simbad import conf as simbad_conf

SIMBAD_MIRRORS = ("simbad.cds.unistra.fr", "simbad.harvard.edu", "simbad.idia.ac.za")


def set_simbad_mirror(server: str):
    """Point the shared astroquery Simbad object at the given mirror host."""
    server = server.replace("https://", "").replace("http://", "").strip("/")
    # astroquery only knows some mirrors by default; register others so the setter accepts them
    if server not in simbad_conf.servers_list:
        simbad_conf.servers_list = [*simbad_conf.servers_list, server]
    Simbad.server = server


def select_simbad_mirror(mirrors=SIMBAD_MIRRORS, timeout: float = 5):
    """Use the first SIMBAD mirror that responds. Return it, or None if none respond."""
    for server in mirrors:
        try:
            ok = requests.get(f"https://{server}/simbad/sim-tap/capabilities", timeout=timeout).ok
        except requests.exceptions.RequestException:
            ok = False
        if ok:
            set_simbad_mirror(server)
            return server
        warnings.warn(f"SIMBAD mirror {server} is not responding")
    warnings.warn("No SIMBAD mirror responded")
    return None


def pytest_collection_modifyitems(config, items):
    """Pick a working SIMBAD mirror; skip `simbad` tests if none respond."""
    simbad_items = [item for item in items if item.get_closest_marker("simbad")]
    if simbad_items and select_simbad_mirror() is None:
        for item in simbad_items:
            item.add_marker(pytest.mark.skip(reason="No SIMBAD mirror is reachable"))
