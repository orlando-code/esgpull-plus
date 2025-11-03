import re
from typing import Union, Optional
from datetime import datetime
import pandas as pd
import xarray as xa
import cftime

def to_time_slice_value(
    time_value: Union[int, str, datetime, pd.Timestamp, None],
    default_start: bool = True
) -> Optional[Union[str, pd.Timestamp]]:
    """
    Convert various time representations to a format suitable for xarray time slicing.
    
    Handles:
    - Integer years (e.g., 2015) -> converted to string "YYYY-01-01" or "YYYY-12-31"
    - String dates (e.g., "2015-01-01") -> returned as-is
    - datetime objects -> converted to pandas Timestamp
    - pandas Timestamp -> returned as-is
    - None -> returns None
    
    Args:
        time_value: Time value as integer year, string date, datetime, or Timestamp
        default_start: If True and time_value is an integer, use Jan 1; if False, use Dec 31
        
    Returns:
        String date or pandas Timestamp suitable for xarray time slicing, or None
    """
    if time_value is None:
        return None
    
    if isinstance(time_value, int):
        # Integer year - convert to date string
        if default_start:
            return f"{time_value}-01-01"
        else:
            return f"{time_value}-12-31"
    
    if isinstance(time_value, str):
        # String date - return as-is (xarray handles ISO format strings)
        return time_value
    
    if isinstance(time_value, datetime):
        # datetime object - convert to pandas Timestamp
        return pd.Timestamp(time_value)
    
    if isinstance(time_value, pd.Timestamp):
        # Already a Timestamp
        return time_value
    
    # Fallback: try to convert to string
    return str(time_value)


def calc_resolution(res: str | float | int) -> float:
    """
    Extract nominal resolution from file.nominal_resolution and return in degrees.
    Supports 'xx km', 'x x degree', or 'x degree'. Returns large value if unknown.
    Handles both string and numeric input.
    """
    if isinstance(res, (float, int)):
        return float(res)
    if not res:
        return 9999.0
    res = str(res).lower().replace(" ", "")
    if m := re.match(r"([\d.]+)km", res):
        return float(m.group(1)) / 111.0
    if m := re.match(r"([\d.]+)x([\d.]+)degree", res):
        return (float(m.group(1)) + float(m.group(2))) / 2.0
    if m := re.match(r"([\d.]+)degree", res):
        return float(m.group(1))
    return 9999.0


def process_xa_d(
    xa_d: xa.Dataset | xa.DataArray,
    rename_lat_lon_grids: bool = False,
    rename_mapping: dict = {
        "lat": "latitude",
        "lon": "longitude",
        "lev": "depth",
    },
    squeeze_coords: Optional[str | list[str]] = None,
    # crs: str = "EPSG:4326",
):
    """
    Process the input xarray Dataset or DataArray by standardizing coordinate names, squeezing dimensions,
    and sorting coordinates.
    
    Args:
        xa_d: xarray Dataset or DataArray to process
        rename_lat_lon_grids: If True, rename latitude/longitude to latitude_grid/longitude_grid
        rename_mapping: Mapping of coordinate names to new names
        squeeze_coords: Dimensions to squeeze
    
    Returns:
        Processed xarray Dataset or DataArray
    # TODO: tidy this up
    """
    temp_xa_d = xa_d.copy()

    # Optionally rename latitude/longitude to latitude_grid/longitude_grid
    if rename_lat_lon_grids:
        temp_xa_d = temp_xa_d.rename(
            {"latitude": "latitude_grid", "longitude": "longitude_grid"}
        )

    # Rename coordinates using mapping if present
    rename_dict = {
        coord: new_coord
        for coord, new_coord in rename_mapping.items()
        if coord in temp_xa_d.coords and new_coord not in temp_xa_d.coords
    }
    if rename_dict:
        temp_xa_d = temp_xa_d.rename(rename_dict)

    # Squeeze singleton 'band' dim and any user-specified dims
    if "band" in temp_xa_d.dims:
        temp_xa_d = temp_xa_d.squeeze("band")
    if squeeze_coords:
        temp_xa_d = temp_xa_d.squeeze(squeeze_coords)

    # Transpose to standard order
    dims = list(temp_xa_d.dims)
    if "time" in dims:
        order = [d for d in ["time", "latitude", "longitude"] if d in dims] + [
            d for d in dims if d not in ["time", "latitude", "longitude"]
        ]
        temp_xa_d = temp_xa_d.transpose(*order)
        # cast time from cftime.Datetime360Day to datetime.datetime if necessary
        if isinstance(temp_xa_d.time.values[0], cftime.Datetime360Day):
            datetime_idx = temp_xa_d.indexes["time"].to_datetimeindex(time_unit="ns")
            temp_xa_d["time"] = datetime_idx
    else:
        order = [d for d in ["latitude", "longitude"] if d in dims] + [
            d for d in dims if d not in ["latitude", "longitude"]
        ]
        temp_xa_d = temp_xa_d.transpose(*order)

    # Remove grid_mapping attribute if present
    temp_xa_d.attrs.pop("grid_mapping", None)

    # Drop non-data variables if present
    drop_vars = [
        v
        for v in ["time_bnds", "lat_bnds", "lon_bnds"]
        if v in getattr(temp_xa_d, "variables", {})
    ]
    if drop_vars and isinstance(temp_xa_d, xa.Dataset):
        temp_xa_d = temp_xa_d.drop_vars(drop_vars)

    # delete x, y coords if present
    if "x" in temp_xa_d.coords:
        temp_xa_d = temp_xa_d.drop_vars("x")
    if "y" in temp_xa_d.coords:
        temp_xa_d = temp_xa_d.drop_vars("y")

    # Sort by all dims
    return temp_xa_d.sortby(list(temp_xa_d.dims))
