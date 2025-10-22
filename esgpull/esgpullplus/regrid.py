# #!/usr/bin/env python3
# # general
# import numpy as np
# import hashlib
# from pathlib import Path
# import tempfile
# import re
# import time
# from typing import Optional

# # spatial
# import xarray as xa
# import xesmf as xe
# from cdo import Cdo

# # parallel
# # from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# # import multiprocessing as mp
# # import os
# # import gc

# # rich
# from rich.console import Console
# from rich.table import Table
# from rich.panel import Panel
# from rich.progress import track


# def process_xa_d(
#     xa_d: xa.Dataset | xa.DataArray,
#     rename_lat_lon_grids: bool = False,
#     rename_mapping: dict = {
#         "lat": "latitude",
#         "lon": "longitude",
#         "lev": "depth",
#     },
#     squeeze_coords: Optional[str | list[str]] = None,
#     # crs: str = "EPSG:4326",
# ):
#     """
#     Process the input xarray Dataset or DataArray by standardizing coordinate names, squeezing dimensions,
#     and sorting coordinates.
#     """
#     temp_xa_d = xa_d.copy()

#     # Optionally rename latitude/longitude to latitude_grid/longitude_grid
#     if rename_lat_lon_grids:
#         temp_xa_d = temp_xa_d.rename(
#             {"latitude": "latitude_grid", "longitude": "longitude_grid"}
#         )

#     # Rename coordinates using mapping if present
#     rename_dict = {
#         coord: new_coord
#         for coord, new_coord in rename_mapping.items()
#         if coord in temp_xa_d.coords and new_coord not in temp_xa_d.coords
#     }
#     if rename_dict:
#         temp_xa_d = temp_xa_d.rename(rename_dict)

#     # Squeeze singleton 'band' dim and any user-specified dims
#     if "band" in temp_xa_d.dims:
#         temp_xa_d = temp_xa_d.squeeze("band")
#     if squeeze_coords:
#         temp_xa_d = temp_xa_d.squeeze(squeeze_coords)

#     # Transpose to standard order
#     dims = list(temp_xa_d.dims)
#     if "time" in dims:
#         order = [d for d in ["time", "latitude", "longitude"] if d in dims] + [
#             d for d in dims if d not in ["time", "latitude", "longitude"]
#         ]
#         temp_xa_d = temp_xa_d.transpose(*order)
#     else:
#         order = [d for d in ["latitude", "longitude"] if d in dims] + [
#             d for d in dims if d not in ["latitude", "longitude"]
#         ]
#         temp_xa_d = temp_xa_d.transpose(*order)

#     # Remove grid_mapping attribute if present
#     temp_xa_d.attrs.pop("grid_mapping", None)

#     # Drop non-data variables if present
#     drop_vars = [
#         v
#         for v in ["time_bnds", "lat_bnds", "lon_bnds"]
#         if v in getattr(temp_xa_d, "variables", {})
#     ]
#     if drop_vars and isinstance(temp_xa_d, xa.Dataset):
#         temp_xa_d = temp_xa_d.drop_vars(drop_vars)

#     # delete x, y coords if present
#     if "x" in temp_xa_d.coords:
#         temp_xa_d = temp_xa_d.drop_vars("x")
#     if "y" in temp_xa_d.coords:
#         temp_xa_d = temp_xa_d.drop_vars("y")

#     # Sort by all dims
#     return temp_xa_d.sortby(list(temp_xa_d.dims))



# class RegridderManager:
#     # TODO: getting a esmf warning when trying to regrid the same (deleted file): have to restart code
#     def __init__(self, ds=None, fs=None, target_res=None, periodic=True):
#         """
#         ds: xarray.Dataset with native curvilinear ocean grid
#         target_res: resolution as (lon_res, lat_res)
#         weight_dir: optional path to save/load weights
#         """
#         self.fs = fs
#         # Don't load the entire dataset - keep it lazy
#         self.ds = ds  # originally loaded
#         self.success_count = 0  # TODO: track successful regriddings
#         self.fail_count = 0
#         self.cdo_threads = self._configure_cdo_performance()
#         self.periodic = periodic
#         self.target_res = self.get_target_resolution() if not target_res else tuple(target_res)
#         self.varname = self._get_varname()
#         self.ds = self._convert_dataarray_to_dataset(self.ds)
#         self.ds = self._standardise_dims()
#         self.ds = self._standardise_coords()
#         self.ds = self._standardise_lon_limits()
#         self.ds = self._select_top_level()
#         self.weight_dir = self.fs.data.parent / "xesmf_regrid_weights" if fs else None  # type: ignore (linter being too fussy)
#         self.weight_dir.mkdir(exist_ok=True) if self.weight_dir else None

#     def _convert_dataarray_to_dataset(self, ds):
#         """
#         Convert DataArray to Dataset if necessary.
#         If ds is a DataArray, convert it to a Dataset with the variable name as the only data variable.
#         """
#         if isinstance(ds, xa.DataArray):
#             return xa.Dataset({self.varname: ds})
#         elif isinstance(ds, xa.Dataset):
#             return ds
#         else:
#             raise ValueError("Input must be an xarray.DataArray or xarray.Dataset")

#     def get_target_resolution(self):
#         """
#         Estimate target resolution based on the dataset's native grid.
#         If target_res is not provided, use the native resolution of the dataset.
#         """
#         res = self.ds.attrs.get("nominal_resolution", None) if self.ds else None
#         if res is None:
#             # Try to infer resolution from coordinates
#             res = self._infer_resolution_from_coords()
#         else:
#             res = str(res).lower().replace(" ", "")
#             if m := re.match(r"([\d.]+)\s*km", res):
#                 val = m.group(1)
#                 if val == "10":
#                     res = (0.1, 0.1)
#                 elif val == "25":
#                     res = (0.25, 0.25)
#                 elif val == "50":
#                     res = (0.5, 0.5)
#                 else:
#                     res = float(val)
#             elif m := re.match(r"([\d.]+)x([\d.]+)degree", res):
#                 res = (float(m.group(1)), float(m.group(2)))
#             else:
#                 print(f"Could not parse resolution from {res}, using default (1, 1)")
#                 res = (1, 1)
#         # assume res is a tuple (lon_res, lat_res) and the same
#         if isinstance(res, (int, float)):
#             res = (res, res)
#         return res

#     def _infer_resolution_from_coords(self):
#         """
#         Infer resolution from the dataset's coordinates.
#         Assumes coordinates are evenly spaced.
#         Returns (1, 1) if unable to infer.
#         """
#         try:
#             lon = self.ds["lon"] if "lon" in self.ds.coords else self.ds["longitude"]
#             lat = self.ds["lat"] if "lat" in self.ds.coords else self.ds["latitude"]
#             if lon.ndim == 1 and lat.ndim == 1 and len(lon) > 1 and len(lat) > 1:
#                 lon_res = float(np.abs(lon[1] - lon[0]))
#                 lat_res = float(np.abs(lat[1] - lat[0]))
#             elif (
#                 lon.ndim == 2
#                 and lat.ndim == 2
#                 and lon.shape[0] > 1
#                 and lon.shape[1] > 1
#             ):
#                 lon_res = float(np.abs(lon[:, 1] - lon[:, 0]).mean())
#                 lat_res = float(np.abs(lat[1, :] - lat[0, :]).mean())
#             else:
#                 # Fallback if dimensions are not as expected
#                 return (1, 1)
#             # round lon_res and lat_res to closest quarter of a degree
#             lon_res = abs(round(lon_res * 4) / 4)
#             lat_res = abs(round(lat_res * 4) / 4)
#             return (lon_res, lat_res)
#         except Exception:
#             return (1, 1)
        
#     def _select_top_level(self):
#         """
#         Select the top level of the dataset.
#         """
#         if self.ds:
#             self.ds = self.ds.isel(lev=0) if "lev" in self.ds.dims else self.ds
#             self.ds = self.ds.isel(depth=0) if "depth" in self.ds.dims else self.ds
#             print("Selected top level of dataset")
#         return self.ds

#     def _get_varname(self, ncfile=None):
#         if isinstance(self.ds, xa.DataArray):
#             try:
#                 return self.ds.name
#             except AttributeError:
#                 print("DataArray has no name, cannot determine variable name.")
#                 return None
#         varname = None
#         for v in self.ds.data_vars if self.ds else []:
#             if not any(sub in v.lower() for sub in ["bnds", "vertices"]):
#                 varname = v
#                 break
#         if varname is None:
#             print(f"No suitable variable found in {ncfile}")
#             return None
#         return varname

#     def _standardise_dims(self):
#         # Robustly assign 'x' to longitude and 'y' to latitude, even if i/j are swapped
#         dim_map = {}
#         dims = list(self.ds.dims) if self.ds else []
#         # If both i and j are present, decide which is x (lon) and which is y (lat) by shape
#         if "i" in dims and "j" in dims:
#             i_len = self.ds.sizes["i"] if self.ds else None
#             j_len = self.ds.sizes["j"] if self.ds else None
#             # Longitude usually has more points than latitude
#             if i_len and j_len and i_len > j_len:
#                 dim_map["i"] = "x"  # i is longitude
#                 dim_map["j"] = "y"  # j is latitude
#             else:
#                 dim_map["i"] = "y"  # i is latitude
#                 dim_map["j"] = "x"  # j is longitude
#         else:
#             if "i" in dims:
#                 dim_map["i"] = "y"
#             if "j" in dims:
#                 dim_map["j"] = "x"
#         # self.ds = self.ds.rename_dims(dim_map)
#         # self.ds = self.ds.rename(dim_map)
#         if dim_map:
#             self.ds = self.ds.swap_dims(dim_map)            
#             coord_map = {}
#             for old_dim, new_dim in dim_map.items():
#                 if old_dim in self.ds.coords:
#                     coord_map[old_dim] = new_dim
            
#             if coord_map:
#                 self.ds = self.ds.rename(coord_map)
#         return self.ds

#     def _standardise_coords(self):
#         # Ensure 'lat' and 'lon' are present and correctly named
#         if self.ds:
#             self.ds = (
#                 self.ds.rename({"latitude": "lat"})
#                 if "latitude" in self.ds.coords and "lat" not in self.ds.coords
#                 else self.ds
#             )
#             self.ds = (
#                 self.ds.rename({"longitude": "lon"})
#                 if "longitude" in self.ds.coords and "lon" not in self.ds.coords
#                 else self.ds
#             )

#         return self.ds

#     def _make_grid_in(self):
#         lons = self.ds["lon"].values if self.ds else None
#         lats = self.ds["lat"].values if self.ds else None

#         if lons.ndim == 2 and lons.shape[1] > lons.shape[0]:
#             lons = lons.T
#             lats = lats.T

#         lons = np.asfortranarray(lons)
#         lats = np.asfortranarray(lats)

#         if lons.ndim == 1 and lats.ndim == 1:
#             return xa.Dataset({"lon": (["x"], lons), "lat": (["y"], lats)})
#         elif lons.ndim == 2 and lats.ndim == 2:
#             return xa.Dataset(
#                 {"lon": (["x", "y"], lons), "lat": (["x", "y"], lats)}
#             )  # TODO: I think this is still sometimes failing (different for different files)
#         else:
#             raise ValueError(
#                 f"Unsupported dimensions: lon: {lons.ndim}, lat: {lats.ndim}. Expected 1D or 2D array."
#             )

#     def _standardise_lon_limits(self):
#         lon = self.ds["lon"] if "lon" in self.ds.coords else self.ds["longitude"]

#         # If all longitudes are >= 0, shift to -180..180/360
#         if np.all(lon.values >= 0):
#             lon = ((lon - 180) % 360) - 180
#             # Also update the dataset so downstream code uses shifted lons
#             if "lon" in self.ds.coords:
#                 self.ds = self.ds.assign_coords(lon=lon)
#             else:
#                 self.ds = self.ds.assign_coords(longitude=lon)
#         return self.ds

#     def _make_grid_out(self):
#         lon_res, lat_res = self.target_res

#         target_lon = np.arange(-180, 180 + lon_res, lon_res)
#         target_lat = np.arange(-90, 90 + lat_res, lat_res)
#         return xa.Dataset({"lon": (["lon"], target_lon), "lat": (["lat"], target_lat)})

#     def _weights_filename(self):
#         # Hash the shape of input grid to ensure reuse
#         id_str = f"{self.ds['lon'].shape}-{self.target_res}"
#         hex_hash = hashlib.md5(id_str.encode()).hexdigest()

#         return self.weight_dir / f"regrid_weights_{hex_hash}.nc"

#     def _get_or_create_regridder(self):
#         grid_in = self._make_grid_in()
#         grid_out = self._make_grid_out()
#         weights_path = self._weights_filename() if self.weight_dir else None

#         if weights_path and weights_path.exists():
#             return xe.Regridder(
#                 grid_in,
#                 grid_out,
#                 method="bilinear",
#                 periodic=self.periodic,
#                 filename=weights_path,
#                 reuse_weights=True,
#             )
#         else:
#             return xe.Regridder(
#                 grid_in,
#                 grid_out,
#                 method="bilinear",
#                 periodic=self.periodic,
#                 ignore_degenerate=True,
#                 # filename=weights_path,
#             )

#     def _trim_unnecessary_vals(self):
#         # remove i,j,latitude, longitude coords
#         coords_to_remove = ["i", "j", "latitude", "longitude"]
#         for coord in coords_to_remove:
#             if coord in self.ds.coords:
#                 self.ds = self.ds.drop_vars(coord)
#         # remove any bounds data variables
#         bounds_vars = [
#             v
#             for v in self.ds.data_vars
#             if "bnds" in v.lower() or "vertices" in v.lower()
#         ]
#         for var in bounds_vars:
#             if var in self.ds.data_vars:
#                 self.ds = self.ds.drop_vars(var)
#         return self.ds

#     def _trim_unnecessary_vals_from_ds(self, ds):
#         """
#         Remove unnecessary coordinates and variables from a dataset.
#         This is a static version of _trim_unnecessary_vals that works on any dataset.
#         """
#         # remove i,j,latitude, longitude coords
#         coords_to_remove = ["i", "j", "latitude", "longitude"]
#         for coord in coords_to_remove:
#             if coord in ds.coords:
#                 ds = ds.drop_vars(coord)
#         # remove any bounds data variables
#         bounds_vars = [
#             v for v in ds.data_vars if "bnds" in v.lower() or "vertices" in v.lower()
#         ]
#         for var in bounds_vars:
#             if var in ds.data_vars:
#                 ds = ds.drop_vars(var)
#         return ds

#     def _cdo_weights_filename(self, grid_type: str, xsize: int, ysize: int):
#         """Generate a unique filename for CDO weights based on grid parameters."""
#         if not self.weight_dir:
#             return None

#         # Create unique identifier for this grid configuration
#         grid_id = (
#             f"cdo_{grid_type}_{xsize}x{ysize}_{self.ds.sizes.get('ncells', 'unknown')}"
#         )
#         hex_hash = hashlib.md5(grid_id.encode()).hexdigest()

#         return self.weight_dir / f"cdo_weights_{hex_hash}.nc"

#     def _configure_cdo_performance(self):
#         """Configure CDO for optimal performance."""
#         import os
        
#         # Set CDO environment variables for better performance
#         os.environ["CDO_NETCDF_COMPRESSION"] = "0"  # Disable compression for speed
#         os.environ["CDO_NETCDF_64BIT_OFFSET"] = "1"  # Use 64-bit offsets for large files
#         os.environ["CDO_NETCDF_USE_PARALLEL"] = "1"  # Enable parallel I/O
        
#         # Set number of threads for CDO
#         import multiprocessing
#         cdo_threads = min(4, multiprocessing.cpu_count())
#         os.environ["CDO_NUM_THREADS"] = str(cdo_threads)
        
#         return cdo_threads


#     def regrid_with_cdo(
#         self,
#         ds: Optional[xa.Dataset] = None,
#         grid_type: str = "remapcon",
#         xsize: int = 360,
#         ysize: int = 180,
#         xfirst: float = -179.5,
#         xinc: float = 1.0,
#         yfirst: float = -89.5,
#         yinc: float = 1.0,
#         reuse_weights: bool = True,
#         chunk_size: str = "auto",
#     ) -> xa.Dataset:
#         """
#         Optimized CDO regridding for large files with chunking and parallel processing.
#         """
#         cdo = Cdo()
#         ds = ds if ds is not None else self.ds
        
#         # Get weights filename if reuse is enabled
#         weights_path = None
#         if reuse_weights:
#             weights_path = self._cdo_weights_filename(grid_type, xsize, ysize)

#         with tempfile.TemporaryDirectory() as tmpdir:
#             input_path = Path(tmpdir) / "input.nc"
#             output_path = Path(tmpdir) / "output.nc"
#             gridfile_path = Path(tmpdir) / "grid.txt"

#             # Create CDO target grid description
#             with open(gridfile_path, "w") as f:
#                 f.write(
#                     f"""gridtype = lonlat
#     xsize = {xsize}
#     ysize = {ysize}
#     xfirst = {xfirst}
#     xinc = {xinc}
#     yfirst = {yfirst}
#     yinc = {yinc}
#     """
#                 )

#             # Check if we can reuse existing weights
#             if weights_path and weights_path.exists():
#                 print(f"Reusing CDO weights: {weights_path.name}")
                
#                 # Use chunked processing for large files
#                 if "time" in ds.dims and len(ds.time) > 1:
#                     # Process in time chunks
#                     ds_out = self._regrid_with_chunks_cdo(
#                         ds, cdo, gridfile_path, weights_path, output_path, chunk_size
#                     )
#                 else:
#                     # Process single time step
#                     ds.to_netcdf(input_path)
#                     cdo.remap(
#                         str(gridfile_path),
#                         str(weights_path),
#                         input=str(input_path),
#                         output=str(output_path),
#                     )
#                     ds_out = xa.open_dataset(output_path)
#             else:
#                 # Generate new weights using a small sample
#                 if weights_path:
#                     print(f"Generating new CDO weights: {weights_path.name}")
#                     temp_weights = Path(tmpdir) / "temp_weights.nc"
                    
#                     # Use first time step for weight generation (much faster)
#                     ds_sample = ds.isel(time=0) if "time" in ds.dims else ds
#                     ds_sample.to_netcdf(input_path)
                    
#                     # Generate weights from sample
#                     getattr(cdo, f"gen{grid_type[5:]}")(  # remove "remap" prefix
#                         str(gridfile_path),
#                         input=str(input_path),
#                         output=str(temp_weights),
#                     )
                    
#                     # Copy weights to permanent location
#                     import shutil
#                     shutil.copy2(temp_weights, weights_path)
                    
#                     # Now process the full dataset with chunks
#                     if "time" in ds.dims and len(ds.time) > 1:
#                         ds_out = self._regrid_with_chunks_cdo(
#                             ds, cdo, gridfile_path, weights_path, output_path, chunk_size
#                         )
#                     else:
#                         ds.to_netcdf(input_path)
#                         cdo.remap(
#                             str(gridfile_path),
#                             str(weights_path),
#                             input=str(input_path),
#                             output=str(output_path),
#                         )
#                         ds_out = xa.open_dataset(output_path)
#                 else:
#                     # Direct regridding without weight saving
#                     print("CDO regridding without weight caching")
#                     if "time" in ds.dims and len(ds.time) > 1:
#                         ds_out = self._regrid_with_chunks_cdo(
#                             ds, cdo, gridfile_path, None, output_path, chunk_size, grid_type
#                         )
#                     else:
#                         ds.to_netcdf(input_path)
#                         getattr(cdo, grid_type)(
#                             str(gridfile_path),
#                             input=str(input_path),
#                             output=str(output_path),
#                         )
#                         ds_out = xa.open_dataset(output_path)

#             return ds_out

#     def _regrid_with_chunks_cdo(
#         self, ds, cdo, gridfile_path, weights_path, output_path, chunk_size, grid_type=None
#     ):
#         """
#         Regrid large datasets in time chunks to reduce memory usage.
#         """
#         # Determine optimal chunk size
#         if chunk_size == "auto":
#             # Use 10 time steps or 1GB chunks, whichever is smaller
#             time_chunks = min(10, max(1, len(ds.time) // 10))
#         else:
#             time_chunks = int(chunk_size)
        
#         # Process in chunks
#         chunked_results = []
        
#         for i in track(range(0, len(ds.time), time_chunks), description="Regridding chunks", total=len(ds.time) // time_chunks):
#             end_idx = min(i + time_chunks, len(ds.time))
#             ds_chunk = ds.isel(time=slice(i, end_idx))
            
#             with tempfile.TemporaryDirectory() as tmpdir:
#                 chunk_input = Path(tmpdir) / "chunk_input.nc"
#                 chunk_output = Path(tmpdir) / "chunk_output.nc"
                
#                 ds_chunk.to_netcdf(chunk_input)
                
#                 if weights_path and weights_path.exists():
#                     cdo.remap(
#                         str(gridfile_path),
#                         str(weights_path),
#                         input=str(chunk_input),
#                         output=str(chunk_output),
#                     )
#                 else:
#                     getattr(cdo, grid_type)(
#                         str(gridfile_path),
#                         input=str(chunk_input),
#                         output=str(chunk_output),
#                     )
                
#                 chunk_result = xa.open_dataset(chunk_output)
#                 chunked_results.append(chunk_result)
        
#         # Combine results
#         return xa.concat(chunked_results, dim="time")

#     def regrid_large_file_optimized(self, ds: xa.Dataset = None, reuse_weights: bool = True):
#         """
#         Optimized regridding for large files, choosing the best method automatically.
#         """
#         ds = ds if ds is not None else self.ds
        
#         # Check file size and choose method
#         file_size_gb = ds.nbytes / (1024**3)
        
#         if file_size_gb > 1.0:
#             print(f"Large file detected ({file_size_gb:.1f} GB), using optimized xESMF")
#             return self._regrid_large_with_xesmf(ds, reuse_weights)
#         else:
#             print("Small file, using CDO")
#             return self.regrid_with_cdo(ds, reuse_weights=reuse_weights)

#     def _regrid_large_with_xesmf(self, ds, reuse_weights=True):
#         """
#         Optimized xESMF regridding for large files with chunking.
#         """
#         # Use xESMF with chunked processing
#         regridder = self._get_or_create_regridder()
        
#         if "time" in ds.dims and len(ds.time) > 10:
#             # Process in time chunks
#             chunk_size = min(10, len(ds.time) // 10)
#             results = []
            
#             for i in range(0, len(ds.time), chunk_size):
#                 end_idx = min(i + chunk_size, len(ds.time))
#                 ds_chunk = ds.isel(time=slice(i, end_idx))
                
#                 # Regrid chunk
#                 result_chunk = regridder(ds_chunk)
#                 results.append(result_chunk)
            
#             # Combine results
#             return xa.concat(results, dim="time")
#         else:
#             # Process entire dataset
#             return regridder(ds)

#     def regrid_ds(
#         self, ds: xa.Dataset = None, reuse_weights: bool = True
#     ) -> xa.Dataset:
#         """
#         Regrid a dataset using the current regridder.
#         This is a convenience method to regrid without needing to manage the regridder directly.

#         Parameters
#         ----------
#         ds : xa.Dataset, optional
#             Dataset to regrid. If None, uses self.ds
#         reuse_weights : bool
#             Whether to reuse weight files for faster CDO regridding
#         """
#         # Update the instance dataset
#         self.ds = ds if ds is not None else self.ds
#         self.varname = self._get_varname()

#         if self.varname is None:
#             raise ValueError("No suitable variable found for regridding")

#         if "ncells" in self.ds.dims:
#             # unstructured grid, use CDO with weight reuse
#             print("Unstructured grid detected, using CDO")
#             lon_res, lat_res = self.target_res
#             xsize = int(360 / lon_res)
#             ysize = int(180 / lat_res)
#             out_ds = self.regrid_with_cdo(
#                 ds, xsize=xsize, ysize=ysize, reuse_weights=reuse_weights
#             )
#             return out_ds
#         else:
#             # Regular grid, use xESMF (already supports weight reuse)
#             print("Structured grid detected, using xESMF")
#             data = self.ds[self.varname]
#             data = data.where(np.isfinite(data), drop=False)
#             data.values[:] = np.ascontiguousarray(data.values)
#             self.regridder = self._get_or_create_regridder()
#             regridded_data = self.regridder(data)

#             # Create output dataset
#             out_ds = self.ds.copy()
#             out_ds[self.varname] = regridded_data
#             out_ds = self._trim_unnecessary_vals_from_ds(out_ds)
#             return out_ds


# def regrid_files_by_subdirectory(
#     watch_dir,
#     subdir="reprojected",
#     delete_original=False,
#     fs=None,
#     # max_workers=1,
#     target_res=None,
#     periodic=True,
#     reuse_weights=True,
# ):
#     """
#     Regrid files by subdirectory, creating separate regridding weights for each subdirectory.
#     This ensures that files with the same grid (in the same subdirectory) share weights.
#     """
#     watch_dir = Path(watch_dir)
#     files_to_regrid = []
#     skipped = 0

#     console = Console()
#     start_time = time.localtime()
#     console.print(f":clock3: START: {time.strftime('%Y-%m-%d %H:%M:%S', start_time)}\n")

#     console.print(f"[blue]Scanning directory:[/blue] {watch_dir}")
    
#     # Find all NetCDF files that need regridding
#     for ncfile in watch_dir.rglob("*.nc*"):
#         if "reprojected" in str(ncfile) or "regrid_weights" in str(ncfile):
#             continue
#         parent_dir = ncfile.parent
#         out_dir = parent_dir / subdir
#         out_dir.mkdir(exist_ok=True)
#         out_file = out_dir / ncfile.name
#         if out_file.exists():
#             skipped += 1
#             continue
#         files_to_regrid.append((ncfile, out_file))

#     if not files_to_regrid:
#         console.print("[yellow]No new files to regrid.[/yellow]")
#         return

#     console.print(f"[blue]Found {len(files_to_regrid)} files to regrid[/blue]")
#     console.print(f"[blue]Skipped {skipped} files (already processed)[/blue]")

#     # Group files by subdirectory
#     subdir_groups = {}
#     for ncfile, out_file in files_to_regrid:
#         # Find the lowest subdirectory that contains .nc files
#         current_dir = ncfile.parent
#         while current_dir != current_dir.parent:
#             if any(current_dir.glob("*.nc*")):
#                 subdir_key = str(current_dir)
#                 break
#             current_dir = current_dir.parent
#         else:
#             subdir_key = str(ncfile.parent)
        
#         if subdir_key not in subdir_groups:
#             subdir_groups[subdir_key] = []
#         subdir_groups[subdir_key].append((ncfile, out_file))

#     console.print(f"[blue]Grouped files into {len(subdir_groups)} subdirectory...[/blue]")

#     success_count = 0
#     fail_count = 0

#     # Process each subdirectory group
#     for subdir_key, subdir_files in subdir_groups.items():
#         console.print(f"[blue]Processing subdirectory:[/blue] {subdir_key}")
#         console.print(f"[blue]Files in subdirectory:[/blue] {len(subdir_files)}")
        
#         # Create weight directory for this subdirectory
#         weight_dir = Path(subdir_key) / "regrid_weights"
#         weight_dir.mkdir(exist_ok=True)
#         console.print(f"[blue]Weight directory:[/blue] {weight_dir}")
        
#         # Process files in this subdirectory
#         for i, (ncfile, out_file) in enumerate(subdir_files):
#             console.print(f"[blue]Processing file {i+1}/{len(subdir_files)}:[/blue] {ncfile.name}")
            
#             try:
#                 console.print(f"  [cyan]Loading dataset...[/cyan]")
#                 # Load dataset
#                 ds = xa.open_dataset(ncfile)
#                 console.print(f"  [cyan]Dataset loaded, shape: {ds.dims}[/cyan]")
                
#                 short_path = str(Path(*ncfile.parts[-6:]))

#                 console.print(f"  [cyan]Creating RegridderManager...[/cyan]")
#                 # Create regridder manager with subdirectory-specific weight directory
#                 regrid_mgr = RegridderManager(
#                     ds=ds, fs=fs, target_res=target_res, periodic=periodic
#                 )
#                 regrid_mgr.weight_dir = weight_dir
#                 regrid_mgr.weight_dir.mkdir(exist_ok=True)
#                 console.print(f"  [cyan]RegridderManager created[/cyan]")

#                 console.print(f"  [cyan]Starting regridding...[/cyan]")
#                 # Use the unified regrid_ds method with weight reuse
#                 regridded_ds = regrid_mgr.regrid_ds(reuse_weights=reuse_weights)
#                 console.print(f"  [cyan]Regridding completed[/cyan]")
                
#                 console.print(f"  [cyan]Processing output...[/cyan]")
#                 regridded_ds = process_xa_d(regridded_ds)
#                 console.print(f"  [cyan]Output processed[/cyan]")

#                 console.print(f"  [cyan]Saving to: {out_file}[/cyan]")
#                 # Save to output file
#                 regridded_ds.to_netcdf(out_file)
#                 console.print(f"  [cyan]File saved[/cyan]")

#                 # Determine regridding method for logging
#                 method_used = "CDO" if "ncells" in ds.dims else "xESMF"
#                 console.print(f"[green]Regridded ({method_used}):[/green] {short_path}")

#                 success_count += 1

#                 # Clean up original file if requested
#                 if delete_original:
#                     ncfile.unlink()
#                     console.print(f"  [cyan]Original file deleted[/cyan]")

#             except Exception as e:
#                 console.print(f"[red]Failed to regrid:[/red] {ncfile.name}")
#                 console.print(f"[red]Error:[/red] {str(e)}")
#                 console.print(f"[red]Error type:[/red] {type(e).__name__}")
#                 import traceback
#                 console.print(f"[red]Traceback:[/red] {traceback.format_exc()}")
#                 fail_count += 1

#     # Print summary
#     console.print("\n[bold blue]Regridding Summary:[/bold blue]")
#     console.print(f"[green]Successful:[/green] {success_count}")
#     console.print(f"[red]Failed:[/red] {fail_count}")
#     console.print(f"[white]Skipped:[/white] {skipped}")
#     console.print(f"[blue]Total processed:[/blue] {success_count + fail_count}")

#     # end timer
#     end_time = time.localtime()
#     console.print(f":clock3: END: {time.strftime('%Y-%m-%d %H:%M:%S', end_time)}\n")
#     duration = time.mktime(end_time) - time.mktime(start_time)
#     mins, secs = divmod(int(duration), 60)
#     hours, mins = divmod(mins, 60)
#     console.print(f":clock3: DURATION: {hours:02d}:{mins:02d}:{secs:02d}\n")
    
# def process_message(self, processing_state: str) -> None:
#     """Display summary of processing settings."""
#     console = Console()
#     # create search table
#     processing_table = Table(
#         title="Processing Criteria",
#         show_header=True,
#         header_style="bold magenta",
#     )
#     processing_table.add_column("Key", style="dim", width=20)
#     processing_table.add_column("Value", style="bold")
#     for k, v in self.meta_criteria.items():
#         if k == "subset":
#             for sk, sv in self.meta_criteria.get("subset", {}).items():
#                 processing_table.add_row(str(sk), str(sv))
#         else:
#             processing_table.add_row(str(k), str(v))
        
#     console.print(
#         Panel(processing_table, title="[cyan]Starting Processing", border_style="cyan")
#     )
#     if processing_state == "post":
#         msg = f"[green]Processing completed.[/green]"  # noqa
#         console.print(
#             Panel(msg, title="[green]Processing Results", border_style="green")
#         )

    
# def main():
#     """Command-line interface for regridding operations."""
#     import argparse

#     parser = argparse.ArgumentParser(
#         description="Regrid NetCDF files in a directory tree",
#         formatter_class=argparse.ArgumentDefaultsHelpFormatter,
#     )

#     # Optional arguments
#     parser.add_argument(
#         "--subdir",
#         default="reprojected",
#         help="Subdirectory name for regridded files",
#     )

#     # Required arguments
#     parser.add_argument(
#         "watch_dir",
#         help="Directory to scan for NetCDF files",
#     )

#     parser.add_argument(
#         "--watch",
#         action="store_true",
#         help="Start in watch mode (asynchronous monitoring)",
#     )

#     parser.add_argument(
#         "--target-res",
#         nargs=2,
#         type=float,
#         default=None,
#         metavar=("LON_RES", "LAT_RES"),
#         help="Target resolution as longitude and latitude degrees",
#     )

#     parser.add_argument(
#         "--max-workers",
#         type=int,
#         default=1,
#         help="Maximum number of parallel workers",
#     )

#     parser.add_argument(
#         "--delete-original",
#         action="store_true",
#         help="Delete original files after successful regridding",
#     )

#     parser.add_argument(
#         "--use-parallel",
#         action="store_true",
#         help="Use process-based parallelization (experimental)",
#     )

#     parser.add_argument(
#         "--by-subdirectory",
#         action="store_true",
#         help="Group files by subdirectory for weight reuse",
#     )

#     parser.add_argument(
#         "--batch-size",
#         type=int,
#         default=5,
#         help="Number of files to process in each batch (watch mode only)",
#     )

#     parser.add_argument(
#         "--batch-timeout",
#         type=int,
#         default=30,
#         help="Timeout in seconds for batch collection (watch mode only)",
#     )
    
#     parser.add_argument(
#         "--process-existing",
#         action="store_true",
#         help="Process existing files in the watch directory",
#     )

#     args = parser.parse_args()

#     if args.watch:
#         # Start in watch mode
#         from .file_watcher import start_async_regridder
#         start_async_regridder(
#             watch_dir=args.watch_dir,
#             target_res=args.target_res,
#             max_workers=args.max_workers,
#             batch_size=args.batch_size,
#             batch_timeout=args.batch_timeout,
#             reuse_weights=True,
#             delete_original=args.delete_original,
#             process_existing=args.process_existing,
#         )
#     elif args.by_subdirectory:
#         # Use subdirectory-based processing
#         regrid_files_by_subdirectory(
#             watch_dir=args.watch_dir,
#             subdir=args.subdir,
#             delete_original=args.delete_original,
#             target_res=args.target_res,
#             max_workers=args.max_workers,
#             reuse_weights=True,
#         )


# if __name__ == "__main__":
#     main()