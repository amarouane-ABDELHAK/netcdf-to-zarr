# NetCDF to Zarr

This repository contains code for converting NetCDF files to Zarr stores. Basic examples follow below.

## Examples

### List of Files

```python
from netcdf_to_zarr import NetCDFToZarr
netcdf_to_zarr = NetCDFToZarr()
netcdf_to_zarr.netcdf_to_zarr("<path_to_necdf_file>")
```

### List of Files in S3

```python
import zarr
from netCDF4 import Dataset

from convert import netcdf_to_zarr
from iterators import S3BasicIterator

# Iterate through S3 objects without downloading all at once
ds_iterator = iter(S3BasicIterator('bucket_name', ['key_1', 'key_2'], 'path_to_download_folder'))
store = zarr.DirectoryStore('store.zarr')
netcdf_to_zarr(ds_iterator, store, 'Time')
```
