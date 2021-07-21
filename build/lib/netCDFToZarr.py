import zarr
import os
import logging
from netCDF4 import Dataset
import numpy as np
import math
import threading
import s3fs

logging.getLogger().setLevel(logging.INFO)


class NetCDFToZarr:
    def __init__(self) -> None:
        pass
    def __get_zarr_directory_store(self,store_name, s3=False, **kwargs):
        if s3:
            region = kwargs.get('region', 'us-west-2')
            endpoint_url = kwargs.get('endpoint_url')
            bucket_name = kwargs.get('bucket_name')
            s3 = s3fs.S3FileSystem(anon=False, client_kwargs=dict(region_name=region, endpoint_url=endpoint_url))
            store = s3fs.S3Map(root=f"{bucket_name}/{store_name}", s3=s3, check=False)
            cache = zarr.LRUStoreCache(store, max_size=2**28)
            return zarr.group(store=cache, overwrite = True)
        store = zarr.DirectoryStore(store_name)
        return zarr.group(store=store, overwrite = True)  

    def netcdf_to_zarr(self, netcdf_path, store = None, s3=False, **kwargs):
        store_name = store or f"{os.path.basename(netcdf_path)}.zarr"
        root = self.__get_zarr_directory_store(store_name, s3=s3, **kwargs)
        ds = Dataset(netcdf_path)
        self.__set_meta(ds,root)
        self.__set_dims(ds,root)
        self.__set_vars(ds,root)


    def __set_meta(self, ds, group):
        logging.info("Set meta")
        group.attrs.put(self.__dsattrs(ds))
    
    # Return attributes as dict
    def __dsattrs(self,dataset):
        # JSON encode attributes so they can be serialized
        return {key: self.__json_encode(getattr(dataset, key)) for key in dataset.ncattrs() }

    # Convert non-json-encodable types to built-in types
    @staticmethod
    def __json_encode(val):

        if isinstance(val, np.integer):
            return int(val)
        elif isinstance(val, np.floating):
            return float(val)
        elif isinstance(val, np.ndarray):
            return val.tolist()
        else:
            return val

    # Set dimensions

    # Set dimensions
    def __set_dims(self, dataset, group):
        logging.info("Setting dimensions")
        threads = []
        for name, _ in dataset.dimensions.items():
            t = threading.Thread(target=self.__set_dim, args=(dataset, group, name,))
            threads.append(t)
            t.start()
        [t.join() for t in threads]
        threads = []
    


    @staticmethod
    def __set_dim(ds, group, name):
        logging.info(f"Set dim {name}")
        var = ds.variables[name]
        dim = ds.dimensions[name]
        group.require_dataset(name, \
            data=np.arange(dim.size), \
            shape=(dim.size,), \
            chunks=(1<<16,) if dim.isunlimited() else (dim.size,), \
            dtype=getattr(np, str(var.dtype)) \
    )
        # Set dimension attrs
        group[name].attrs['_ARRAY_DIMENSIONS'] = [name]
    
    def __set_vars(self, ds, group):
        logging.info("Set vars")
        threads = []
       
        for name, _ in ds.variables.items():
            t =  threading.Thread(target=self.__set_var, args=(ds, group, name,))
            threads.append(t)
            t.start()
        [t.join()for t in threads]
        threads = []


    # Set variable data, including dimensions and metadata

    def __set_var(self, ds, group, name):
        logging.info("Setting " + name)
        var = ds.variables[name]

        try:
            group.require_dataset(name, \
            data=var, \
            shape=var.shape, \
            chunks=(self.__get_var_chunks(var, 2<<24)), \
            dtype= getattr(np, str(var.dtype)) \
    )
        except Exception as ex:
            print(f"Can't process {name} because {var.dtype}: {ex}")
        attrs = self.__dsattrs(var)
        attrs['_ARRAY_DIMENSIONS'] = list(var.dimensions)
        group[name].attrs.put(attrs)


    # Calculate chunk size for variable
    @staticmethod
    def __get_var_chunks(var, max_size):
        chunks = []
        # TODO: Improve chunk size calculation
        for i, dim in enumerate(var.shape):
            dim_chunk_length = min(math.floor(max_size ** (1/(len(var.shape)-i))), dim)
            max_size //= dim_chunk_length
            chunks.append(dim_chunk_length)


if __name__ == "__main__":
    netcdf_to_zarr = NetCDFToZarr()
    netcdf_to_zarr.netcdf_to_zarr("/home/amarouane/Downloads/f13_ssmi_20091102v7.nc", s3=True, endpoint_url="http://localhost:4566", bucket_name="amarouane/yuey")