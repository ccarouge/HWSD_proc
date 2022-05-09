import errno
import xarray as xr
import argparse as ap
import datetime as dt
import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

import pathlib

def parse_arguments():
    '''Read in the arguments.'''
    parser = ap.ArgumentParser(description='Process soil composition from HWSD data')
    parser.add_argument('--config', nargs='?',
                    help='path to the config file',
                    default='config.yaml')
    parser.add_argument('--fValue', nargs='?',
                    help='value for FillValue',
                    default='-9999.')

    args = parser.parse_args()

    return args

def read_config(conffile):
    '''Read the input yaml file'''

    try:
        with open(conffile, 'r') as config_file:
            config = yaml.load(config_file, Loader=Loader)
    except IOError as exc:
        if exc.errno == errno.ENOENT:
            print(f'warning: Configuration file {conffile} not found!')
        else:
            raise

    # Get the data path into a Path object:
    config['path'] = pathlib.Path(config['path'])

    return config

def readin(config):
    '''Read the HWSD data for a category for both top soil and subsoil. Concatenate along a new level dimension
    config: dictionary with the configuration info'''

    # paths:
    flist=[config['path']/f'{tt}_{soilv}.nc4' for tt in ['T','S'] for soilv in config['soil_vars']]

    # Create new coordinate
    level=xr.DataArray(['subsoil','top'],dims='level')

    data = xr.open_mfdataset(flist,combine='by_coords')    

    return(data)

if __name__ == "__main__":

# Read in arguments
    args=parse_arguments()

# Read in the config file
    config = read_config(args.config)

    # Need to read both top and subsoil data for all variables. Returns the dataset.
    # hwsd_data has both T_{soilv} and S_{soilv} variables
    hwsd_data=readin(config)

    print(" Load data in memory")
    T_sand = hwsd_data.T_SAND.load()/100.
    T_silt = hwsd_data.T_SILT.load()/100.
    T_clay = hwsd_data.T_CLAY.load()/100.
    T_oc   = hwsd_data.T_OC.load()/100.
    T_bulkden = hwsd_data.T_BULK_DEN.load()*1000.
    S_sand = hwsd_data.S_SAND.load()/100.
    S_silt = hwsd_data.S_SILT.load()/100.
    S_clay = hwsd_data.S_CLAY.load()/100.
    S_oc   = hwsd_data.S_OC.load()/100.
    S_bulkden = hwsd_data.S_BULK_DEN.load()*1000.

    print("Size of 1 array: ",S_oc.nbytes / 1024**3, 'GB')



# Fill in missing pixels for S_sand, S_clay and S_silt

    print("fill missing pixels")
    sand_test = S_sand.where((S_sand+S_silt+S_clay) >= 0.9, other=T_sand)
    clay_test = S_clay.where((sand_test+S_silt+S_clay) >= 0.9, other=T_clay)
    silt_test = S_silt.where((sand_test+S_silt+clay_test) >= 0.9, other=T_silt)

#    S_sand = sand_test.copy()
#    S_clay = clay_test.copy()
#    S_silt = silt_test.copy()

# Get a total column weighted mean
    print("Calculate total column")
    Sand = T_sand*0.3 + S_sand*0.7
    Clay = T_clay*0.3 + S_clay*0.7
    Silt = T_silt*0.3 + S_silt*0.7
    Organic = T_oc*0.3 + S_oc*0.7
    Bulkden = T_bulkden*0.3 + S_bulkden*0.7

# Create a dataset and add metadata

# Set fillvalues to -9999.
    print("Fill NaN values")
    Sand = Sand.fillna(args.fValue)
    Clay = Clay.fillna(args.fValue)
    Silt = Silt.fillna(args.fValue)
    Organic = Organic.fillna(args.fValue)
    Bulkden = Bulkden.fillna(args.fValue)

# Common attributes to all variables are stored in attrs dictionary.
    attrs={
           'units':"-", 
           "comment":"Column of soil between 0 and 100 cm.", 
           "_FillValue":args.fValue
        }

    Sand = Sand.assign_attrs(long_name="soil sand fraction", **attrs)
    Silt= Silt.assign_attrs(long_name= "soil silt fraction", **attrs)
    Clay= Clay.assign_attrs(long_name= "soil clay fraction", **attrs)
    Organic= Organic.assign_attrs(long_name= "soil organic carbon", **attrs)
    Bulkden= Bulkden.assign_attrs(long_name= "soil bulk density", **attrs)
    Bulkden= Bulkden.assign_attrs(units="kg m-3")

    print("Create dataset")
    ds = xr.Dataset({"SAND":Sand, "CLAY":Clay, "SILT":Silt, "OC":Organic, "BULK_DEN":Bulkden})

    # Modify the original file attributes to keep provenance.
    new_attrs = {**hwsd_data.attrs}

    # Keep the old attributes we want to keep
    new_attrs["original_creator"] = new_attrs["creator"]
    new_attrs["original_institution"] = new_attrs["institution"]
    new_attrs["original_processing"] = "The source data was created with this processing.\n " + new_attrs["processing"]

    cur_time = dt.datetime.now(dt.timezone.utc).strftime("%a %d %b %Y %H:%M:%S %Z")
    new_attrs["modified"] = f"Claire Carouge, {cur_time}"
    new_attrs["institution"] = "CLEX, UNSW"
    new_attrs["creator"] = "Claire Carouge"
    new_attrs["creator_email"] = "c.carouge@unsw.edu.au"
    new_attrs["source"] = "3x3minute regridded HWSD"
    new_attrs["title"] = "Whole soil column 3x3minute regridded HWSD"
    new_attrs["processing"] = "Weighted average of the top soil and subsoil data by the height of the top soil column and subsoil soil column.\n "\
        "All parameters are output in one netcdf file.\n"
    new_attrs["history"] = "Weighted average with HWSD_total_column.py\n " + new_attrs["history"]
    ds = ds.assign_attrs(**new_attrs)

    encod = {'dtype':'float64','zlib':True, 'complevel':5}
    print("Write to file")
    all_encod={var:encod for var in ds.data_vars }#for var,_ in darray.items() }
    ds.to_netcdf(path=config["path"]/f"HWSD_soilcomposition_test.nc",encoding=all_encod)
