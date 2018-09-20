import math
import json
import subprocess


def parse_config(configfile):
    """
    reads configs saved as json record in configuration file and returns them
    :type configfile: str       path to config file
    :rtype          : dict      configs
    """
    conf = json.load(open(configfile, "r"))
    return replace_envvars_with_vals(conf)


def replace_envvars_with_vals(dic):
    """
    for a dictionary dic which may contain values of the form "$varname",
    replaces such values with the values of corresponding environmental variables
    :type dic: dict     dictionary where to parse environmental variables
    :rtype   : dict     dictionary with parsed environmental variables
    """
    for el in dic.keys():
        val = dic[el]
        if type(val) is dict:
            val = replace_envvars_with_vals(val)
        else:
            if type(val) in [unicode, str] and len(val) > 0 and '$' in val:
                command = "echo {}".format(val)
                dic[el] = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout.read().strip()
    return dic


# lat min = 35.98  max = 36.31 log  min -115.65 max  -115.04
def determine_block_lat_ids(lat):
    """
    calculates ids of blocks based on given coordinates
    :type lon: float            longitude
    :type lat: float            latitude
    :rtype : (int, int)       tuples which contain x and y ids
    """
    # size of large block is 0.005  degree lat/lon, about 350 meters
    corner = lat - 35.98
    block_id_lat = int(math.floor(corner/0.005))
    return block_id_lat


def determine_block_log_ids(log):
    """
    calculates ids of blocks based on given coordinates
    :type lon: float            longitude
    :type lat: float            latitude
    :rtype : (int, int)       tuples which contain x and y ids
    """
    # size of large block is 0.005  degree lat/lon, about 350 meters
    corner = log + 115.65
    block_id_log = int(math.floor(corner/0.005))
    return block_id_log

def add_block_fields(record):
    """
    adds fields block_id ((int, int)), sub_block_id ((int, int)), block_latid (int), block_lonid (int)
    to the record based on existing fields longitude and latitude
    returns None if unable to add fields
    :type record: dict      record into which insert new fields
    :rtype      : dict      record with inserted new fields
    """
    try:
        log, lat = [record[field] for field in ["longitude", "latitude"]]
        record["longtitude_id"] = determine_block_lat_ids(log)
        record["latitude_id"] = determine_block_lat_ids(lat)
    except:
        return
    return dict(record)












