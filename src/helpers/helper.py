import math
import json
import re
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


def trim_zipcode(raw_code):
    """
    trim zip into 5-digits US standard zipcode
    :type raw_code        : str      raw zipcode string
    :rtype trimmed_code   : str      trimmed zipcode with 5 digits
    """
    if raw_code is not None:
        if len(raw_code) == 5:
            return raw_code
        elif len(raw_code) == 10:
            trimmed_code = raw_code[0:5]
            return trimmed_code


def format_address(address):
    """
    format address and remove unneccessary apt number
    :type address : str      raw address string
    :rtype s      : str      formatted address only with street and number
    """
    if address is not None:
        s = address.split(',')[0]
        s = re.sub(r"\.", "", s)
        return s.lower()


def format_name(name):
    """
    clean restaurant name and remove common parts
    :type name    : str      restaurant name
    :rtype s      : str      cleaned restaurant name with unique name
    """
    if name is not None:
        s = name.lower()
        s = s.split("@")[0].strip(" ").strip(",")
        s = s.split("at")[0].strip(" ").strip(",")
        s = s.split("-")[-1].strip(" ").strip(",")
        s = re.sub(r"\s#[\S]+", "", s)
        s = re.sub(r"\s[0-9]+", "", s)
        s = re.sub(r"\scompany", "", s)
        s = re.sub(r"\srestaurant", "", s)
        s = re.sub(r"\skitchen", "", s)
        s = re.sub(r"\sbar", "", s)
        s = re.sub(r"\sclub", "", s)
        s = re.sub(r"\spub", "", s)
        s = re.sub(r"\shotel", "", s)
        s = re.sub(r"\sgrill", "", s)
        s = re.sub(r"\sbbq", "", s)
        s = re.sub(r"\sfood", "", s)
        s = re.sub(r"\sshop", "", s)
        s = re.sub(r"\sstore", "", s)
        s = re.sub(r"\scafe", "", s)
        s = re.sub(r"\scoffee", "", s)
        s = re.sub(r"\splaza", "", s)
        s = re.sub(r"\scenter", "", s)
        s = re.sub(r"\slas vegas", "", s)
        s = re.sub(r"\sinc", "", s)
        s = re.sub(r"\sthe", "", s)
        s = s.lower().strip(" ")
        return s


def fuzzy_match(s1, s2):
    """
    match business with fuzzy name
    :type s1      : str  name string 1
    :type s2      : str  name string 2
    :rtype ratio  : int  similarity ratio
    """
    from fuzzywuzzy import fuzz
    if s1 is not None and s2 is not None:
        ratio = fuzz.ratio(s1, s2)
        return ratio

def convert_sentiment(s):
    """
    convert sentiment from positive/negative to numerical value 1/-1
    :type s       : str  polarity string
    :rtype score  : int  polarity numerical value
    """
    if s == "positive":
        score = 1
    elif s == "negative":
        score = -1
    elif s == "neutral":
        score = 0
    else:
        score = None
    return score


def calculate_score(x, y, z):
    """
    calculate final score combining with ratings, sentiment score and sanitary score
    :type x       : float  average sentiment score
    :type y       : float  yelp rating
    :type z       : int  sanitary score
    :rtype score  : float final score
    """
    if x is not None and y is not None and z is not None:
        score = (x + y*0.2 + 1/(z+1)) / 3
        return score




def determine_block_lat_ids(lat):
    """
    calculates ids of blocks based on given coordinates,
    size of block is 0.005 around 350 meters
    :type lat           : float  latitude
    :rtype block_id_lat : int  latitude id
    """
    corner = float(lat) - 35.98 # lat range from 35.98 to 36.31
    block_id_lat = int(math.floor(corner / 0.005))
    return block_id_lat


def determine_block_log_ids(log):
    """
    calculates ids of blocks based on given coordinates,
    size of block is 0.005 around 350 meters
    :type log           : float  logitude
    :rtype block_id_log : int  logitude id
    """
    corner = float(log) + 115.65 #log range from -115.65 to -115.04
    block_id_log = int(math.floor(corner / 0.005))
    return block_id_log


def add_block_fields(record):
    """
    adds fields block_id with lat_id and log_id
    to the record based on existing fields longitude and latitude
    returns None if unable to add fields
    :type record: dict      record into which insert new fields
    :rtype      : dict      record with inserted new fields
    """
    try:
        lat, log = [record[field] for field in ["latitude", "longitude"]]
        record["longitude_id"] = determine_block_log_ids(log)
        record["latitude_id"] = determine_block_lat_ids(lat)
    except:
        return
    return dict(record)
