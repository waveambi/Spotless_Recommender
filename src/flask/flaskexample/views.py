from flaskexample import app
from flask import render_template, jsonify, request
from sqlalchemy import create_engine
import psycopg2
import pandas as pd

user = 'myinsightusername'
password = 'myinsightpassword'
host = 'localhost'
dbname = 'mydatabase'
db = create_engine('postgres://%s/%s'%(host,dbname))
conn_string = "host=%s dbname=%' user=%s password=%s" % (host, dbname, user, password)


import sys
sys.path.append("/home/ubuntu/Insight_Recommendation/helpers/")

from math import floor
from more_itertools import peekable


# configure connection string for PostgreSQL database
user = 'myinsightusername'
password = 'myinsightpassword'
host = 'localhost'
dbname = 'mydatabase'

app.conn_str = "host={} dbname={} user={} password={}".format(host, dbname, user, dbname)

# set default _id and the list of coordinates to display
app.vid = []
app.res = []
app.coords = []


# read Google Maps API Key from file
with open("/home/ubuntu/TaxiOptimizer/config/GoogleAPIKey.config") as f:
    app.APIkey = f.readline().strip()


def fetch_from_postgres(query):
    """
    gets the data from PostgreSQL database using the specified query
    :type query: str
    :rtype     : list of records from database
    """
    conn = psycopg2.connect(app.conn_str)
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


def get_spots(vid):
    """
    yields next pickup spots for taxi with given vid
    :type vid: str
    :rtype   : generator
    """
    while True:
        query = "SELECT user_id, business_id, address, score FROM %s WHERE user_id='%s' ORDER BY score" % (app.dbconfig["dbtable_streaming"], vid)
        for entry in fetch_from_postgres(query):
            entr = list(entry)
            yield entr

def get_next(coords, res=None):
    """
    from list of spots generators coords and list of previous spots,
    produces a list of current spots
    :type coords: list[generator]       list of peekable generators
    :type res   : list[spot]            spot schema is defined by query in get_spots(vid)
    :rtype      : list[spot]
    """
    if res == None:
        res = [c.next() for c in coords]
        if len(coords) == 1 or app.curtime >= 1320:
            app.curtime = 590
        return res

    for i, c in enumerate(coords):
        if res[i][-1] > app.curtime:
            continue
        while res[i][-1] < app.curtime:
            if c.peek()[-1] >= res[i][-1]:
                res[i] = c.peek()
                c.next()
            else:
                break
    return res


# get the set of valid taxi vehicle_ids
app.allowed_taxis = fetch_from_postgres("SELECT DISTINCT vehicle_id FROM %s" % app.dbconfig["dbtable_stream"])
app.allowed_taxis = [x[0] for x in app.allowed_taxis]



# define the behavior when accessing routes '/', '/index', '/demo', '/track' and '/query'

@app.route('/')
@app.route('/index')
def index():
    app.curtime = 600
    app.vid, app.coords = [], []
    return render_template('index.html', APIkey=app.APIkey)


@app.route("/query")
def query():
    app.curtime += 0.5

    if app.curtime > 1320:
        app.coords = [peekable(get_spots(vid)) for vid in app.vid]
        app.res = get_next(app.coords)
    else:
        app.res = get_next(app.coords, app.res)

    return jsonify(vid=app.vid,
                   timestr=print_time(app.curtime),
                   taxiloc=[{"lat": rs[3][1], "lng": rs[3][0]} for rs in app.res],
                   corners=[(floor(rs[3][1]*200)/200.0, floor(rs[3][0]*200)/200.0) for rs in app.res],
                   spots=[[{"lat": el[0], "lng": el[1]} for el in zip(rs[0], rs[1])] for rs in app.res])