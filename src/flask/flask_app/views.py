import sys
sys.path.append("./helpers/")
from flaskexample import app
from flask import render_template
from flask import request
import psycopg2
import pandas as pd
import math

# read Google Maps API Key from file
#with open("./config/GoogleAPIKey.config") as f:
#    app.APIkey = f.readline().strip()


@app.route('/')
@app.route('/output')
def cesareans_output():
    """
    gets the data from PostgreSQL database using the specified query
    :type query: str
    :rtype     : list of records from database
    """
    user = 'myinsightusername'
    password = 'myinsightpassword'
    host = 'myinsightinstance.cxp3kjm75iyc.us-east-1.rds.amazonaws.com'
    port = '5432'
    dbname = 'mydatabase'
    conn = psycopg2.connect(database=dbname, user=user, password=password, host=host, port=port, connect_timeout=10)
    cursor = conn.cursor()
    location_string = request.args.get('user_location')
    location = [36.12, -115.28]
    location_id = [45, 123]
    if location_string:
        s = location_string.split(",")
        location = [float(s[0]), float(s[1])]
        location_id[0] = int(math.floor((location[0] - 35.98) / 0.003))
        location_id[1] = int(math.floor((location[1] + 115.65) / 0.003))
    query = "SELECT name, address, score, latitude, longitude FROM {} WHERE latitude_id={} and longitude_id={} ORDER BY score DESC Limit 5".format(
        'Ranking', location_id[0], location_id[1])
    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data)
    print(df)
    cursor.close()
    conn.close()
    names = []
    for i in range(df.shape[0]):
        names.append(dict(Restaurant=df.iloc[i, 0], Address=df.iloc[i, 1], Score=df.iloc[i, 2], Latitude=df.iloc[i, 3], Longitude=df.iloc[i, 4]))
    return render_template('output.html', names=names, location=location)

