import sys
sys.path.append("./helpers/")
from flaskexample import app
from flask import render_template
from flask import request
import psycopg2
import pandas as pd


# set default _id and the list of coordinates to display
app.vid = [54, 147]
app.res = []
app.coords = []


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
    location_string = request.args.get('birth_month')
    if location_string:
        location_id = location_string.split(",")
    else:
        location_id = [54, 147]
    query = "SELECT name, address, score FROM {} WHERE latitude_id={} and longitude_id={} ORDER BY score DESC".format(
        'Ranking', location_id[0], location_id[1])
    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data)
    print(df)
    cursor.close()
    conn.close()
    names = []
    for i in range(df.shape[0]):
        names.append(dict(Restaurant=df.iloc[i, 0], Address=df.iloc[i, 1], Score=df.iloc[i, 2]))
    return render_template('output.html', names=names)

