import psycopg2

def save_to_postgresql(sql_data, config):
    """
    saves Spark DataFrame to PostgreSQL database with given configurations
    :type sql_data  : Spark DataFrame   DataFrame to save
    :type config    : dict              dictionary with PostgreSQL configurations
    :type savemode  : str               "overwrite", "append"
    """
    try:
        conn = psycopg2.connect(**config)
    except:
        error = "Unable to connect to the database."
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute = """create table DF if not exists
        (id varchar(20));"""

    except:
        error = "Can't get data from database!"
    conn.close()
    return eval(error)




def read_from_postgresql(sqlContext, config):
    """
    reads from PostgreSQL database with given configurations into Spark DataFrame
    :type sqlContext: SQLContext        Spark SQL Context for saving
    :type config    : dict              dictionary with PostgreSQL configurations
    :rtype          : Spark DataFrame   SQL DataFrame representing the table
    """
    options = "".join([".options(%s=config[\"%s\"])" % (opt, opt) for opt in config.keys()])
    command = "sqlContext.read.format(\"jdbc\")%s.load()" % options
    return eval(command)



def add_index_postgresql(dbtable, column, config):
    """
    adds index to PostgreSQL table dbtable on column
    :type dbtable: str      name of the table
    :type column : str      name of the column
    :type config : dict     dictionary with PostgreSQL configurations
    """
    conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % (config["host"],
                                                                     config["dbname"],
                                                                     config["user"],
                                                                     config["password"])
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX ON %s (%s)" % (dbtable, column))
    conn.commit()
    cursor.close()
    conn.close()
