
import sys
sys.path.append("./helpers/")
import json
import pyspark
import helper
import postgre
import numpy as np
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

class SparkStreamerFromKafka:
    """
    class that streams messages from Kafka topic and cleans up the message content
    """

    def __init__(self, kafka_configfile, stream_configfile, psql_configfile):
        """
        class constructor that initializes the instance according to the configurations
        of Kafka (brokers, topic, offsets), data schema and batch interval for streaming
        :type kafka_configfile:  str        path to s3 config file
        :type schema_configfile: str        path to schema config file
        :type stream_configfile: str        path to stream config file
        """
        self.kafka_config  = helper.parse_config(kafka_configfile)
        self.stream_config = helper.parse_config(stream_configfile)
        self.psql_config = helper.parse_config(psql_configfile)
        self.conf = SparkConf()
        self.sc  = SparkContext(conf=self.conf).getOrCreate()
        self.spark = SparkSession.builder.config(conf=self.conf).getOrCreate()
        self.ssc = StreamingContext(self.sc, self.stream_config["INTERVAL"])
        self.sc.setLogLevel("ERROR")

    def initialize_stream(self):
        """
        initializes stream from Kafka topic
        """
        topic, n = self.kafka_config["TOPIC"], self.kafka_config["PARTITIONS"]
        self.dataStream = KafkaUtils.createDirectStream(self.ssc, [topic],
                                                {"metadata.broker.list": self.kafka_config["BROKERS_IP"]})


    def process_stream(self):
        """
        cleans the streamed data
        """
        self.initialize_stream()
        partitions = self.stream_config["PARTITIONS"]
        self.dataStream = (self.dataStream
                                    .repartition(partitions)
                                    .map(lambda x: json.loads(x))
                                    .map(helper.add_block_fields)
                                    .filter(lambda x: x is not None)
                                    .map(lambda x: ((x["latitude_id"], x["longitude_id"]),
                                                    (x["latitude"], x["longitude"], x["datetime"]))))


    def run(self):
        """
        starts streaming
        """
        self.process_stream()
        self.ssc.start()
        self.ssc.awaitTermination()


####################################################################

class Streamer(SparkStreamerFromKafka):
    """
    class that provides each taxi driver with the top-n pickup spots
    """

    def __init__(self, kafka_configfile, stream_configfile, psql_configfile):
        """
        class constructor that initializes the instance according to the configurations
        of Kafka (brokers, topic, offsets), PostgreSQL database, data schema and batch interval for streaming
        :type kafka_configfile:  str        path to s3 config file
        :type stream_configfile: str        path to stream config file
        :type psql_configfile:   str        path to psql config file
        :type start_offset:      int        offset from which to read from partitions of Kafka topic
        """
        SparkStreamerFromKafka.__init__(self, kafka_configfile, stream_configfile, psql_configfile)
        self.spark = SparkSession.builder.config(conf=self.conf).getOrCreate()
        self.load_batch_data()
        self.psql_n = 0


    def load_batch_data(self):
        """
        reads result of batch transformation from PostgreSQL database, splits it into BATCH_PARTS parts
        """
        config = {key: self.psql_config[key] for key in ["url", "driver", "user", "password"]}
        config["query"] = "(SELECT * FROM Rankings) as df_batch"
        self.df_batch = self.spark.load \
                        .format("jdbc") \
                        .option("url", config["url"]) \
                        .option("driver", config["driver"]) \
                        .option("dbtable", config['query'] ) \
                        .option("user", config["user"]) \
                        .option("password", config["password"]) \
        #print("loaded batch with {} rows".format(self.df_batch.count()))

'''
    def process_each_rdd(self, time, rdd):
        """
        for every record in rdd, queries database historic_data for the answer
        :type time: datetime     timestamp for each RDD batch
        :type rdd:  RDD          Spark RDD from the stream
        """

        def my_join(x):
            """
            joins the record from table with historical data with the records of the taxi drivers' locations
            on the key (time_slot, block_latid, block_lonid)
            schema for x: ((time_slot, block_latid, block_lonid), (longitude, latitude, passengers))
            schema for el: (vehicle_id, longitude, latitude, datetime)
            :type x: tuple( tuple(int, int, int), tuple(float, float, int) )
            """
            try:
                return map(lambda el: (  el[0],
                                        (el[1], el[2]),
                                     zip( x[1][0],  x[1][1]),
                                        x[1][2],
                                        el[3]  ), rdd_bcast.value[x[0]])
            except:
                return [None]

        def select_customized_spots(x):
            """
            chooses no more than 3 pickup spots from top-n,
            based on the total number of rides from that spot
            and on the order in which the drivers send their location data
            schema for x: (vehicle_id, (longitude, latitude), [list of spots (lon, lat)], [list of passenger pickups], datetime)
            :type x: tuple( str, tuple(float, float), list[tuple(float, float)], tuple(int, list[int]), str )
            """
            try:
                length, total = len(x[3]), sum(x[3])
                np.random.seed(4040 + int(x[0]))
                choices = np.random.choice(length, min(3, length), p=np.array(x[3])/float(total), replace=False)
                return {"vehicle_id": x[0], "vehicle_pos": list(x[1]),
                        "spot_lon": [x[2][c][0] for c in choices],
                        "spot_lat": [x[2][c][1] for c in choices],
                        "datetime": x[4]}
            except:
                return {"vehicle_id": x[0], "vehicle_pos": list(x[1]),
                        "spot_lon": [], "spot_lat": [], "datetime": x[4]}


        global iPass
        try:
            iPass += 1
        except:
            iPass = 1

        print("========= RDD Batch Number: {0} - {1} =========".format(iPass, str(time)))

        try:
            parts, total = self.parts, self.total

            # calculate list of distinct time_slots in current RDD batch
            tsl_list = rdd.map(lambda x: x[0][0]*parts/total).distinct().collect()

            # transform rdd and broadcast to workers
            # rdd_bcast has the following schema
            # rdd_bcast = {key: [list of value]}
            # key = (time_slot, block_latid, block_lonid)
            # value = (vehicle_id, longitude, latitude, datetime)
            rdd_bcast = (rdd.groupByKey()
                            .mapValues(lambda x: sorted(x, key=lambda el: el[3]))
                            .collect())
            if len(rdd_bcast) == 0:
                return

            rdd_bcast = self.sc.broadcast({x[0]:x[1] for x in rdd_bcast})

            # join the batch dataset with rdd_bcast, filter None values,
            # and from all the spot suggestions select specific for the driver to ensure no competition
            resDF = self.sc.union([(self.hdata[tsl]
                                          .flatMap(my_join, preservesPartitioning=True)
                                          .filter(lambda x: x is not None)
                                          .map(select_customized_spots)) for tsl in tsl_list])

            # save data
            self.psql_n += 1
            configs = {key: self.psql_config[key] for key in ["url", "driver", "user", "password"]}
            configs["dbtable"] = self.psql_config["dbtable_stream"]

            postgres.save_to_postgresql(resDF, self.sqlContext, configs, self.stream_config["mode_stream"])
            if self.psql_n == 1:
                postgres.add_index_postgresql(configs["dbtable"], "vehicle_id", self.psql_config)

        except:
            pass


    def process_stream(self):
        """
        processes each RDD in the stream
        """
        SparkStreamerFromKafka.process_stream(self)

        process = self.process_each_rdd
        self.dataStream.foreachRDD(process)
'''