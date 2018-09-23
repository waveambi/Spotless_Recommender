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
        self.kafka_config = helper.parse_config(kafka_configfile)
        self.stream_config = helper.parse_config(stream_configfile)
        self.psql_config = helper.parse_config(psql_configfile)
        self.conf = SparkConf()
        self.sc = SparkContext(conf=self.conf).getOrCreate()
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
        self.load_batch_data()
        self.psql_n = 0

    def load_batch_data(self):
        """
        reads result of batch transformation from PostgreSQL database, splits it into BATCH_PARTS parts
        """
        config = {key: self.psql_config[key] for key in ["url", "driver", "user", "password", "dbtable_batch"]}
        config["query"] = "(SELECT * FROM Ranking) as df_batch"
        self.df_batch = self.spark.read \
            .format("jdbc") \
            .option("url", config["url"]) \
            .option("driver", config["driver"]) \
            .option("dbtable", config['dbtable_batch']) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .load()
        # print("loaded batch with {} rows".format(self.df_batch.count()))
        self.df_batch.persist(pyspark.StorageLevel.MEMORY_ONLY_2)

    def process_each_rdd(self, time, rdd):
        """
        for every record in rdd, queries database historic_data for the answer
        :type rdd:  RDD          Spark RDD from the stream
        """

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
                choices = np.random.choice(length, min(3, length), p=np.array(x[3]) / float(total), replace=False)
                return {"vehicle_id": x[0], "vehicle_pos": list(x[1]),
                        "spot_lon": [x[2][c][0] for c in choices],
                        "spot_lat": [x[2][c][1] for c in choices],
                        "datetime": x[4]}
            except:
                return {"vehicle_id": x[0], "vehicle_pos": list(x[1]),
                        "spot_lon": [], "spot_lat": [], "datetime": x[4]}

        try:
            df_streaming = self.spark.createDataFrame(rdd)
            df_streaming.createOrReplaceTempView("df_streaming_view")
            # join the batch dataset with rdd_bcast, filter None values,
            # and from all the spot suggestions select specific for the driver to ensure no competition
            self.reDF = self.spark.sql(
                "select user_id, restaurant_id from df_streaming_view inner join df_batch on df_streaming_view.latitude_id == df_batch.latitude.id and df_streaming_view.longitude_id == df_batch.longitude_id  ")

            # save data
            configs = {key: self.psql_config[key] for key in ["url", "driver", "user", "password"]}
            configs["dbtable"] = self.psql_config["dbtable_stream"]



        except:
            pass

    def process_stream(self):
        """
        processes each RDD in the stream
        """
        SparkStreamerFromKafka.process_stream(self)
        process = self.process_each_rdd
        self.dataStream.foreachRDD(process)
