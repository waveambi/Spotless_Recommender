import sys

sys.path.append("./helpers/")
import json
import pyspark
import helper
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
                           .map(lambda x: json.loads(x[1]))
                           .map(helper.add_block_fields)
                           .filter(lambda x: x is not None)
                           .map(lambda x: ((x["latitude_id"], x["longitude_id"]),
                                           (x["latitude"], x["longitude"], x["user_id"]))))
        print("process_stream success")

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

    def load_batch_data(self):
        """
        reads result of batch transformation from PostgreSQL database, splits it into BATCH_PARTS parts
        """
        config = {key: self.psql_config[key] for key in ["url", "driver", "user", "password", "dbtable_batch", "dbtable_cf"]}
        self.df_ranking_result = self.spark.read \
            .format("jdbc") \
            .option("url", config["url"]) \
            .option("driver", config["driver"]) \
            .option("dbtable", config['dbtable_batch']) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .load()
        '''
        self.df_cf = self.spark.read \
            .format("jdbc") \
            .option("url", config["url"]) \
            .option("driver", config["driver"]) \
            .option("dbtable", config['dbtable_cf']) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .load()
        '''
        # print("loaded batch with {} rows".format(self.df_ranking_result.count()))
        self.df_ranking_result = self.df_ranking_result.select("business_id", "name", "address", "latitude_id", "longitude_id", "score")
        self.df_ranking_result = (self.df_ranking_result.rdd.repartition(self.stream_config["PARTITIONS"])
                           .map(lambda x: x.asDict())
                           .map(lambda x: ((x["latitude_id"], x["longitude_id"]),
                                           (x["business_id"], x["name"], x["address"], x["score"]))))
        #self.df_cf = self.df_cf.rdd.repartition(self.stream_config["PARTITIONS"])
        self.df_ranking_result.persist(pyspark.StorageLevel.MEMORY_ONLY_2)
        print("load batch data successfully")


    def process_each_rdd(self, time, rdd):
        """
        for every record in rdd, queries database historic_data for the answer
        :type rdd:  RDD          Spark RDD from the stream
        """
        global iPass
        try:
            iPass += 1
        except:
            iPass = 1

        print("========= RDD Batch Number: {0} - {1} =========".format(iPass, str(time)))
        # transform rdd and broadcast to workers
        # rdd_bcast has the following schema
        # rdd_bcast = {key: [list of value]}
        # key = (time_slot, block_latid, block_lonid)
        # value = (vehicle_id, longitude, latitude, datetime)
        #rdd_bcast = (rdd.groupByKey().mapValues(lambda x: sorted(x, key=lambda el: el[0])).collect())
        # join the batch dataset with rdd_bcast, filter None values,
        # and from all the spot suggestions select specific for the driver to ensure no competition
        self.resDF = rdd.join(self.df_ranking_result)
        #.reduceByKey(lambda x,y: x+y)
        print("None")
        if self.resDF.isEmpty():
            return
        print self.resDF.take(5)
        print "None"
        # save data
        config = {key: self.psql_config[key] for key in
                  ["url", "driver", "user", "password", "mode_streaming", "dbtable_streaming", "nums_partition"]}
        self.resDF.toDF().write \
            .format("jdbc") \
            .option("url", config["url"]) \
            .option("driver", config["driver"]) \
            .option("dbtable", config["dbtable_streaming"]) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .mode(config["mode_streaming"]) \
            .option("numPartitions", config["nums_partition"]) \
            .save()


    def process_stream(self):
        """
        processes each RDD in the stream
        """
        SparkStreamerFromKafka.process_stream(self)
        process = self.process_each_rdd
        self.dataStream.foreachRDD(process)
