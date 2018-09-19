import sys
sys.path.append("./helpers/")
import json
import helper
import postgre
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
class BatchProcessor:
	"""
	class that reads data from S3 bucket, prcoesses it with Spark
	and saves the results into PostgreSQL database
	"""
	def __init__(self, s3_configfile, psql_configfile):
		"""
		class constructor that initializes the instance according to the configurations of the S3 bucket, raw data and PostgreSQL table
		:type s3_configfile:     str  path to S3 config file
		:type psql_configfile:   str  path tp psql config file
		"""
		self.s3_config   = helper.parse_config(s3_configfile)
		self.psql_config = helper.parse_config(psql_configfile)
		self.conf = SparkConf()
		self.sc = SparkContext(conf=self.conf)
		self.sc.setLogLevel("ERROR")
		self.spark = SparkSession.builder.config(conf=self.conf).getOrCreate().sql("set SparkSession.sql.caseSensitive=true")

	def read_from_s3(self):
		"""
		reads files from s3 bucket defined by s3_configfile and creates Spark DataFrame
		"""
		yelp_business_filename = "s3a://{}/{}/{}".format(self.s3_config["BUCKET1"], self.s3_config["FOLDER2"], self.s3_config["RAW_DATA_FILE1"])
		yelp_rating_filename = "s3a://{}/{}/{}".format(self.s3_config["BUCKET2"], self.s3_config["FOLDER2"], self.s3_config["RAW_DATA_FILE2"])
		sanitory_inspection_filename = "s3a://{}/{}/{}".format(self.s3_config["BUCKET3"], self.s3_config["FOLDER3"], self.s3_config["RAW_DATA_FILE3"])
		self.df_yelp_business = self.spark.read.json(yelp_business_filename)
		self.df_yelp_rating = self.spark.read.json(yelp_rating_filename)
		self.df_sanitory_inspection = self.spark.read.csv(sanitory_inspection_filename, header=True)


	def spark_transform(self):
		"""
		transforms Spark DataFrame with raw data into cleaned data;
		adds information
		"""
		self.df_yelp_rating = self.df_yelp_rating.select("review_id", "user_id", "business_id", "stars", "text").withColumnRenamed("stars", "ratings")
		self.df_yelp_business = self.df_yelp_business.filter(self.df_yelp_business.city == "Las Vegas").select("business_id", "name", "address", "city", "postal_code", "latitude", "longitude", "state", "stars", "review_count")
		self.df = self.df_yelp_business.join(self.df_sanitory_inspection, (self.df_yelp_business.address == self.df_sanitory_inspection.Address) & (self.df_yelp_business.name == self.df_sanitory_inspection.Restaurant_Name), 'inner')
		self.df = self.df.select("business_id", "name", "address", "latitude", "longitude", "stars", "Categoriy_Name", "Current_Demerits") #.join(self.df_yelp_rating, "business_id", 'inner')


	def spark_create_block(self):
		self.determine_block_lat_ids_udf = udf(lambda z: helper.determine_block_lat_ids(z), IntegerType())
		self.determine_block_log_ids_udf = udf(lambda z: helper.determine_block_log_ids(z), IntegerType())
		self.df = self.df.withColumn("latitude_id", self.determine_block_lat_ids_udf("latitude"))
		self.df = self.df.withColumn("longitude_id", self.determine_block_log_ids_udf("longitude"))

	def spark_ranking(self):
		"""
        calculates restaurant recommendation and ranks with Spark DataFrame
        """
		# KNN and MF



	def save_to_postgresql(self):
		"""
		save batch processing results into PostgreSQL database and adds necessary index
		"""
		config = {key: self.psql_config[key] for key in ["url", "format", "driver", "user", "password", "mode_batch", "dbtable_batch"]}
		self.df.write\
			.format(config["format"])\
    		.option("url", config["url"])\
    		.option("driver", config["driver"])\
    		.option("dbtable", config["dbtable_batch"])\
    		.option("user", config["user"])\
    		.option("password", config["password"]) \
    		.mode(config["mode_batch"])\
    		.save()


	def run(self):
		"""
		executes the read from S3, transform by Spark and write to PostgreSQL database sequence
		"""
		self.read_from_s3()
		self.spark_transform()
		self.spark_create_block()
		self.save_to_postgresql()
		



