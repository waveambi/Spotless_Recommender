import sys
sys.path.append("./helpers/")
import json
import helper
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType
class BatchProcessor:
	"""
	class that reads data from S3 bucket, prcoesses it with Spark
	and saves the results into PostgreSQL database
	"""
	def __init__(self, s3_configfile, psql_configfile):
		"""
		class constructor that initializes the Spark job according to the configurations of
		the S3 bucket, and PostgreSQL connection
		:type s3_configfile:     str  path to S3 config file
		:type psql_configfile:   str  path tp psql config file
		"""
		self.s3_config   = helper.parse_config(s3_configfile)
		self.psql_config = helper.parse_config(psql_configfile)
		self.conf = SparkConf()
		self.sc = SparkContext(conf=self.conf)
		self.spark = SparkSession.builder.config(conf=self.conf).getOrCreate()
		self.sc.setLogLevel("ERROR")

	def read_from_s3(self):
		"""
		reads files from s3 bucket defined by s3_configfile and creates Spark Dataframe
		"""
		yelp_business_filename = "s3a://{}/{}/{}".format(self.s3_config["BUCKET"], self.s3_config["YELP_FOLDER"], self.s3_config["YELP_BUSINESS_DATA_FILE"])
		yelp_rating_filename = "s3a://{}/{}/{}".format(self.s3_config["BUCKET"], self.s3_config["YELP_FOLDER"], self.s3_config["YELP_REVIEW_DATA_FILE"])
		sanitory_inspection_filename = "s3a://{}/{}/{}".format(self.s3_config["BUCKET"], self.s3_config["INSPECTION_FOLDER"], self.s3_config["INSPECTION_DATA_FILE"])
		self.df_yelp_business = self.spark.read.json(yelp_business_filename)
		self.df_yelp_rating = self.spark.read.json(yelp_rating_filename)
		self.df_sanitory_inspection = self.spark.read.csv(sanitory_inspection_filename, header=True)
		#self.df_sanitory_inspection = self.df_sanitory_inspection.groupby('name').agg({'Current_Demerits':'mean'}).withColumnRenamed("")


	def spark_ranking_transform(self):
		"""
		transforms Spark DataFrame with business dataset and sanitory inspection into cleaned data;
		adds information
		"""
		self.trim_zipcode_udf = udf(lambda x: helper.trim_zipcode(x), StringType())
		self.format_address_udf = udf(lambda x: helper.format_address(x), StringType())
		self.format_name_udf = udf(lambda x: helper.format_name(x), StringType())
		self.fuzzy_match_udf = udf(lambda x, y: helper.fuzzy_match(x, y), IntegerType())

		self.df_sanitory = self.df_sanitory.select("Restaurant_Name", "Location_Name", "Category_Name", "Address", \
												   "City", "Zip", "Location_1", "Inspection_Demerits")
		self.df_sanitory = self.df_sanitory.withColumn("Zipcode", self.trim_zipcode_udf("Zip")).drop("Zip")
		self.df_sanitory_summary = self.df_sanitory.groupby("Location_Name", "Address", "Zipcode").agg(
			{"Inspection_Demerits": "mean"}).withColumnRenamed("avg(Inspection_Demerits)",
															   "Avg_Inspection_Demerits").dropna()
		self.df_sanitory_summary = self.df_sanitory_summary.withColumn("Formatted_Address", self.format_address_udf("Address"))
		self.df_sanitory_summary = self.df_sanitory_summary.withColumn("Formatted_Name", self.format_name_udf("Location_Name"))

		self.df_yelp_business = self.df_yelp_business\
									.filter(self.df_yelp_business.city == "Las Vegas")\
									.select("business_id", "name", "address", "city", "postal_code",\
											"latitude", "longitude", "stars", "review_count")\
									.dropna()
		self.df_yelp_business = self.df_yelp_business.withColumn("formatted_address", self.format_address_udf("address"))
		self.df_yelp_business = self.df_yelp_business.withColumn("formatted_name", self.format_name_udf("name"))
		self.df_joined = self.df_yelp_business.join(self.df_sanitory_summary, (self.df_yelp_business.formatted_address == self.df_sanitory_summary.Formatted_Address) \
													& (self.df_yelp_business.postal_code == self.df_sanitory_summary.Zipcode), 'inner')
		self.df_ranking = self.df_joined.withColumn("ratio", self.fuzzy_match_udf("formatted_name", "Formatted_Name"))\
								.filter(self.df_joined.ratio >= 60)\
								.select("business_id", "name", "address", "latitude", "longitude", "stars", "Avg_Inspection_Demerits")


	def spark_recommendation_transform(self):
		"""
		transform Spark DataFrame
		:return:
		"""
		self.df_yelp_rating = self.df_yelp_rating.select("review_id", "user_id", "business_id", "stars", "text")\
									.withColumnRenamed("stars", "ratings")

		self.df_yelp_rating = self.df_yelp_rating.select("review_id", "user_id", "business_id", "stars", "text")\
									.withColumnRenamed("stars", "ratings")
		self.df_yelp_business = self.df_yelp_business.filter(self.df_yelp_business.city == "Las Vegas").select("business_id", "name", "address", "city", "postal_code", "latitude", "longitude", "state", "stars", "review_count")
		self.df = self.df_yelp_business.join(self.df_sanitory_inspection, (self.df_yelp_business.address == self.df_sanitory_inspection.Address) & (self.df_yelp_business.name == self.df_sanitory_inspection.Restaurant_Name), 'inner')
		self.df = self.df.select("business_id", "name", "address", "latitude", "longitude", "stars", "Category_Name", "Current_Demerits") #.join(self.df_yelp_rating, "business_id", 'inner')


	def spark_create_block(self):
		self.determine_block_lat_ids_udf = udf(lambda z: helper.determine_block_lat_ids(z), IntegerType())
		self.determine_block_log_ids_udf = udf(lambda z: helper.determine_block_log_ids(z), IntegerType())
		self.df_ranking = self.df_ranking.withColumn("latitude_id", self.determine_block_lat_ids_udf("latitude"))
		self.df_ranking = self.df_ranking.withColumn("longitude_id", self.determine_block_log_ids_udf("longitude"))

	def spark_ranking(self):
		"""
        calculates restaurant recommendation and ranks with Spark DataFrame
        """
		# KNN and MF


	def save_to_postgresql(self):
		"""
		save batch processing results into PostgreSQL database and adds necessary index
		"""
		config = {key: self.psql_config[key] for key in ["url", "driver", "user", "password", "mode_batch", "dbtable_batch"]}
		self.df_ranking.write\
			.format("jdbc")\
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
		self.spark_ranking_transform()
		self.spark_create_block()
		self.save_to_postgresql()
		



