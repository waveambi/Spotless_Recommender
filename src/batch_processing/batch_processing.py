import sys
sys.path.append("./helpers/")
import json
import helper
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator


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
		sanitary_inspection_filename = "s3a://{}/{}/{}".format(self.s3_config["BUCKET"], self.s3_config["INSPECTION_FOLDER"], self.s3_config["INSPECTION_DATA_FILE"])
		self.df_yelp_business = self.spark.read.json(yelp_business_filename)
		self.df_yelp_review = self.spark.read.json(yelp_rating_filename)
		self.df_sanitary = self.spark.read.csv(sanitary_inspection_filename, header=True)
		#self.df_sanitary_inspection = self.df_sanitary_inspection.groupby('name').agg({'Current_Demerits':'mean'}).withColumnRenamed("")


	def spark_ranking_transform(self):
		"""
		transforms Spark DataFrame with business dataset and sanitary inspection into cleaned data;
		adds information
		"""
		self.trim_zipcode_udf = udf(lambda x: helper.trim_zipcode(x), StringType())
		self.format_address_udf = udf(lambda x: helper.format_address(x), StringType())
		self.format_name_udf = udf(lambda x: helper.format_name(x), StringType())
		self.fuzzy_match_udf = udf(lambda x, y: helper.fuzzy_match(x, y), IntegerType())

		self.df_sanitary = self.df_sanitary.select("Restaurant_Name", "Location_Name", "Category_Name", "Address", \
												   "City", "Zip", "Location_1", "Inspection_Demerits")
		self.df_sanitary = self.df_sanitary.withColumn("Zipcode", self.trim_zipcode_udf("Zip")).drop("Zip")
		self.df_sanitary_summary = self.df_sanitary.groupby("Location_Name", "Address", "Zipcode").agg(
			{"Inspection_Demerits": "mean"}).withColumnRenamed("avg(Inspection_Demerits)",
															   "Avg_Inspection_Demerits").dropna()
		self.df_sanitary_summary = self.df_sanitary_summary.withColumn("Formatted_Address", self.format_address_udf("Address"))
		self.df_sanitary_summary = self.df_sanitary_summary.withColumn("Formatted_Name", self.format_name_udf("Location_Name"))
		self.df_yelp_business = self.df_yelp_business\
									.filter(self.df_yelp_business.city == "Las Vegas")\
									.select("business_id", "name", "address", "city", "postal_code",\
											"latitude", "longitude", "stars", "review_count")\
									.dropna()
		self.df_yelp_business = self.df_yelp_business.withColumn("formatted_address", self.format_address_udf("address"))
		self.df_yelp_business = self.df_yelp_business.withColumn("formatted_name", self.format_name_udf("name"))
		self.df_joined = self.df_yelp_business.join(self.df_sanitary_summary, (self.df_yelp_business.formatted_address == self.df_sanitary_summary.Formatted_Address) \
													& (self.df_yelp_business.postal_code == self.df_sanitary_summary.Zipcode), 'inner')
		self.df_joined = self.df_joined.withColumn("ratio", self.fuzzy_match_udf("formatted_name", "Formatted_Name"))
		self.df_ranking = self.df_joined.filter(self.df_joined.ratio >= 60)\
								.select("business_id", "name", "address", "latitude",\
										"longitude", "stars", "Avg_Inspection_Demerits")
		self.df_ranking = self.df_ranking.groupby("business_id", "name", "address", "latitude", "longitude", "stars")\
							.agg({"Avg_Inspection_Demerits": "mean"})\
							.withColumnRenamed("avg(Avg_Inspection_Demerits)", "Avg_Inspection_Demerits")\
							.dropna()

	def spark_recommendation_transform(self):
		"""
		transform Spark DataFrame
		:return:
		"""
		self.df_yelp_review = self.df_yelp_review\
									.select("user_id", "business_id", "stars")\
									.withColumnRenamed("stars", "ratings")
		self.df_yelp_business = self.df_yelp_business\
									.filter(self.df_yelp_business.city == "Las Vegas")\
									.select("business_id", "name", "address", "latitude", "longitude")
		self.df_yelp_rating = self.df_yelp_business\
									.join(self.df_yelp_review, self.df_yelp_business.business_id == self.df_yelp_review.business_id)\
									.drop(self.df_yelp_review.business_id)
		self.df_yelp_filter_user = self.df_yelp_rating\
										.groupby("user_id")\
										.agg({"business_id": "count"})\
										.withColumnRenamed("count(business_id)", "ratings_count")
		self.df_yelp_filter_user = self.df_yelp_filter_user\
										.filter(self.df_yelp_filter_user.ratings_count >= 3)

		self.df_yelp_filter_business = self.df_yelp_rating\
											.groupby("business_id")\
											.agg({"user_id": "count"})\
											.withColumnRenamed("count(user_id)", "ratings_count")
		self.df_yelp_filter_business = self.df_yelp_filter_business\
											.filter(self.df_yelp_filter_business.ratings_count >= 3)

		self.df_yelp_rating_sample = self.df_yelp_rating\
										.join(self.df_yelp_filter_user,	self.df_yelp_rating.user_id == self.df_yelp_filter_user.user_id, "inner")\
										.drop(self.df_yelp_filter_user.user_id)\
										.join(self.df_yelp_filter_business, self.df_yelp_rating.business_id == self.df_yelp_filter_business.business_id, "inner")\
										.drop(self.df_yelp_filter_business.business_id)\
										.select("business_id", "user_id", "ratings")


	def spark_create_block(self):
		self.determine_block_lat_ids_udf = udf(lambda z: helper.determine_block_lat_ids(z), IntegerType())
		self.determine_block_log_ids_udf = udf(lambda z: helper.determine_block_log_ids(z), IntegerType())
		self.df_ranking = self.df_ranking.withColumn("latitude_id", self.determine_block_lat_ids_udf("latitude"))
		self.df_ranking = self.df_ranking.withColumn("longitude_id", self.determine_block_log_ids_udf("longitude"))

	def spark_machine_learning(self):
		"""
        calculates restaurant recommendation and ranks with ALS Matrix Factorization
        """
		self.user_indexer = StringIndexer(inputCol="user_id", outputCol="user_id_indexed", handleInvalid='error')
		self.user_index = self.user_indexer\
								.fit(self.df_yelp_rating_sample.select("user_id"))\
								.transform(self.df_yelp_rating_sample.select("user_id"))
		self.df_yelp_rating_sample = self.df_yelp_rating_sample\
											.join(self.user_index, self.df_yelp_rating_sample.user_id == self.user_index.user_id, "inner")\
											.drop(self.user_index.user_id)

		self.business_indexer = StringIndexer(inputCol="business_id", outputCol="business_id_indexed", handleInvalid='error')
		self.business_index = self.business_indexer\
									.fit(self.df_yelp_rating_sample.select("business_id"))\
									.transform(self.df_yelp_rating_sample.select("business_id"))
		self.df_yelp_rating_sample = self.df_yelp_rating_sample\
											.join(self.business_index, self.df_yelp_rating_sample.business_id == self.business_index.business_id, "inner")\
											.drop(self.business_index.business_id)

		self.df_yelp_rating_sample = self.df_yelp_rating_sample\
											.withColumn("user_id_indexed", self.df_yelp_rating_sample["user_id_indexed"].cast(IntegerType()))\
											.withColumn("business_id_indexed", self.df_yelp_rating_sample["business_id_indexed"].cast(IntegerType()))
		self.df_training, self.df_test = self.df_yelp_rating_sample.randomSplit([0.8, 0.2])
		als = ALS(maxIter=5, coldStartStrategy="drop", userCol='user_id_indexed', itemCol='business_id_indexed', ratingCol='ratings')

		pipeline = Pipeline(stages=[als])
		param = ParamGridBuilder()\
				.addGrid(als.regParam = [0.01, 0.05, 0.1, 0.5])\
				.addGrid(als.rank: [5, 10, 15])\
				.build()
		crossval = CrossValidator(estimator=pipeline,
								  estimatorParamMaps=param,
								  evaluator=RegressionEvaluator(metricName='rmse', labelCol='rating'),
								  numFolds=2)
		cvModel = crossval.fit(self.df_training)
		cvModel.write.save("sample-model")
		predictions = cvModel.transform(self.df_test)
		# predictions.dropna().describe().show()
		evaluator = RegressionEvaluator(metricName='rmse', labelCol='rating')
		rmse = evaluator.evaluate(predictions)
		rmse.write.save("rmse")


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
		#self.spark_ranking_transform()
		#self.spark_create_block()
		#self.save_to_postgresql()
		self.spark_recommendation_transform()
		self.spark_machine_learning()
		



