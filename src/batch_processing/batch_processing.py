import sys
sys.path.append("./helpers/")
import json
import helper
import postgre
from pyspark import SparkContext

class BatchProcessor:
	"""
	class that reads data from S3 bucket, prcoesses it with Spark
	and saves the results into PostgreSQL database
	"""
	def __init__(self, s3_configfile, psql_configfile):
		"""
		class constructor that initializes the instance according to the configurations of the S3 bucket, raw data and PostgreSQL table
		:type s3_configfile:     str  path to S3 config file
		:type schema_configfile: str  path to schema config file
		:type psql_configfile:   str  path tp psql config file
		"""
        self.s3_config   = helper.parse_config(s3_configfile)
        	#self.schema      = helpers.parse_config(schema_configfile)
        self.psql_config = helper.parse_config(psql_configfile)
        self.sc = SparkContext()
        self.sc.setLogLevel("ERROR")


    def read_from_s3(self):
		"""
    	reads files from s3 bucket defined by s3_configfile and creates Spark RDD
    	"""
    	yelp_business_filename = "s3a://{}/{}/{}".format(self.s3_config["BUCKET1"],
    									   self.s3_config["FOLDER2"],
    									   self.s3_config["RAW_DATA_FILE1"])
    	yelp_rating_filename = "s3a://{}/{}/{}".format(self.s3_config["BUCKET2"],
    									   self.s3_config["FOLDER2"],
    									   self.s3_config["RAW_DATA_FILE2"])
    	sanitory_inspection_filename = "s3a://{}/{}/{}".format(self.s3_config["BUCKET3"],
    									   self.s3_config["FOLDER3"],
    									   self.s3_config["RAW_DATA_FILE3"])
    	self.df_yelp_business = self.sc.read.json(yelp_business_filename)
    	self.df_yelp_rating = self.sc.read.json(yelp_rating_filename)
    	self.df_sanitory_inspection = self.sc.read.csv(sanitory_inspection_filename)


	def save_to_postgresql(self):
		"""
		save batch processing results into PostgreSQL database and adds necessary index
		"""
		config = {key: self.psql_config[key] for key in ["url", "driver", "user", "password"]}
		config["dbtable"] = self.psql_config["dbtable_batch"]
		postgres.save_to_postgresql(self.df, pyspark.sql.SQLContext(self.sc), configs, self.psql_config["mode_batch"])


	def run(self):
		"""
        executes the read from S3, transform by Spark and write to PostgreSQL database sequence
        """
        self.read_from_s3()
        #self.spark_transform()
        #self.save_to_postgresql()
		


"""
df_business = sc.read.json("s3a://insightdatascience/Yelp/yelp_academic_dataset_business.json")
df_business_vegas = df_business.filter(df_business.city == "Las Vegas").select("business_id", "name", "address", "city", "postal_code", "latitude", "longitude", "state", "stars", "review_count") #28865 business in total
df_review = sc.read.json("s3a://insightdatascience/Yelp/yelp_academic_dataset_review.json").select("review_id", "user_id", "business_id", "stars", "text") #5,996,996 in total records
df_sanitory = sc.read.csv("s3a://insightdatascience/Las_Vegas_Restaurant_Inspections.csv", header=True) #21762 unique, 162977 in total restaurant check

df_restaurant = df_business_vegas.join(df_sanitory, (df_business_vegas.address == df_sanitory.Address) & (df_business_vegas.name == df_sanitory.Restaurant_Name), 'inner') #2564 joined result
df = df_restaurant.join(df_review, "business_id", 'inner')
df.cache()
df.groupby('user_id').agg({'business_id':'count'}).withColumnRenamed("count(business_id)", "restaurant_count")
df.groupby('business_id').agg({'user_id':'count'}).show()
"""


