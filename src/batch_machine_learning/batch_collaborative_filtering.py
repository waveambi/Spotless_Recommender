import sys

sys.path.append("./helpers/")
import helper
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator


class BatchMachineLearning:
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
        self.s3_config = helper.parse_config(s3_configfile)
        self.psql_config = helper.parse_config(psql_configfile)
        self.conf = SparkConf()
        self.sc = SparkContext(conf=self.conf)
        self.spark = SparkSession.builder.config(conf=self.conf).getOrCreate()
        self.sc.setLogLevel("ERROR")


    def read_from_s3(self):
        """
        reads files from s3 bucket defined by s3_configfile and creates Spark Dataframe
        """
        yelp_business_filename = "s3a://{}/{}/{}".format(self.s3_config["BUCKET"], self.s3_config["YELP_FOLDER"],
                                                         self.s3_config["YELP_BUSINESS_DATA_FILE"])
        yelp_rating_filename = "s3a://{}/{}/{}".format(self.s3_config["BUCKET"], self.s3_config["YELP_FOLDER"],
                                                       self.s3_config["YELP_REVIEW_DATA_FILE"])
        self.df_yelp_business = self.spark.read.json(yelp_business_filename)
        self.df_yelp_review = self.spark.read.json(yelp_rating_filename)


    def spark_recommendation_transform(self):
        """
        transform Spark DataFrame
        :return:
        """
        self.df_yelp_review = self.df_yelp_review \
            .select("user_id", "business_id", "stars") \
            .withColumnRenamed("stars", "ratings")
        self.df_yelp_business = self.df_yelp_business \
            .filter(self.df_yelp_business.city == "Las Vegas") \
            .select("business_id", "name", "address", "latitude", "longitude")
        config = {key: self.psql_config[key] for key in
                  ["url", "driver", "user", "password", "mode_batch", "dbtable_batch", "nums_partition"]}
        self.df_filter_id = self.spark.read \
            .format("jdbc") \
            .option("url", config["url"]) \
            .option("driver", config["driver"]) \
            .option("dbtable", config['dbtable_batch']) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .load()
        self.df_filter_id = self.df_filter_id.select("business_id")
        self.df_yelp_business = self.df_yelp_business \
            .join(self.df_filter_id, self.df_yelp_business.business_id
                  == self.df_filter_id.business_id, 'inner') \
            .drop(self.df_filter_id.business_id)

        self.df_yelp_rating = self.df_yelp_business \
            .join(self.df_yelp_review, self.df_yelp_business.business_id == self.df_yelp_review.business_id) \
            .drop(self.df_yelp_review.business_id)
        '''
        self.df_yelp_filter_user = self.df_yelp_rating \
            .groupby("user_id") \
            .agg({"business_id": "count"}) \
            .withColumnRenamed("count(business_id)", "ratings_count")
        self.df_yelp_filter_user = self.df_yelp_filter_user \
            .filter(self.df_yelp_filter_user.ratings_count >= 10)
        print("total number of users with more than 10 records is ", self.df_yelp_filter_user.count().collect()[0][0])

        self.df_yelp_filter_business = self.df_yelp_rating \
            .groupby("business_id") \
            .agg({"user_id": "count"}) \
            .withColumnRenamed("count(user_id)", "ratings_count")
        self.df_yelp_filter_business = self.df_yelp_filter_business \
            .filter(self.df_yelp_filter_business.ratings_count >= 1)

        self.df_yelp_rating_sample = self.df_yelp_rating \
            .join(self.df_yelp_filter_user, self.df_yelp_rating.user_id == self.df_yelp_filter_user.user_id, "inner") \
            .drop(self.df_yelp_filter_user.user_id) \
            .join(self.df_yelp_filter_business,
                  self.df_yelp_rating.business_id == self.df_yelp_filter_business.business_id, "inner") \
            .drop(self.df_yelp_filter_business.business_id) \
            .select("business_id", "user_id", "ratings")
        '''
        self.df_yelp_rating_sample = self.df_yelp_rating.select("business_id", "user_id", "ratings")
        self.user_indexer = StringIndexer(inputCol="user_id", outputCol="user_id_indexed", handleInvalid='error')
        self.user_index = self.user_indexer \
            .fit(self.df_yelp_rating_sample.select("user_id")) \
            .transform(self.df_yelp_rating_sample.select("user_id"))
        self.df_yelp_rating_sample = self.df_yelp_rating_sample \
            .join(self.user_index, self.df_yelp_rating_sample.user_id == self.user_index.user_id, "inner") \
            .drop(self.user_index.user_id)

        self.business_indexer = StringIndexer(inputCol="business_id", outputCol="business_id_indexed",
                                              handleInvalid='error')
        self.business_index = self.business_indexer \
            .fit(self.df_yelp_rating_sample.select("business_id")) \
            .transform(self.df_yelp_rating_sample.select("business_id"))
        self.df_yelp_rating_sample = self.df_yelp_rating_sample \
            .join(self.business_index, self.df_yelp_rating_sample.business_id == self.business_index.business_id,
                  "inner") \
            .drop(self.business_index.business_id)

        self.df_yelp_rating_sample = self.df_yelp_rating_sample \
            .withColumn("user_id_indexed", self.df_yelp_rating_sample["user_id_indexed"].cast(IntegerType())) \
            .withColumn("business_id_indexed", self.df_yelp_rating_sample["business_id_indexed"].cast(IntegerType()))


    def spark_recommendation_prediction(self):
        """
        calculates restaurant recommendation and ranks with ALS Matrix Factorization
        """
        self.df_training, self.df_test = self.df_yelp_rating_sample.randomSplit([0.8, 0.2])
        als = ALS(maxIter=5, coldStartStrategy="drop", userCol='user_id_indexed', itemCol='business_id_indexed',
                  ratingCol='ratings')

        pipeline = Pipeline(stages=[als])
        param = ParamGridBuilder() \
            .addGrid(als.regParam, [0.01, 0.1]) \
            .addGrid(als.rank, [5, 10]) \
            .build()
        crossval = CrossValidator(estimator=pipeline,
                                  estimatorParamMaps=param,
                                  evaluator=RegressionEvaluator(metricName='rmse', labelCol='rating'),
                                  numFolds=3)
        cvModel = crossval.fit(self.df_training)
        cvModel.write.save("sample-model")
        predictions = cvModel.transform(self.df_test)
        evaluator = RegressionEvaluator(metricName='rmse', labelCol='rating')
        rmse = evaluator.evaluate(predictions)
        print("model's RMSE is ", rmse)
        #rmse.write.save("rmse")
        self.df_results = cvModel.recommendForAllUsers(10)


    def save_to_postgresql(self):
        """
        save batch processing results into PostgreSQL database and adds necessary index
        """
        config = {key: self.psql_config[key] for key in
                  ["url", "driver", "user", "password", "mode_batch", "dbtable_batch", "nums_partition"]}
        self.df_results.write \
            .format("jdbc") \
            .option("url", config["url"]) \
            .option("driver", config["driver"]) \
            .option("dbtable", config["dbtable_batch"]) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .mode(config["mode_batch"]) \
            .option("numPartitions", config["nums_partition"]) \
            .save()


    def run(self):
        """
        executes the read from S3, transform by Spark and write to PostgreSQL database sequence
        """
        self.read_from_s3()
        self.spark_recommendation_transform()
        self.spark_recommendation_prediction()
        self.save_to_postgresql()
