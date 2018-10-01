import sys

sys.path.append("./helpers/")
import helper
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import functions
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

from pyspark.ml import Pipeline
from sparknlp.annotator import SentenceDetector, Tokenizer, Normalizer, Lemmatizer, SentimentDetector
from sparknlp.base import DocumentAssembler, Finisher


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
        sanitary_inspection_filename = "s3a://{}/{}/{}".format(self.s3_config["BUCKET"],
                                                               self.s3_config["INSPECTION_FOLDER"],
                                                               self.s3_config["INSPECTION_DATA_FILE"])
        self.df_yelp_business = self.spark.read.json(yelp_business_filename)
        self.df_yelp_review = self.spark.read.json(yelp_rating_filename)
        self.df_sanitary = self.spark.read.csv(sanitary_inspection_filename, header=True)
        self.trim_zipcode_udf = udf(lambda x: helper.trim_zipcode(x), StringType())
        self.format_address_udf = udf(lambda x: helper.format_address(x), StringType())
        self.format_name_udf = udf(lambda x: helper.format_name(x), StringType())
        self.fuzzy_match_udf = udf(lambda x, y: helper.fuzzy_match(x, y), IntegerType())
        self.convert_sentiment_udf = udf(lambda x: helper.convert_sentiment(x), IntegerType())
        self.calculate_score_udf = udf(lambda x, y, z: helper.calculate_score(x, y, z), IntegerType())

    def spark_ranking_transform(self):
        """
        transforms Spark DataFrame with business dataset and sanitary inspection into cleaned data;
        adds information
        """
        self.df_sanitary = self.df_sanitary.select("Restaurant_Name", "Location_Name", "Category_Name", "Address", \
                                                   "City", "Zip", "Location_1", "Inspection_Demerits")
        self.df_sanitary = self.df_sanitary.withColumn("Zipcode", self.trim_zipcode_udf("Zip")).drop("Zip")
        self.df_sanitary_summary = self.df_sanitary.groupby("Location_Name", "Address", "Zipcode").agg(
            {"Inspection_Demerits": "mean"}).withColumnRenamed("avg(Inspection_Demerits)",
                                                               "Avg_Inspection_Demerits").dropna()
        self.df_sanitary_summary = self.df_sanitary_summary.withColumn("Formatted_Address",
                                                                       self.format_address_udf("Address"))
        self.df_sanitary_summary = self.df_sanitary_summary.withColumn("Formatted_Name",
                                                                       self.format_name_udf("Location_Name"))
        self.df_yelp_business = self.df_yelp_business \
            .filter(self.df_yelp_business.city == "Las Vegas") \
            .select("business_id", "name", "address", "city", "postal_code", \
                    "latitude", "longitude", "stars", "review_count") \
            .dropna()
        self.df_yelp_business = self.df_yelp_business.withColumn("formatted_address",
                                                                 self.format_address_udf("address"))
        self.df_yelp_business = self.df_yelp_business.withColumn("formatted_name", self.format_name_udf("name"))
        self.df_joined = self.df_yelp_business.join(self.df_sanitary_summary, (
            self.df_yelp_business.formatted_address == self.df_sanitary_summary.Formatted_Address) \
                                                    & (
                                                        self.df_yelp_business.postal_code == self.df_sanitary_summary.Zipcode),
                                                    'inner')
        self.df_joined = self.df_joined.withColumn("ratio", self.fuzzy_match_udf("formatted_name", "Formatted_Name"))
        self.df_ranking = self.df_joined.filter(self.df_joined.ratio >= 60) \
            .select("business_id", "name", "address", "latitude", \
                    "longitude", "stars", "Avg_Inspection_Demerits")
        self.df_ranking = self.df_ranking.groupby("business_id") \
            .agg({"Avg_Inspection_Demerits": "mean"}) \
            .withColumnRenamed("avg(Avg_Inspection_Demerits)", "Avg_Inspection_Demerits") \
            .dropna()
        self.df_yelp_business_slice = self.df_yelp_business.select("business_id", "name", "address", "latitude",
                                                                   "longitude", "stars")
        self.df_ranking = self.df_ranking.join(self.df_yelp_business_slice,
                                               (self.df_ranking.business_id == self.df_yelp_business_slice.business_id),
                                               "inner").drop(self.df_ranking.business_id)
        self.df_ranking.cache()

    def spark_nlp_sentiment_analysis(self):
        """
        :return:
        """
        self.lemma_file = "s3a://{}/{}/{}".format(self.s3_config["BUCKET"], self.s3_config["TEXT_CORPUS_FOLDER"], \
                                                  self.s3_config["LEMMA_FILE"])
        self.sentiment_file = "s3a://{}/{}/{}".format(self.s3_config["BUCKET"], self.s3_config["TEXT_CORPUS_FOLDER"], \
                                                      self.s3_config["SENTIMENT_FILE"])

        self.df_yelp_review = self.df_yelp_review \
            .select("user_id", "business_id", "stars", "text") \
            .withColumnRenamed("stars", "ratings")
        self.df_id_filter = self.df_ranking.select("business_id")
        self.df_yelp_review = self.df_yelp_review \
            .join(self.df_id_filter, self.df_yelp_review.business_id
                  == self.df_id_filter.business_id, 'inner') \
            .drop(self.df_id_filter.business_id)
        self.df_yelp_review.cache()

        document_assembler = DocumentAssembler() \
            .setInputCol("text")
        sentence_detector = SentenceDetector() \
            .setInputCols(["document"]) \
            .setOutputCol("sentence")
        tokenizer = Tokenizer() \
            .setInputCols(["sentence"]) \
            .setOutputCol("token")
        normalizer = Normalizer() \
            .setInputCols(["token"]) \
            .setOutputCol("normal")
        lemmatizer = Lemmatizer() \
            .setInputCols(["token"]) \
            .setOutputCol("lemma") \
            .setDictionary(self.lemma_file, key_delimiter="->", value_delimiter="\t")
        sentiment_detector = SentimentDetector() \
            .setInputCols(["lemma", "sentence"]) \
            .setOutputCol("sentiment_score") \
            .setDictionary(self.sentiment_file, delimiter=",")
        finisher = Finisher() \
            .setInputCols(["sentiment_score"]) \
            .setOutputCols(["sentiment"])
        pipeline = Pipeline(stages=[
            document_assembler, \
            sentence_detector, \
            tokenizer, \
            normalizer, \
            lemmatizer, \
            sentiment_detector, \
            finisher
        ])

        self.df_sentiment = pipeline.fit(self.df_yelp_review).transform(self.df_yelp_review)
        self.df_sentiment.cache()

        self.df_sentiment = self.df_sentiment.select(self.df_sentiment.business_id,
                                                     functions.when(self.df_sentiment.sentiment == "positive", 1).when(
                                                         self.df_sentiment.sentiment == "negative", -1).otherwise(0)) \
            .withColumnRenamed("CASE WHEN (sentiment = positive) THEN 1 WHEN (sentiment = negative) THEN -1 ELSE 0 END",
                               "sentiment")
        self.df_sentiment = self.df_sentiment.groupby("business_id").agg({"sentiment": "mean"}).withColumnRenamed(
            "avg(sentiment)", "avg_sentiment_score")

    def spark_create_block(self):
        self.determine_block_lat_ids_udf = udf(lambda z: helper.determine_block_lat_ids(z), IntegerType())
        self.determine_block_log_ids_udf = udf(lambda z: helper.determine_block_log_ids(z), IntegerType())
        self.df_ranking = self.df_ranking.withColumn("latitude_id", self.determine_block_lat_ids_udf("latitude"))
        self.df_ranking = self.df_ranking.withColumn("longitude_id", self.determine_block_log_ids_udf("longitude"))

    def spark_join_ranking_and_review(self):
        """

        :return:
        """
        self.df = self.df_ranking \
            .join(self.df_sentiment, self.df_ranking.business_id == self.df_sentiment.business_id, 'inner') \
            .drop(self.df_sentiment.business_id) \
            .dropna()
        avg_demerits = self.df.agg({"Avg_Inspection_Demerits": "mean"}).collect()[0][0]  # around 6.53~
        print("Average score on sanitory inspections is ", avg_demerits)
        avg_rating = self.df.agg({"stars": "mean"}).collect()[0][0]
        print("Average rating on yelp review is ", avg_rating) #3.33
        avg_sentiment = self.df.agg({"avg_sentiment_score": "mean"}).collect()[0][0] #0.77
        print("Average sentiment on yelp review is ", avg_sentiment)
        self.df = self.df.fillna({"avg_sentiment_score": 0.77, "stars": 3.33, "Avg_Inspection_Demerits": 6.53})
        self.df = self.df.withColumn("score", self.calculate_score_udf("avg_sentiment_score", "stars", "Avg_Inspection_Demerits"))
        column_list = ["latitude_id", "longitude_id"]
        window = Window.partitionBy([col(x) for x in column_list]).orderBy(self.df['score'].desc())
        self.df = self.df.select("latitude_id", "longitude_id", "business_id", "name", "address", "latitude", "longitude", "score", "avg_sentiment_score", "stars", "Avg_Inspection_Demerits") \
                    .select("*", rank().over(window).alias('rank')).filter(col('rank') <= 10)

    def save_to_postgresql(self):
        """
        save batch processing results into PostgreSQL database and adds necessary index
        """
        config = {key: self.psql_config[key] for key in
                  ["url", "driver", "user", "password", "mode_batch", "dbtable_batch", "nums_partition"]}
        self.df.write \
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
        self.spark_ranking_transform()
        self.spark_nlp_sentiment_analysis()
        self.spark_create_block()
        self.spark_join_ranking_and_review()
        self.save_to_postgresql()
