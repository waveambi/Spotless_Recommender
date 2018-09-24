import sys

sys.path.append("./helpers/")
import helper
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType

from pyspark.ml import Pipeline, PipelineModel
from sparknlp.annotator import SentenceDetector, Tokenizer, Normalizer, Lemmatizer, SentimentDetector
from sparknlp.common import RegexRule
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

    def spark_nlp_sentiment_analysis(self):
        """
        :return:
        """
        self.lemma_file = "s3a://{}/{}/{}".format(self.s3_config["BUCKET"], self.s3_config["TEXT_CORPUS_FOLDER"], \
                                                  self.s3_config["LEMMA_FILE"])
        self.sentiment_file = "s3a://{}/{}/{}".format(self.s3_config["BUCKET"], self.s3_config["TEXT_CORPUS_FOLDER"], \
                                                      self.s3_config["SENTIMENT_FILE"])
        self.convert_sentiment_udf = udf(lambda x: helper.convert_sentiment(x), IntegerType())
        self.df_yelp_review = self.df_yelp_review \
            .select("user_id", "business_id", "stars", "text") \
            .withColumnRenamed("stars", "ratings")
        self.df_yelp_business_city_filter = self.df_yelp_business \
            .filter(self.df_yelp_business.city == "Las Vegas") \
            .select("business_id")
        self.df_yelp_review = self.df_yelp_review \
            .join(self.df_yelp_business_city_filter, self.df_yelp_review.business_id
                  == self.df_yelp_business_city_filter.business_id, 'inner') \
            .drop(self.df_yelp_business_city_filter.business_id)

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
            .setDictionary(self.lemma_file, key_delimiter="->", value_delimiter="\t") \
            .option(readAs="SPARK_DATASET")
        sentiment_detector = SentimentDetector() \
            .setInputCols(["lemma", "sentence"]) \
            .setOutputCol("sentiment_score") \
            .setDictionary(self.sentiment_file, delimiter=",") \
            .option(readAs="SPARK_DATASET")
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
        self.df_sentiment_positive = self.df_sentiment.filter(self.df_sentiment.sentiment == "positive")
        self.df_sentiment_positive = self.df_sentiment_positive.groupBy("business_id").agg(
            {"sentiment": "count"}).withColumnRenamed("count(sentiment)", "positive").na.fill(0)
        self.df_sentiment_negative = self.df_sentiment.filter(self.df_sentiment.sentiment == "negative")
        self.df_sentiment_negative = self.df_sentiment_negative.groupBy("business_id").agg(
            {"sentiment": "count"}).withColumnRenamed("count(sentiment)", "negative").na.fill(0)
        self.df_sentiment = self.df_sentiment_positive.join(self.df_sentiment_negative,
                                    self.df_sentiment_positive.business_id == self.df_sentiment_negative.business_id, 'outer') \
                                .drop(self.df_sentiment_negative.business_id)
        self.df_sentiment = self.df_sentiment.withColum("sentiment_score", (self.df_sentiment.positive - self.df_sentiment.negative)
                                                        / (self.df_sentiment.positive + self.df_sentiment.negative)) \
                                .select("business_id", "sentimen_score")

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
                                                                   "longitude",
                                                                   "stars")
        self.df_ranking = self.df_ranking.join(self.df_yelp_business_slice,
                                               (self.df_ranking.business_id == self.df_yelp_business_slice.business_id),
                                               "right").drop(
            self.df_ranking.business_id)
        avg_demerits = self.df_ranking.agg({"Avg_Inspection_Demerits": "mean"}).collect()[0][0]
        self.df_ranking = self.df_ranking.na.fill(avg_demerits)

    def spark_join_ranking_and_review(self):
        """

        :return:
        """
        self.df = self.df_ranking \
            .join(self.df_sentiment, self.df_ranking.business_id == self.df_sentiment.business_id, 'inner') \
            .drop(self.df_sentiment.business_id) \
            .dropna()

    def spark_create_block(self):
        self.determine_block_lat_ids_udf = udf(lambda z: helper.determine_block_lat_ids(z), IntegerType())
        self.determine_block_log_ids_udf = udf(lambda z: helper.determine_block_log_ids(z), IntegerType())
        self.df_ranking = self.df_ranking.withColumn("latitude_id", self.determine_block_lat_ids_udf("latitude"))
        self.df_ranking = self.df_ranking.withColumn("longitude_id", self.determine_block_log_ids_udf("longitude"))

    def save_to_postgresql(self):
        """
        save batch processing results into PostgreSQL database and adds necessary index
        """
        config = {key: self.psql_config[key] for key in
                  ["url", "driver", "user", "password", "mode_batch", "dbtable_batch"]}
        self.df.write \
            .format("jdbc") \
            .option("url", config["url"]) \
            .option("driver", config["driver"]) \
            .option("dbtable", config["dbtable_batch"]) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .mode(config["mode_batch"]) \
            .save()

    def run(self):
        """
        executes the read from S3, transform by Spark and write to PostgreSQL database sequence
        """
        self.read_from_s3()
        self.spark_nlp_sentiment_analysis()
        self.spark_ranking_transform()
        self.spark_join_ranking_and_review()
        self.spark_create_block()
        self.save_to_postgresql()
