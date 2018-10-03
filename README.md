# Spotless Restaurant Recommender
> ***Real-Time Nearby Sanitary Food Recommendation***

This is a project I completed during the Insight Data Engineering program (New York, Fall 2018).

***

This project is for clean food recommendation with Yelp ratings, reviews sentiments and sanitary inspection results released by goverment, to deliver a better restaurant recommendation near users.

Simulated user requests are generated with real-time location to **Recommender**, and they would receive top 5 nearby restaurant recommendation.*

> *Users could also receive top 5 best recommendation based on their food preference and dining history among the city.

![alt text](https://github.com/AndreyBozhko/TaxiOptimizer/blob/master/docs/map.jpg "TaxiOptimizer Screenshot")

Nearby restaurant are defined as walking distance within 2 minutes and map are segmentated into 200m x 200m chunks of city street.

Pipeline

-----------------

![alt text](https://github.com/waveambi/Insight_Recommendation/Pipeline.jpg "Recommender Pipeline")

***Batch Job***: download yelp ratings and reviews from Kaggle and sanitary inspections data from OpenData. Those are ingested from S3 bucket into Spark, which computes top-n nearby recommendation within every district of the grid, and results are writed into the AWS RDS Postgresql database.
> This process is scheduled and automated with Airflow.

***Streaming Job***: real-time simulated user requests and locations are ingested by Kafka and then consumed by Spark Streaming, where the data stream is joined with pre-calculated results from batch job to produce nearby recommendation for users.

### Data Sources
  1. Batch: [Yelp dataset by Kaggle](https://www.kaggle.com/yelp-dataset/yelp-dataset) contains restaurant data, user ratings and reviews data.

  2. Batch: [Sanitary Inspections Dataset](https://opendata.lasvegasnevada.gov/Public-Safety/Restaurant-Inspections/q8ye-5kwk) contains restaurants sanitary inspections data from 2010 to 2018

  2. Streaming: generated by simulator within this respiratory (streamed at 500~1,000 requests/s).


### Environment Setup

Install and configure [AWS CLI](https://aws.amazon.com/cli/) and [Pegasus](https://github.com/InsightDataScience/pegasus) on your local machine, and clone this repository using
`https://github.com/waveambi/Insight_Recommendation`.

> AWS Tip: Add your local IP to your AWS VPC inbound rules

> Pegasus Tip: follow the notes in docs/pegasus_setup.odt to configure Pegasus

#### CLUSTER STRUCTURE:

To reproduce my environment, 11 m4.large AWS EC2 instances are needed:

- (6 nodes) Spark Cluster - Batch and Streaming
- (4 nodes) Kafka Cluster
- Flask Node
- AWS RDS Postgresql node

To create the clusters, put the appropriate `master.yml` and `workers.yml` files in each `cluster_setup/<clustername>` folder (following the template in `cluster_setup/dummy.yml.template`), list all the necesary software in `cluster_setup/<clustername>/install.sh`, and run the `cluster_setup/create-clusters.sh` script.

> After that, **Recommender** need to be cloned to the clusters (with necessary .jar files downloaded and required Python packages installed), and any node's address from cluster *some-cluster-name* will be saved as the environment variable SOME_CLUSTER_NAME_$i on every master, where $i = 0, 1, 2, ...*


##### Airflow setup

The Apache Airflow scheduler can be installed on the master node of *spark-cluster*. Follow the instructions to launch the Airflow server.


##### PostgreSQL setup
The PostgreSQL database sits on AWS RDS *Postgresql* instance.

##### Configurations
Configuration settings for Kafka, Streaming, PostgreSQL, AWS S3 bucket for the data are stored in the respective files in `config/` folder.
> Replace the settings in `config/s3bucket.config.` with the names and paths for your S3 bucket.

## Running TaxiOptimizer

### Schedule the Batch Job
Running `airflow/schedule.sh` on the master of *spark-cluster* will add the batch job to the scheduler. The batch job is set to execute every 24 hours, and it can be started and monitored from the Airflow GUI at `http://$SPARK_CLUSTER_0:8081`.
Also batch job could be running manualy with `spark-batch-run.sh` and `spark-batch-machine-learning.sh` with this respirotary.

### Start the Streaming Job
Execute `./spark-streaming-run.sh` on the master of *spark-cluster* (preferably after batch job).

### Start streaming messages with Kafka
Execute `./kafka-run --produce` on the master of *kafka-cluster* to start simulating real-time user requests.
If the topic does not exist, run `./kafka-run --create`. To describe existing topic, run `./kafka-run --describe`.
It is also possible to delete inactive topic using the option `--delete`, or view the messages in the topic with the option `--console-consume`.

### Flask
Pending
