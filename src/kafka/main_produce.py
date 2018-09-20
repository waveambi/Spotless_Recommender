import sys
sys.path.append("./helpers/")
from kafka_producer import MyKafkaProducer

### main method that produces messages into Kafka topic ###

if __name__ == "__main__":

    if len(sys.argv) != 4:
        sys.stderr("Usage: main_produce.py <kafkaconfigfile> <s3configfile> \n")
        sys.exit(-1)

    kafka_configfile, s3_configfile = sys.argv[1:3]

    prod = MyKafkaProducer(kafka_configfile, s3_configfile)
    prod.produce_msgs()