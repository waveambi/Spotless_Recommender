import sys
from streamer import Streamer

### main method that executes streaming job ###

if __name__ == '__main__':

    if len(sys.argv) != 4:
        sys.stderr.write(
            "Usage: spark-submit --packages <packages> main_stream.py <kafkaconfigfile> <streamconfigfile> <postgresconfigfile> \n")
        sys.exit(-1)

    kafka_configfile, stream_configfile, psql_configfile = sys.argv[1:4]

    streamer = Streamer(kafka_configfile, stream_configfile, psql_configfile)
    streamer.run()
