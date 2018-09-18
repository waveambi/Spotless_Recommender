import sys
from batch_prcoessing import BatchProcessor

### main method that executes batch job ###

if __name__ == '__main__':

    if len(sys.argv) != 3:
        sys.stderr.write("Usage: spark-submit --jars <jars> main_batch.py <s3configfile> <postgresconfigfile> \n")
        sys.exit(-1)

    s3_configfile, psql_configfile = sys.argv[1:2]

    transformer = BatchProcessor(s3_configfile, psql_configfile)
    transformer.run()
