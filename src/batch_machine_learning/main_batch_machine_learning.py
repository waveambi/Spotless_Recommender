import sys
from batch_collaborative_filtering import BatchMachineLearning

### main method that executes batch job ###

if __name__ == '__main__':

    if len(sys.argv) != 3:
        sys.stderr.write("Usage: spark-submit main_batch.py <s3configfile> <postgresconfigfile> \n")
        sys.exit(-1)

    s3_configfile, psql_configfile = sys.argv[1:3]
    learner = BatchMachineLearning(s3_configfile, psql_configfile)
    learner.run()
