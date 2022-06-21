# command line libs
import argparse
# etl libs
from lib.etl.pipeline import Pipeline

if __name__ == "__main__":
    # create the command line parser
    parser = argparse.ArgumentParser(
        description='Apache Cassandra ETL Pipeline')

    # set the command line arguments
    parser.add_argument(
        '--batchsize', help='Batch size for the pipeline - default: 250', type=int, default=250)
    parser.add_argument(
        '--sourcedir', help='Relative path to the CSV data source directory - default: data/event_data', default='data/event_data')
    parser.add_argument(
        '--targetfile', help='Relative path to the target CSV file - default: events_datafile_new.csv', default='event_datafile_new.csv')
    parser.add_argument(
        '--hostname', help='Apache Cassandra hostname - default: 127.0.0.1', default='127.0.0.1')
    parser.add_argument(
        '--keyspace', help='Apache Cassandra keyspace - default: udacity', default='udacity')

    # parse the command line arguments
    args = parser.parse_args()

    # run the pipeline
    pipeline = Pipeline(args.sourcedir, args.targetfile,
                        args.hostname, args.keyspace)
    pipeline.run(args.batchsize)
