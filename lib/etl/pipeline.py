# sys libs
from timeit import default_timer as timer
# data libs
import csv
# etl libs
from lib.etl.batch import InsertBatch
from lib.etl.builder import InsertBuilder
from lib.etl.csv_data import CSVData
from lib.etl.database import Database
from lib.etl.queries import *


class Pipeline:
    """
    The ETL Pipeline class.
    """

    #: The dictionary of all the tables in the pipeline.
    TABLES = {
        'songplayBySession': ['sessionId', 'itemInSession', 'artist', 'song', 'length'],
        'songplayByUser': ['userId', 'sessionId', 'itemInSession', 'artist', 'song', 'firstName', 'lastName'],
        'songplayBySong': ['song', 'userId', 'firstName', 'lastName']
    }

    def __init__(self, source, target, host, keyspace):
        """
        Create a new Pipeline object, initialize the database and the CSV file processor.

        Args:

        `source`: The source folder path.
        `target`: The target file path.
        `host`: The hostname of the Cassandra cluster.
        `keyspace`: The keyspace name.
        """
        self.csv_data = CSVData(source, target)
        self.database = Database(host, keyspace)

    def __create_insert_builders(self):
        """
        Create a list of insert statement builders with all the tables.
        """
        return [InsertBuilder(table, self.TABLES[table], self.__csv_mapper) for table in self.TABLES.keys()]

    def __csv_mapper(self, values, columns):
        """
        Maps the csv data line in the target file to the table columns' values.

        Args:

        `values`: The CSV data line in target file.
        """
        # replace the single quote char by the corresponding unicode char
        # otherwise the insert statement will fail
        values[0] = values[0].replace("'", "\u2027")
        values[9] = values[9].replace("'", "\u2027")

        map = {
            'artist': f"'{values[0]}'",
            'firstName': f"'{values[1]}'",
            'gender': f"'{values[2]}'",
            'itemInSession': values[3],
            'lastName': f"'{values[4]}'",
            'length': values[5],
            'level': f"'{values[6]}'",
            'location': f"'{values[7]}'",
            'sessionId': values[8],
            'song': f"'{values[9]}'",
            'userId': values[10]
        }

        return [map[column] for column in columns]

    def __process_database_schema(self):
        """
        Create the database schema.
        """
        self.database.execute_queries(drop_table_queries)
        self.database.execute_queries(create_table_queries)

    def __process_batch_insert(self, batch_size):
        """
        Process a batch of insert statements.

        Args:

        `batch_size`: The maximum number of insert statements in a batch.
        """
        # create a batch processing for all table inserts
        # set the maximum size of each batch
        batch = InsertBatch(
            self.database.session, self.__create_insert_builders(), batch_size)

        with open(self.csv_data.target, encoding='utf8') as f:
            csvreader = csv.reader(f)
            next(csvreader)  # skip header

            # write the csv data line into the batch buffer
            # automatically commit if the buffer size is equal to the maximum size
            for line in csvreader:
                batch.write(line)

            # commit the last batch
            batch.commit()

    def run(self, batch_size=250):
        """
        Process the ETL pipeline.

        Args:

        `batch_size`: The maximum number of insert statements in a batch.
        """
        print('-----------------------------------------------------')
        print('Apache Cassandra ETL Pipeline')
        print('-----------------------------------------------------')
        # PHASE 1: create the database schema
        print('INFO: Creating the database schema...')
        schema_start = timer()

        self.__process_database_schema()

        schema_end = timer()
        print('INFO: Database schema created.')

        # PHASE 2: extract and transform the data
        print('INFO: Extracting and transforming from CSV files...')
        extract_start = timer()

        self.csv_data.extract()

        extract_end = timer()
        print('INFO: Extracted from CSV files.')

        # PHASE 3: load the data into the database
        print('INFO: Loading data into the database...')
        load_start = timer()

        self.__process_batch_insert(batch_size)

        load_end = timer()
        print('INFO: Loaded data into the database.')

        # STATS: print the time statistics
        print('-----------------------------------------------------')
        print('Time Statistics')
        print('-----------------------------------------------------')
        print(
            f'Database schema time: {round(schema_end - schema_start, 2)} seconds')
        print(
            f'Extracting data time: {round(extract_end - extract_start, 2)} seconds')
        print(f'Loading data time: {round(load_end - load_start, 2)} seconds')
        print(f'Total ETL time: {round(load_end - schema_start, 2)} seconds')

        # SHUTDOWN: close the database connection
        self.database.shutdown()
        print('-----------------------------------------------------')
        print('Done')
        print('-----------------------------------------------------')
