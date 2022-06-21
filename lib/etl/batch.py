# sys libs
from io import StringIO


class InsertBatch:
    """
    This class defines a batch processing object that can be
    used to insert data into the Apache Cassandra tables.

    See Also:

    `https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useBatchGoodExample.html`: DataStax documentation on batch statement.
    """

    def __init__(self, session, inserts, max_size=250):
        """
        The constructor instantiates a batch object with the following arguments:

        Args:

        `session`: Cassandra session object.
        `inserts`: List of insert statement builders per table.
        `max_size`: The maximum number of rows to be inserted by each commit.
        """
        self.session = session

        self.buffers = [None] * len(inserts)
        self.inserts = inserts

        self.max_size = max_size
        self.size = 0

    def __begin_batch(self):
        """
        Initializes the StringIO buffer for each table batch object
        and writes the BEGIN BATCH statement.
        """
        for index, buffer in enumerate(self.buffers):
            self.buffers[index] = StringIO()
            self.buffers[index].write("BEGIN BATCH\n")

    def __commit_batch(self):
        """
        Commits each table batch following the steps below:

        - Writes the APPLY BATCH statement.
        - Sets the StringIO buffer cursor to the beginning of the buffer.
        - Executes the batch statements using Cassandra's session method.
        - Closes the StringIO buffer.
        - Clears the batches array.
        - Resets the batch size counter.
        """
        for buffer in self.buffers:
            try:
                buffer.write('APPLY BATCH;\n')
                buffer.seek(0)
                self.session.execute(buffer.read())
                buffer.close()
            except Exception as e:
                print(e)

        self.buffers = [None] * len(self.inserts)
        self.size = 0

    def __write(self, values):
        """
        Maps the data input for each table insert statement and
        writes to the corresponding StringIO buffer.

        Args:

        `values`: The data input to write as a SQL insert.
        """
        for index, buffer in enumerate(self.buffers):
            buffer.write(self.inserts[index].to_sql(values))

    def commit(self, close=False):
        """
        This method must be called at the end of the processing
        to commit the last batch of insert instructions.
        Once the batch has not reached the maximum size
        to be automatically committed by the insertion method.

        Args:

        `close`: If True, closes the session.
        """
        self.__commit_batch()
        if close:
            self.session.shutdown()
            self.session.close()
            self.session = None

    def write(self, values):
        """
        Writes the data input into the the batch buffer as an insert statement.
        If the batch size is equal to the maximum size, the batch is committed
        and the buffer is cleared.

        Args:

        `values`: The data input to write as a SQL insert.
        """
        if self.size == 0:
            self.__begin_batch()

        if self.size < self.max_size:
            self.__write(values)
            self.size += 1
        else:
            self.__write(values)
            self.__commit_batch()
