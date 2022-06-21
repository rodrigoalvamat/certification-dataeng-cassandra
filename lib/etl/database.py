# database libs
from cassandra.cluster import Cluster


class Database:
    """
    Defines a class to connect to a Cassandra database.
    """

    def __init__(self, host, keyspace):
        """
        Create a new Database object to process SQL queries.

        Args:

        `host`: The hostname of the Cassandra cluster.
        `keyspace`: The keyspace name.
        """
        self.cluster, self.session = self.__create_session(host)
        self.__create_keyspace(keyspace)

    def __create_session(self, host):
        """
        Creates a new cluster and session objects.
        Also sets the current session keyspace.

        Args:

        `host`: The hostname of the Cassandra cluster.

        Returns:

        `cluster, session`: The cluster and the session objects.
        """
        try:
            # creates a new cluster and session objects.
            cluster = Cluster([host])
            return cluster, cluster.connect()
        except Exception as e:
            print(e)

    def __create_keyspace(self, keyspace):
        """
        Create a keyspace if it doesn't exist and set as current session keyspace.

        Args:

        `keyspace`: The keyspace name.
        """
        try:
            # create a keyspace if it doesn't exist
            self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace} 
            WITH REPLICATION = 
            {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}
            """)
            # set the current session keyspace
            self.session.set_keyspace(keyspace)
        except Exception as e:
            print(e)

    def execute_queries(self, queries):
        """
        Executes a list of queries and returns the responses.

        Args:

        `queries`: The list of queries.
        """
        responses = []
        try:
            for query in queries:
                response = self.session.execute(query)
                responses.append(response)
        except Exception as e:
            print(e)

        return responses

    def shutdown(self):
        """
        Shutdown the database connection.
        """
        self.session.shutdown()
        self.cluster.shutdown()
