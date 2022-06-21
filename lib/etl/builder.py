class InsertBuilder:
    """
    This class defines an insert statement builder
    that maps a data input into a SQL statement.
    """

    def __init__(self, table, columns, mapper):
        """
        The constructor instantiates an insert builder with the following arguments:

        Args:

        `table`: The name of the table to generate the insert statement.
        `columns`: The list of the table columns.
        `mapper`: A function that maps the input values to the table columns' values.
        """
        self.table = table
        self.columns = columns
        self.mapper = mapper

    def to_sql(self, values):
        """
        Maps the data input to the table insert statement.

        Args:

        `values`: The data to be mapped into insert values.
        """
        query = f"""
        INSERT INTO {self.table} ({', '.join(self.columns)})
        VALUES ({', '.join(self.mapper(values, self.columns))});\n
        """
        return query
