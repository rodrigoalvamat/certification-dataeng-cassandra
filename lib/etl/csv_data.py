# sys libs
import os
import glob
# data libs
import csv


class CSVData:
    """
    Defines the CSVData class to process data files. 
    """

    def __init__(self, source, target):
        """
        Create a new CSVData object to process CSV data from source to target.

        Args:

        `source`: The source folder path.
        `target`: The target file path.
        """
        self.source = source
        self.target = target

    def __get_files(self):
        """
        Get the list of file paths in the data folder.

        Returns:

        `files`: The list of file paths.
        """
        # get the current folder and 'dir' subfolder
        filepath = os.path.abspath(f'{os.getcwd()}/{self.source}')

        # create a list of files and collect each filepath
        for root, dirs, file_paths in os.walk(filepath):
            # join the file path and roots with the subdirectories using glob
            files = glob.glob(os.path.join(root, '*'))

        return files

    def __get_rows(self):
        """
        Read all CSV files and return a list of data rows.

        Returns:

        `rows`: The list of CSV data rows.
        """
        # initiating an empty list of rows
        rows = []
        # for every filepath in the file path list
        for f in self.__get_files():
            # reading csv file
            with open(f, 'r', encoding='utf8', newline='') as csvfile:
                # create a csv reader object
                csvreader = csv.reader(csvfile)
                next(csvreader)
                # extracting each data row and append it
                for line in csvreader:
                    rows.append(line)

        return rows

    def __write_target(self, rows):
        """
        Create a single target CSV file from the data rows.

        Args:

        `rows`: The list of CSV data rows.

        Returns:

        `target`: The absolute path of the target file.
        """
        # file used to insert data into the Apache Cassandra tables
        target = os.path.abspath(f'{os.getcwd()}/{self.target}')

        # configure a csv writer object
        csv.register_dialect(
            'myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
        # create a csv writer object
        with open(target, 'w', encoding='utf8', newline='') as f:
            # write the header row
            writer = csv.writer(f, dialect='myDialect')
            writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName',
                            'length', 'level', 'location', 'sessionId', 'song', 'userId'])
            # write each data row
            for row in rows:
                # ignore all rows where the artist is empty
                if (row[0] == ''):
                    continue
                writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6],
                                row[7], row[8], row[12], row[13], row[16]))

        # returns the absolute target file path
        return target

    def extract(self):
        """
        Process all the event_data CSV files and write to a single target CSV file.
        """
        # read all csv files in 'data/event_data' and return a list of data rows
        rows = self.__get_rows()

        # write a single csv file from the data rows
        target = self.__write_target(rows)

        # print the number of rows in the source files
        print(f'INFO: Rows read from the source CSV: {len(rows)}')

        # print the number of rows in the target file
        with open(target, 'r', encoding='utf8') as f:
            print(f'INFO: Rows written in target CSV: {sum(1 for line in f)}')

        print('INFO: Source files were filtered to remove rows with empty artist names.')
