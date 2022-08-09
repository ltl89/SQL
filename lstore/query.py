from lstore.table import Table, Record
from lstore.index import Index
from time import time
from lstore.config import *

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """


    def delete(self, primary_key):
        # check if we had it
        key_col = self.table.key
        the_record = self.table.index.locate(0 , primary_key)
        if the_record == False:
            return False
        self.table.delete_req(the_record)
        return True
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        # check if we had it already
        key_col = self.table.key
        cur_insert_key = columns[key_col]
        if self.table.index.locate(key_col, cur_insert_key):
            return False
        # call table.insert()
        self.table.insert(schema_encoding, columns)
        return True

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, index_value, index_column, query_columns, inT=False):
        return self.table.select_req(index_value, index_column, query_columns, inT)
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        # check if we had it
        key_col = self.table.key
        record_list = self.select(primary_key, key_col, [1 for i in range(len(columns))])
        if not record_list:
            return False

        schema_encoding = ''
        for i in columns:
            if i is None:
                schema_encoding += '0'
            else:
                schema_encoding += '1'
        self.table.update(record_list, schema_encoding, columns)
        return True


    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        return self.table.sum_req(start_range, end_range, aggregate_column_index)

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
