
# from lstore.BPlusTree import BplusTree
from BTrees.IOBTree import IOBTree
"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""


class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.create_index(table.key)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        '''return list {of base rid} by searching value on indeces[column].
            return False when not found'''
        if value in self.indices[column]:
            return self.indices[column][value]
        return False

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass
        # not goting to be used at all

    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        '''This func creates a BPlusTree order of 50 as index tree for column_number'''
        if self.indices[column_number] is None:
            self.indices[column_number] = IOBTree(order=50)
            return True
        else:
            return False

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
