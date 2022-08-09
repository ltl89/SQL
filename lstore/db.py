from lstore.table import Table
from lstore.BPlusTree import BplusTree
import os


class Log:
    def __init__(self) -> None:
        self.actList = []
        self.RIDList = []
        self.recordList = []

class Database():

    def __init__(self):
        self.tables = []
        self.name2index = {}
        self.path = None

    # Not required for milestone1
    def open(self, path):
        '''
        path/db.txt : db info
        if file above does not exist, create one, and create init a db for user.
        path/table_[tablename].txt : table [tablename]'s info
        path/pr_[tablename]_[somenumber].txt : data of pagerange.
        self.path = path
        isExist = os.path.exists(path)
        if not isExist:
            os.makedirs(path)
        '''
        pass


    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        if self.get_table(name) != False:
            return False
        table = Table(self.path, name, num_columns, key_index)
        self.name2index[name] = len(self.tables)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name not in self.name2index:
            return False
        index_to_be_drop = self.name2index[name]
        del self.tables[index_to_be_drop]
        del self.name2index[name]
        return True


    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        if name not in self.name2index:
            return False
        index_get = self.name2index[name]
        return self.tables[index_get]