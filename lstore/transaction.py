from lstore.table import Table, Record
from lstore.index import Index
import threading
class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        tableInvovled = []
        for query, table, args in self.queries:
            if query.__name__ == 'select':
                args = list(args)# arg =( key, 0, [1, 1, 1, 1, 1])
                args.append(True)
            result = query(*args)
            if table not in tableInvovled:
                tableInvovled.append(table)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort(tableInvovled)
        return self.commit(tableInvovled)

    def abort(self,tableInvovled):
        #TODO: do roll-back and any other necessary operations
        for table in tableInvovled:
            table.threadEnd()
            print('aaaaaabort')
        return False

    def commit(self,tableInvovled):
        # TODO: commit to database
        for table in tableInvovled:
            table.threadEnd()
        return True

