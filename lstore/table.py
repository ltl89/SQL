from cgi import print_directory
from lstore.index import Index
from time import time
from lstore.config import *
from lstore.page import PageRange
import threading

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
# still has page range logical problem

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns # <List>

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, path, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {} # len8str-rid to basepage
        self.index = Index(self)
        # store index of column that has index[]
        self.pageRange = [] # will be moved to bufferpool
        self.pageRangeCount = 0
        self.brid = 0
        self.trid = 0
        self.path = path # './ECS165'

        self.readLock = []
        self.readLockTransaction = []
        self.writeLock = []

        self.threadid = []
        self.ownedRID = []
        # self.writeLockTransaction = []

        # FOR NOW we ASSUMER rid < 9999999
    def thread_ownsRID(self, RID):
        if threading.current_thread().getName() in self.threadid:
            for i, _id in enumerate(self.threadid):
                if threading.current_thread().getName() == _id:
                    if RID not in self.ownedRID[i]:
                        self.ownedRID[i].append(RID)
                    return True
        else:
            self.threadid.append(threading.current_thread().getName())
            self.ownedRID.append([RID])
            return True

    def threadEnd(self):
        if threading.current_thread().getName() in self.threadid:
            for i, _id in enumerate(self.threadid):
                if threading.current_thread().getName() == _id:
                    for RID in self.ownedRID[i]:
                        self.releaseLock(RID)
                    self.threadid.pop(i)
                    self.ownedRID.pop(i)
        return True

    def acquireRead(self, RID):
        if RID in self.writeLock:
            return False
        if RID in self.readLock:
            for i,_RID in enumerate(self.readLock):
                if _RID == RID:
                    self.readLockTransaction[i].append(threading.current_thread().getName())
                    self.thread_ownsRID(RID)
                    return True
        else:
            self.readLock.append(RID)
            self.readLockTransaction.append([threading.current_thread().getName()])
            self.thread_ownsRID(RID)
            return True

    def acquireWrite(self, RID):
        if RID in self.writeLock:
            return False
        if RID in self.readLock:
            for i,_RID in enumerate(self.readLock):
                if _RID == RID:
                    if len(self.readLockTransaction[i]) == 1 and threading.current_thread().getName() in self.readLockTransaction[i]:
                        self.readLock.pop(i)
                        self.readLockTransaction.pop(i)
                    else:
                        return False
        self.writeLock.append(RID)
        self.thread_ownsRID(RID)
        return True

    def releaseLock(self, RID):
        # if this is a write lock
        if RID in self.writeLock:
            self.writeLock.remove(RID)
            return True
        # then this is a read lock
        for i,_RID in enumerate(self.readLock):
            if _RID == RID:
                if len(self.readLockTransaction[i]) == 1:
                    self.readLock.pop(i)
                    self.readLockTransaction.pop(i)
                    return True
                else:
                    self.readLockTransaction[i].remove(threading.current_thread().getName())
                    return True
        return False





    def insert(self, schema_encoding, columns):
        # we had to do 4 thing in insert()
        # 0. check page status.
        # 1. assign rid
        # 2. write into basepage
        # 2.1 metadatas (4 of them)
        # 2.2 all columns
        # 3. update pagedirectory
        # 4. insert in index
        ## 0
        pageRangeID = self.prewrite(True)
        ## 1
        ## 2 is in assign_base_rid
        cur_rid = self.assign_base_rid()
        ## 3
        value_list = []
        value_list.append(cur_rid)
        value_list.append(RID_NONE) # special case for None RID
        value_list.append(schema_encoding)
        value_list.append(int(time()))
        for i in columns:
            value_list.append(i)
        info = self.pageRange[pageRangeID].write(True, value_list)
        self.page_directory[cur_rid] = info

        ## 4 insert index
        for i in range(self.num_columns):
            if self.index.indices[i] is not None:
                if columns[i] not in self.index.indices[i]:
                    self.index.indices[i][columns[i]] = [cur_rid]
                else:
                    self.index.indices[i][columns[i]] += [cur_rid]
        return True
        # the 4 Pages are for : rid - indirection - schema_encoding - time()  . and then  ori value

    def assign_base_rid(self):
        len_7_brid = str(self.brid)
        while len(len_7_brid) != 7:
            len_7_brid = '0' + len_7_brid
        next_base_rid = 'b' + len_7_brid
        #######//end of assigning rid
        # done with page_directory
        self.brid += 1
        return next_base_rid

    def assign_tail_rid(self):
        len_7_trid = str(self.trid)
        while len(len_7_trid) != 7:
            len_7_trid = '0' + len_7_trid
        next_tail_rid = 't'+ len_7_trid

        self.trid += 1
        return next_tail_rid

    def prewrite(self, base_or_tail, baseRID=''):
        '''
        function: make sure everything is good to write a new data into table
        @ parameter
        base_or_tail : True for insert base, False for insert tail
        @ return
        pageRageID
        '''
        if not self.pageRange:
            # if page range is empty, we add one
            self.pageRange.append(PageRange(self.path, self.name, self.pageRangeCount))
            self.pageRangeCount += 1
        # check if it has capacity
        if base_or_tail:
            # TODO, upgrade to bufferpool first, and then insert somewhere else.
            if self.pageRange[self.pageRangeCount-1].isFull():
                self.pageRange.append(PageRange(self.path, self.name, self.pageRangeCount))
                self.pageRangeCount += 1
            return self.pageRangeCount - 1
        else:
            return self.page_directory[baseRID][0]

    def get_column_value(self, rid, col_num):
        '''given rid and column number, get one column value, using page_directory'''
        PRID = self.page_directory[rid][0]
        inPRIndex = self.page_directory[rid][1]
        inPageIndex = self.page_directory[rid][2]
        return self.pageRange[PRID].get_column_value(inPRIndex,inPageIndex,col_num)

    def get_datadata(self, rid, requested_col):
        result = []
        schema = self.get_column_value(rid, 2)
        tailRID = self.get_column_value(rid, 1)
        for logical_col_num in requested_col:
            col_num = logical_col_num+4
            if schema[logical_col_num] == '0':
                result.append(self.get_column_value(rid, col_num))
            else:
                result.append(self.get_column_value(tailRID, col_num))
        return result



    def select_req(self, index_value, index_column, query_columns,inT=False):
        '''  Returns a list of Record objects upon success
        # Returns False if record locked by TPL
        # Read a record with specified key

        # :param index_value: the value of index you want to search
        # :param index_column: the column number of index you want to search based on
        # :param query_columns<List>: what columns to return. array of 1 or 0 values.
        '''
        ## step 1 goto tree, get rids into a list.
        ## step 2
        ## create Record class and put them in a list to return back to query.py
        if not self.index.indices[index_column]:
            print('table.py :: index not exist')
            return False
        if index_value in self.index.indices[index_column]:
            rid_list = self.index.indices[index_column][index_value]
        else:
            return False
        column_selected = [ i for i in range(len(query_columns)) if query_columns[i]==1 ]
        # for each rid, get key, get column needed:
        ## for each column selected check if updated:
        ### get new value by indirection if updated
        ### or get base value.
        # create a Record and stored for returning in list
        ans = []
        for query_rid in rid_list:
            if inT:
                get_lock = self.acquireRead(query_rid)
                if get_lock == False:
                    return False
            if query_rid not in self.page_directory:
                continue
            col_collection = self.get_datadata(query_rid, column_selected)
            ans.append(Record(query_rid,index_value,col_collection))
        return ans

    def sum_req(self, start_range, end_range, aggregate_column_index):
        rid_list = []
        for i in self.index.indices[self.key].itervalues(start_range, end_range):
            for j in i:
                rid_list.append(j)
        # use rid_list , sum up and then return.
        ans = 0
        for rid in rid_list:
            ans += self.get_datadata(rid, [aggregate_column_index])[0]
        return ans

    def delete_req(self, rid_delete_list):
        for rid_delete in rid_delete_list:
            get_lock = self.acquireWrite(rid_delete)
            if get_lock == False:
                return False
            ## get data columns
            data_column = []
            for col_num in range(self.num_columns):
                data_column.append(self.get_column_value(rid_delete, 4+col_num))
            ## delete from page_directory
            if rid_delete in self.page_directory:
                del self.page_directory[rid_delete]
            ## delete all its index.
            for i in range(self.num_columns):
                if self.index.indices[i] is not None:
                    if data_column[i] in self.index.indices[i]:
                        len_temp = len(self.index.indices[i][data_column[i]])
                        if len_temp == 1:
                            del self.index.indices[i][data_column[i]]
                        elif len_temp >1:
                            self.index.indices[i][data_column[i]].remove(rid_delete)
                        elif len_temp == 0:
                            # somethings wrong
                            pass

    def update(self, record_list, schema_encoding, columns):
        # for each rid
        #1 prewrite
        #2 get values ready
        #3 wrtie data
        #4 update base record indirection col
        #5 update index
        # for each rid
        for kk in range(len(record_list)): # kk is only used in the next 2 line
            base_rid = record_list[kk].rid
            last_columns = record_list[kk].columns
            '''
            get_lock = self.acquireWrite(base_rid)
            if get_lock == False:
                return False
            '''
        #1 prewrite
            pageRangeID = self.prewrite(False, base_rid)
        #2 get values ready
            ## writing data to page, base , and tail.
            tail_rid = self.assign_tail_rid() # write this in base column(1).
            last_tail_rid = self.get_column_value(base_rid, 1)# 1 is indirection.
            last_schema = self.get_column_value(base_rid, 2)# 2 is schema.
            value_list = []
            value_list.append(tail_rid)
            value_list.append(last_tail_rid) # could be None RID if base was not changed before
            ####### processing new_schema and new_columns
            new_schema = ''
            new_columns = []
            for i in range(len(schema_encoding)):
                if schema_encoding[i] == '1':
                    new_schema += '1'
                    new_columns.append(columns[i])
                elif last_schema[i] == '1':
                    new_schema += '1'
                    new_columns.append(last_columns[i])
                else:
                    new_schema += '0'
                    new_columns.append(None)
            ####### processing done
            value_list.append(new_schema) #  write this into base column(2) as well
            value_list.append(int(time()))
            for i in new_columns:
                value_list.append(i)
        # at this point value_list is well packed
        #3 wrtie data
        info = self.pageRange[pageRangeID].write(False, value_list)
        self.page_directory[tail_rid] = info

        #4 update base record indirection col
            # do it here, not in PageRange.
        info = self.page_directory[base_rid]
            # update indirection
        self.pageRange[info[0]].base[info[1]][1].random_write(info[2], tail_rid)
            # update schema encoding
        self.pageRange[info[0]].base[info[1]][2].random_write(info[2], new_schema)
        #5 update index
        for i in range(len(new_columns)): #[column].insert(value, rid)
                if self.index.indices[i]:
                # this index existes.
                    if schema_encoding[i] == '1':
                        # this index need to be updated
                        self.index.indices[i].remove(last_columns[i])
                        self.index.indices[i].insert(columns[i], base_rid)

    def __merge(self):
        print("merge is happening")
        pass


