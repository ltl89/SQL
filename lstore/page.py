
# record (rid, indirection, one of []) => 3 int
from lstore.config import INTSIZE, RECORDSIZE

class PageRange:
    # I want each file(PageRange) to be 256 K,
    # contains 9 set of page, for each set of page,
    # 7 set total(7*9 * 4 = 63 * 4 = 252 K),
    # 4K left for storing PageRange info. (I want to store this part at the beginning of the file)
    def __init__(self, path, tableName, pageRangeID):
        self.base = []
        tmp = []
        for i in range(9):
            tmp.append(Page())
        self.base.append(tmp)
        self.tail = []
        tmp = []
        for i in range(9):
            tmp.append(Page())
        self.tail.append(tmp)
        self.numberOfBase = 4
        self.maxRecord = 4096 // RECORDSIZE
        self.TPS = 0
        self.writeAge = 0 # used for periodically merge e.g. every 1000 write .
        self.pageID = 0
        self.pageRangeID = pageRangeID
        self.fname = None
        self.prefix = None
        # self.createFile(path, tableName, pageRangeID)

    def goLocal(self):
        for i in range(len(self.base)):
            for j in range(9):
                fn = str(self.pageRangeID) + '_' + str(i) + '_' + str(j) + '.txt'
                with open(fn,'ab') as f:
                    f.write(self.base[i][j].data)

        for i in range(len(self.tail)):
            for j in range(9):
                fn =  str(self.pageRangeID) + '_' + str(i) + '_' + str(j) + 'tail.txt'
                with open(fn,'ab') as f:
                    f.write(self.tail[i][j].data)
        pass

    def isFull(self):
        ''' return True when full'''
        if len(self.base) == 4:
            if self.base[3][0].has_capacity() == False:
                return True
        return False
    '''
    def createFile(self ,path, tableName, pageRangeID):
        filename = path+'/pagerange_' + tableName+'_'+ str(pageRangeID)+'.txt'
        self.fname = filename
        self.prefix = path + '/' + tableName + '_' + str(pageRangeID)
        with open(filename, "ab+") as f:
            f.write(bytearray(4096))
    '''
    def write(self, base_or_tail, valueToInsert):
        '''
        function: Write a data in.
        @ parameters
        base_or_tail : True for write base, False for write tail
        valueToInsert : a list of value to insert to each page.

        @ Return
        [pagerangeid, set#, record#]

        file will be store as table1_[pageRangeID]_[set#]_[col#].txt
        '''
        inPRIndex = -1# should be 0,1,2,3
        inPageIndex = -1 # should be 4,5,6,7...
        if base_or_tail:
            if self.base[len(self.base) - 1][0].has_capacity():
                inPRIndex = len(self.base) - 1
                for i in range(9):
                    inPageIndex = self.base[len(self.base) - 1][i].write(valueToInsert[i])
            else:
                tmp = []
                for i in range(9):
                    tmp.append(Page())
                self.base.append(tmp)
                inPRIndex = len(self.base) - 1
                for i in range(9):
                    inPageIndex = self.base[len(self.base) - 1][i].write(valueToInsert[i])
        else:
            if self.tail[len(self.tail) - 1][0].has_capacity():
                inPRIndex = len(self.tail) - 1 + 4
                for i in range(9):
                    inPageIndex = self.tail[len(self.tail) - 1][i].write(valueToInsert[i])
            else:
                tmp = []
                for i in range(9):
                    tmp.append(Page())
                self.tail.append(tmp)
                inPRIndex = len(self.tail) - 1 + 4
                for i in range(9):
                    inPageIndex = self.tail[len(self.tail) - 1][i].write(valueToInsert[i])
        return [self.pageRangeID, inPRIndex, inPageIndex]
        '''
        choice = []
        if base_or_tail:
            choice = range(self.numberOfBase) #[0,1,2,3]
        else:
            choice = range(self.numberOfBase,len(self.pageSetSize))
        for i in choice:
            if self.pageSetSize[i] == self.maxRecord:
                continue
            if self.pageSetSize[i] == 0:
                # in this case, files are not setup yet
                for j in range(len(valueToInsert)):
                    self.pageSetId[i] = self.pageID
                    fname = self.prefix + '_' + str(self.pageSetId[i])+ '_' + str(j) + '.txt'
                    with open(fname, "ab") as f:
                        f.write(bytearray(4096))
                self.pageID += 1
            setNumber = self.pageSetId[i]
            for j in range(len(valueToInsert)):
                fname = self.prefix + '_' + str(self.pageSetId[setNumber])+ '_' + str(j) + '.txt'
                with open(fname, "rb+") as f:
                    offset = 8 * self.pageSetSize[i]
                    f.seek(offset)
                    value = valueToInsert[j]
                    if value is None:
                        continue
                    elif type(value) is int:
                        f.write(value.to_bytes(RECORDSIZE, byteorder='big'))
                    elif type(value) is str:
                        while len(value) < 8:
                            value += '0'
                        if len(value) > 8:
                            print('page.py :: you cant insert string len > 8')
                        f.write(bytes(value,'utf-8'))
                    else:
                        print('page.py :: type undefined inserted')
            result = [self.pageRangeID, self.pageSetId[setNumber], self.pageSetSize[i]]
            self.pageSetSize[setNumber] += 1
            return result
        else:
            pass
            return 'not prepared'
        '''
    def get_column_value(self, inPRIndex, inPageIndex, col_num):
        data = bytearray(8)
        if inPRIndex < 4: # base record
            data = self.base[inPRIndex][col_num].data[inPageIndex*8 : inPageIndex*8 + 8]
        else:
            data = self.tail[inPRIndex-4][col_num].data[inPageIndex*8 : inPageIndex*8 + 8]
        if col_num in [0,1,2]:
            return data.decode()
        else:
            return int.from_bytes(data, "big")

class Page:
    def __init__(self):
        self.data = bytearray(4096)
        self.max_load = 4096 // RECORDSIZE
        self.num_records = 0
        # we will write metadata at the end of self.data and decrease max_load by a
        # neccessary amount in the future.

    def has_capacity(self):
        return self.num_records < self.max_load

    def write(self, value):
        if not self.has_capacity():
            print("full!!!!page full!!!!", self.num_records)
            return -1
        start = self.num_records * RECORDSIZE
        if value is None:
            pass
        elif type(value) is int:
            self.data[start:start+RECORDSIZE] = value.to_bytes(RECORDSIZE,byteorder='big')
        elif type(value) is str:
            while len(value) < 8:
                value += '0'
            if len(value) > 8:
                print('you cannot insert string len > 8')
            self.data[start:start+RECORDSIZE] = bytes(value,'utf-8')
        else:
            print('type undefined in page.py')
            return -1
        self.num_records += 1
        return self.num_records - 1

    def random_write(self, num_records, value):
        start = num_records * RECORDSIZE
        if value is None:
            return 0
        elif type(value) is int:
            self.data[start:start+RECORDSIZE] = value.to_bytes(RECORDSIZE,byteorder='big')
        elif type(value) is str:
            while len(value) < 8:
                value += '0'
            if len(value) > 8:
                print('you cant insert string len > 8')
            self.data[start:start+RECORDSIZE] = bytes(value,'utf-8')
        else:
            print('type undefined in page.py')
            return -1
        return 0

