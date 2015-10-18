import hamming

class Map(object):

    def __init__(self):
        self.table = {}

    def map(self, k, v):
        pass

    def emit(self, k, v):
        if k in self.table:
            self.table[k].append(v)
        else:
            self.table[k] = [v]

    def get_table(self):
        return self.table


class Reduce(object):

    def __init__(self):
        self.result_list = []

    def reduce(self, k, vlist):
       pass

    def emit(self, v):
        self.result_list.append(v)

    def get_result_list(self):
        return self.result_list

class WordCountMap(Map):

    def map(self, k, v):
        words = v.split()
        for w in words:
            self.emit(w, '1')

class WordCountReduce(Reduce):

    def reduce(self, k, vlist):
        count = 0
        for v in vlist:
            count = count + int(v)
        self.emit(k + ':' + str(count))

class hammingEncMap(Map):

    def map(self, k, v):
        enc = hamming.HammingEncoder()
        self.emit(k, enc.encode(v))

class hammingReduce(Reduce):

    def reduce(self, k, vlist):
        for v in vlist:
            self.emit(v)

class hammingDecMap(Map):
    def map(self, k, v):
        dec = hamming.HammingDecoder()
        self.emit(k, dec.decode(v))

class hammingFixMap(Map):
    def map(self, k, v):
        fixer = hamming.HammingFixer()
        self.emit(k, fixer.fix(v))