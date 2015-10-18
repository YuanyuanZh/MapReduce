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

    def partition(self,keys,nr):
        pass

    def write_out(self,keys,table):
        rst = []
        for k in keys:
            rst.append(table[k])
        out = ''




class Reduce(object):

    def __init__(self):
        self.result_list = []

    def reduce(self, k, vlist):
       pass

    def emit(self, v):
        self.result_list.append(v)

    def get_result_list(self):
        return self.result_list

    def write_result(self,output_base,reduce_serial):
        pass

class WordCountMap(Map):

    def map(self, k, v):
        words = v.split()
        for w in words:
            self.emit(w, '1')

    def partition(self, keys,nr):
        job_for_reduces = {}
        pos = []
        alphabet = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
       # nr = self.num_reducer
        div_unit = len(alphabet)/nr
        while div_unit <= 26:
            pos.append(div_unit-1)
            div_unit = div_unit + div_unit
        for i in range(len(keys)):
            key_str = keys[i]
            for j in range(len(pos)):
                if (key_str[0]).lower() < alphabet[pos[j]]:
                    if j in job_for_reduces:
                        job_for_reduces[j][keys[i]] = self.table[keys[i]]
                        break
                    else:
                        job_for_reduces[j] = {keys[i]: self.table[keys[i]]}
                        break
        return job_for_reduces

class WordCountReduce(Reduce):

    def reduce(self, k, vlist):
        count = 0
        for v in vlist:
            count = count + int(v)
        self.emit(k + ':' + str(count))

    def write_result(self,output_base,reduce_serial):
        rst = self.get_result_list()
        out_file = open(output_base+"_"+str(reduce_serial),'w')
        out = str(rst)
        out_file.write(out)

class SortMap(Map):
    def map(self, k, v):
        words = v.split()
        words_list = []
        for w in words:
            words_list.append(w)
            words_list.sort()
        for s in words_list:
            self.emit(k, s)

    def partition(self,keys,nr):
        job_for_reduces = {}
        pos = []
        alphabet = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
        # nr = self.num_reducer
        div_unit = len(alphabet)/nr
        while div_unit <= 26:
            pos.append(div_unit-1)
            div_unit = div_unit + div_unit
        for i in range(len(keys)):
            word_list = self.table[keys[i]]
            for i in word_list:
                w = i
                for j in range(len(pos)):
                    if (w[0]).lower() < alphabet[pos[j]]:
                        if j in job_for_reduces:
                            (job_for_reduces[j]).append(w)
                            break
                        else:
                            job_for_reduces[j] = [w]
                            break
            #key_str = keys[i]
            # for j in range(len(pos)):
            #     if (key_str[0]).lower() < alphabet[pos[j]]:
            #         if j in job_for_reduces:
            #             job_for_reduces[j][keys[i]] = self.table[keys[i]]
            #             break
            #         else:
            #             job_for_reduces[j] = {keys[i]: self.table[keys[i]]}
            #             break
        return job_for_reduces

class SortReduce(Reduce):

    def collectMapResult(self):
        pass

    def reduce(self, k, vlist):
        vlist.sort()
        self.emit(vlist)

    def write_result(self,output_base,reduce_serial):
        rst = self.get_result_list()
        out_file = open(output_base+"_"+str(reduce_serial),'w')
        out = ''
        for i in rst:
            out += str(i)
        out_file.write(out)

class hammingEncMap(Map):

    def map(self, k, v):
        enc = hamming.HammingEncoder()
        self.emit(k, enc.encode(v))

class hammingDecMap(Map):
    def map(self, k, v):
        dec = hamming.HammingDecoder()
        self.emit(k, dec.decode(v))

class hammingFixMap(Map):
    def map(self, k, v):
        fixer = hamming.HammingFixer()
        self.emit(k, fixer.fix(v))

class hammingReduce(Reduce):

    def reduce(self, k, vlist):
        for v in vlist:
            self.emit(v)

    def write_result(self,output_base,reduce_serial):
        rst = self.get_result_list()
        out_file = open(output_base+"_"+str(reduce_serial),'w')
        out = ''
        for i in rst:
            out += str(i)
        out_file.write(out)
