import hamming
from json import dumps

class Map(object):

    def __init__(self,split_id):
        self.table = {}
        self.split_id = split_id

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

    def get_split_id(self):
        return self.split_id


class Reduce(object):

    def __init__(self):
        self.result_list = {}
        self.output_order = None

    def emit(self, pm_order,v):
        if pm_order in self.result_list:
            self.result_list[pm_order].append(v)
        else:
            self.result_list[pm_order] = [v]

    def get_result_list(self):
        return self.result_list

    def set_output_oder(self,pm_num):
        self.output_order = pm_num

    def write_Jason_result(self,output_base):
        rst = self.get_result_list()
        out_file_name = output_base+"_"+"Jason"+"_"+str(self.output_order)
        with open(out_file_name, "w") as file:
            dumps(rst, file, indent=4)
        file.close()

class WordCountMap(Map):

    def map(self, k, v):
        words = v.split()
        for w in words:
            self.emit(w, '1')

    def partition(self, keys,nr):
        job_for_reduces = {}
        pos = []
        alphabet = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
        split_order = int(self.get_split_id())
        div_unit = len(alphabet)/nr
        while div_unit <= 26:
            pos.append(div_unit-1)
            div_unit = div_unit + div_unit
        for i in range(len(keys)):
            key_str = keys[i]
            for j in range(len(pos)):
                if (key_str[0]).lower() < alphabet[pos[j]]:
                    if split_order*10+j in job_for_reduces:
                        job_for_reduces[split_order*10+j][keys[i]] = self.table[keys[i]]
                        break
                    else:
                        job_for_reduces[split_order*10+j] = {keys[i]: self.table[keys[i]]}
                        break
        return job_for_reduces

class WordCountReduce(Reduce):

    def reduce(self, pm_order,k, vlist):
        count = 0
        for v in vlist:
            count = count + int(v)
        self.emit(pm_order,k + ':' + str(count))

    def write_txt_result(self,output_base):
        rst = self.get_result_list()
        out_file = open(output_base+"_"+str(self.output_order),'w')
        out = rst.values()
        out_file.write(str(out))
        out_file.close()

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
        split_id = int(self.get_split_id())
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
                        if split_id*10+j in job_for_reduces:
                            job_for_reduces[split_id*10+j].append(w)
                            break
                        else:
                            job_for_reduces[split_id*10+j] = [w]
                            break
        return job_for_reduces

class SortReduce(Reduce):

    def reduce(self, k, vlist):
        vlist.sort()
        self.emit(k,vlist)

    def write_result(self,output_base):
        reduce_serial = self.output_order
        rst = self.get_result_list()
        out_file = open(output_base+"_"+str(reduce_serial),'w')
        out = ''
        for i in rst.values():
            out += str(i)
        out_file.write(out)

    def write_txt_result(self,output_base):
        rst = self.get_result_list()
        out_file = open(output_base+"_"+str(self.output_order),'w')
        out = rst.values()
        out_file.write(str(out))
        out_file.close()

class ham(Map):

    def partition(self,keys,nr):
        job_for_reduces = {}
        num_reducer = nr
        value_list = self.table.get(keys[0])[0]
        start = 0
        count = 0
        l = len(value_list)
        div_unit = len(value_list)/num_reducer
        while start < len(value_list):
            if count < num_reducer-1:
                end = start + div_unit
            else:
                end = len(value_list)
            job_for_reduces[self.split_id*10+count] = value_list[start:end]
            start = end
            count = count + 1

        return job_for_reduces

class hammingEncMap(ham):

    def map(self, k, v):
        enc = hamming.HammingEncoder()
        self.emit(k, enc.encode(v))

class hammingDecMap(ham):
    def map(self, k, v):
        dec = hamming.HammingDecoder()
        self.emit(k, dec.decode(v))

class hammingFixMap(ham):
    def map(self, k, v):
        fixer = hamming.HammingFixer()
        self.emit(k, fixer.fix(v))

class hammingReduce(Reduce):

    def reduce(self, k, vlist):
        self.emit(k,vlist)

    def write_text_result(self,output_base):
        rst = self.get_result_list()
        out_file = open(output_base+"_"+str(self.output_order),'w')
        out_file.write(str(rst))
