import sys
import mr_classes

class Engine(object):

    def __init__(self, input_file, split_size, class_name, num_reducers,output_base):
        self.input_file = input_file
        self.split_size = int(split_size)
        self.class_name = class_name
        self.num_reducer = int(num_reducers)
        self.result_list = None
        self.output_base = output_base

    def create_map_instance(self,class_name):
        if class_name == 'WordCount':
            return mr_classes.WordCountMap()
        elif class_name == "hammingEnc":
            return mr_classes.hammingEncMap()
        elif class_name == "hammingDec":
            return mr_classes.hammingDecMap()
        elif class_name == "hammingFix":
            return mr_classes.hammingFixMap()

    def create_reduce_instance(self,class_name):
        if class_name == 'WordCount':
            return mr_classes.WordCountReduce()
        else:
            return mr_classes.hammingReduce()

    def adjust_offset(self,file_object,start,end):

        newpos = start
        file_object.seek(start,0)
        while newpos < end and newpos < self.split_size-1 + start:
            file_object.readline()
            newpos = file_object.tell()
        return newpos

    def generate_split_info(self,file_object):

        file_object.seek(0, 2)
        end = file_object.tell()
        split_map = {}
        offset = 0
        if self.class_name == "hammingEnc":
            while offset < end:
                split_map[offset] = self.split_size
                offset = offset+ self.split_size
        elif self.class_name == "hammingDec":
            while (self.split_size *8)%12 != 0:
                self.split_size = self.split_size + 1
            while offset <end:
                prev = offset
                offset = self.adjust_offset(file_object, offset, end)
                split_map[prev] = offset - prev
        else:
            while offset < end:
                prev = offset
                offset = self.adjust_offset(file_object, offset, end)
                split_map[prev] = offset - prev
        return split_map

    def read_input(self,file_object,split_dict):
        input = []
        for offset in split_dict.keys():
            file_object.seek(offset)
            content = file_object.read(split_dict[offset])
            input.append(content)
        return input

    def partion(self, keys, table):
        job_for_reduces = {}
        num_reducer = self.num_reducer
        num_map_result = len(keys)
        for i in range(num_map_result):
            hashkey = i % num_reducer
            if hashkey in job_for_reduces:
                job_for_reduces[hashkey][keys[i]] = table[keys[i]]
            else:
                job_for_reduces[hashkey] = {keys[i]: table[keys[i]]}

        return job_for_reduces

    def execute(self):
        # Map phase
        mapper = self.create_map_instance(self.class_name)
        file_object = open(self.input_file)
        split_hashmap = self.generate_split_info(file_object)
        input = self.read_input(file_object,split_hashmap)
        for j, v in enumerate(input):
                mapper.map(j, v)

        # Sort intermediate keys
        table = mapper.get_table()
        keys = table.keys()
        keys.sort()

        #partion
        job_for_reduces= self.partion(keys, table)

        #Reduce phase
        for i in range(len(job_for_reduces)):
            reducer = self.create_reduce_instance(self.class_name)
            keys = job_for_reduces[i].keys()
            keys.sort()
            for k in keys:
                reducer.reduce(k, job_for_reduces[i][k])
            self.result_list = reducer.get_result_list()

            out_file = open(self.output_base+"_"+str(i),'w')
            out = ''
            if self.class_name == "WordCount":
                out = str(self.result_list)
            else:
                for i in self.result_list:
                    out += str(i)
            out_file.write(out)

if __name__ == '__main__':
    class_name = sys.argv[1]
    split_size = sys.argv[2]
    num_reducer = sys.argv[3]
    in_filename = sys.argv[4]
    output_base = sys.argv[5]
    engine = Engine(in_filename, split_size,class_name, num_reducer, output_base)
    engine.execute()
