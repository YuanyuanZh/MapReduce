import sys
import mr_classes
import input_split

class Engine(object):

    def __init__(self, input_file, split_size, class_name, num_reducers,output_base):
        self.input_file = input_file
        self.split_size = int(split_size)
        self.class_name = class_name
        self.num_reducer = int(num_reducers)
        self.result_list = None
        self.output_base = output_base

    def create_map_instance(self,class_name):
        pass

    def create_reduce_instance(self,class_name):
        pass

    def read_input(self,file_object,split_dict):
        input = []
        for offset in split_dict.keys():
            file_object.seek(offset)
            content = file_object.read(split_dict[offset])
            input.append(content)
        return input

    def execute(self):
        pass

class WordCountEngine(Engine):

    def create_map_instance(self,class_name):
        return mr_classes.WordCountMap()

    def create_reduce_instance(self,class_name):
        return mr_classes.WordCountReduce()

    def execute(self):
        split_hashmap = input_split.Split(self.input_file,self.split_size,self.class_name).generate_split_info()
        # Map phase
        mapper = self.create_map_instance(self.class_name)
        file_object = open(self.input_file)
        input = self.read_input(file_object,split_hashmap)
        for j, v in enumerate(input):
                mapper.map(j, v)

        # Sort intermediate keys
        table = mapper.get_table()
        keys = table.keys()
        keys.sort()

        #partion
        job_for_reduces= mapper.partition(keys,self.num_reducer)

        #Reduce phase
        for i in range(len(job_for_reduces)):
            reducer = self.create_reduce_instance(self.class_name)
            keys = job_for_reduces[i].keys()
            keys.sort()
            for k in keys:
                reducer.reduce(k, job_for_reduces[i][k])
            reducer.write_result(self.output_base,i)

class SortEngine(Engine):

    def create_map_instance(self,class_name):
        return mr_classes.SortMap()

    def create_reduce_instance(self,class_name):
        return mr_classes.SortReduce()

    def execute(self):
        split_hashmap = input_split.Split(self.input_file,self.split_size,self.class_name).generate_split_info()
        # Map phase
        mapper = self.create_map_instance(self.class_name)
        file_object = open(self.input_file)
        input = self.read_input(file_object,split_hashmap)
        for j, v in enumerate(input):
                mapper.map(j, v)

        # Sort intermediate keys
        table = mapper.get_table()
        keys = table.keys()
        keys.sort()

        #partion
        job_for_reduces= mapper.partition(keys,self.num_reducer)

        #Reduce phase
        for i in range(len(job_for_reduces)):
            reducer = self.create_reduce_instance(self.class_name)
            reducer.reduce(i,job_for_reduces[i])
            reducer.write_result(self.output_base,i)

class HammingEngine(Engine):

    def create_map_instance(self,class_name):
        if class_name == "hammingEnc":
            return mr_classes.hammingEncMap()
        elif class_name == "hammingDec":
            return mr_classes.hammingDecMap()
        elif class_name == "hammingFix":
            return mr_classes.hammingFixMap()

    def create_reduce_instance(self,class_name):
        return mr_classes.hammingReduce()

    def execute(self):
        split_hashmap = input_split.Split(self.input_file,self.split_size,self.class_name).generate_split_info()
        # Map phase
        mapper = self.create_map_instance(self.class_name)
        file_object = open(self.input_file)
        input = self.read_input(file_object,split_hashmap)
        for j, v in enumerate(input):
                mapper.map(j, v)

        # Sort intermediate keys
        table = mapper.get_table()
        keys = table.keys()
        keys.sort()

        #partion
        job_for_reduces= mapper.partition(keys,self.num_reducer)

        #Reduce phase
        for i in range(len(job_for_reduces)):
            reducer = self.create_reduce_instance(self.class_name)
            keys = job_for_reduces[i].keys()
            keys.sort()
            for k in keys:
                reducer.reduce(k, job_for_reduces[i][k])
            reducer.write_result(self.output_base,i)


if __name__ == '__main__':
    class_name = sys.argv[1]
    split_size = sys.argv[2]
    num_reducer = sys.argv[3]
    in_filename = sys.argv[4]
    output_base = sys.argv[5]

    if class_name == 'WordCount':
        engine = WordCountEngine(in_filename, split_size,class_name, num_reducer, output_base)
    elif class_name == 'Sort':
        engine = SortEngine(in_filename, split_size,class_name, num_reducer, output_base)
    else:
        engine = HammingEngine(in_filename, split_size,class_name, num_reducer, output_base)
    engine.execute()
    