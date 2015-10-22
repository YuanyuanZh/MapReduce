import sys
import mr_classes
import input_split
import collect_data

class Engine(object):

    def __init__(self, input_file, split_size, class_name, num_reducers,output_base):
        self.input_file = input_file
        self.split_size = int(split_size)
        self.class_name = class_name
        self.num_reducer = int(num_reducers)
        self.output_base = output_base

    def read_input(self,file_object,split_dict):

        offset = split_dict.keys()[0]
        size = split_dict.get(offset)
        file_object.seek(offset)
        input = []
        pos = file_object.tell()
        while pos < offset+size:
            line = file_object.readline()
            input.append(line)
            pos = file_object.tell()
        return input

    def execute(self):
        pass

class WordCountEngine(Engine):

    def create_map_instance(self,split_id):
        return mr_classes.WordCountMap(split_id)

    def create_reduce_instance(self):
        return mr_classes.WordCountReduce()

    def execute(self):
        split_hashmap = input_split.Split(self.input_file,self.split_size,self.class_name).generate_split_info()
        job_for_reduces = {}
        num_maps = split_hashmap.keys()
        for i in num_maps:
            mapper = self.create_map_instance(i)
            file_object = open(self.input_file)
            input = self.read_input(file_object,split_hashmap[i])
            for j, v in enumerate(input):
                mapper.map(j, v)

            table = mapper.get_table()
            keys = table.keys()
            keys.sort()

            job_for_reduces[i]= mapper.partition(keys,self.num_reducer)

        for j in range(int(self.num_reducer)):
            reducer = self.create_reduce_instance()
            reducer.set_output_oder(j)
            jobs = {}
            collect = {}
            for k in num_maps:
                index = str(k)+str(j)
                jobs[index]=(job_for_reduces.get(k)).get(index)

            for key in jobs.keys():
                keys = jobs[key].keys()
                for k in keys:
                    if k in collect:
                        collect[k].append(jobs[key][k][0])
                    else:
                        collect[k]= jobs[key][k]
            keys = collect.keys()
            keys.sort()
            for k in keys:
                reducer.reduce(j,k,collect[k])
            reducer.write_Jason_result(self.output_base)

class SortEngine(Engine):

    def create_map_instance(self,split_id):
        return mr_classes.SortMap(split_id)

    def create_reduce_instance(self):
        return mr_classes.SortReduce()

    def execute(self):
        split_hashmap = input_split.Split(self.input_file,self.split_size,self.class_name).generate_split_info()
        job_for_reduces = {}
        num_maps = split_hashmap.keys()
        for i in num_maps:
            mapper = self.create_map_instance(i)
            file_object = open(self.input_file)
            input = self.read_input(file_object,split_hashmap[i])
            for j, v in enumerate(input):
                mapper.map(j, v)

            table = mapper.get_table()
            keys = table.keys()
            keys.sort()

            job_for_reduces[i]= mapper.partition(keys,self.num_reducer)

        for j in range(int(self.num_reducer)):
            reducer = self.create_reduce_instance()
            reducer.set_output_oder(j)
            jobs = {}
            collect = {}
            for k in num_maps:
                index = str(k)+str(j)
                jobs[index]=(job_for_reduces.get(k)).get(index)

            for key in jobs.keys():
                if j in collect:
                    collect[j] += jobs[key]
                else:
                    collect[j] = jobs[key]

            reducer.reduce(j,collect[j])
            reducer.write_Jason_result(self.output_base)

class HammingEngine(Engine):

    def create_map_instance(self,class_name,split_id):
        if class_name == "hammingEnc":
            return mr_classes.hammingEncMap(split_id)
        elif class_name == "hammingDec":
            return mr_classes.hammingDecMap(split_id)
        elif class_name == "hammingFix":
            return mr_classes.hammingFixMap(split_id)
        elif class_name == "hammingChk":
            return mr_classes.hammingChkMap(split_id)
        elif class_name == 'hammingErr':
            return mr_classes.hammingErrMap(split_id)

    def create_reduce_instance(self):
        return mr_classes.hammingReduce()

    def read_input(self,file_object,split_dict):
        offset = split_dict.keys()[0]
        size = split_dict.get(offset)
        file_object.seek(offset)
        input = file_object.read(size)
        return input

    def execute(self):
        split_hashmap = input_split.Split(self.input_file,self.split_size,self.class_name).generate_split_info()
        job_for_reduces = {}
        num_maps = split_hashmap.keys()
        for i in num_maps:
            mapper = self.create_map_instance(self.class_name,i)
            file_object = open(self.input_file)
            input = self.read_input(file_object,split_hashmap[i])
            if self.class_name == 'hammingErr':
                self.set_err_operation(i, mapper, split_hashmap)

            mapper.map(i, input)

            table = mapper.get_table()
            keys = table.keys()
            keys.sort()

            job_for_reduces[i]= mapper.partition(keys,self.num_reducer)

        for j in range(int(self.num_reducer)):
            reducer = self.create_reduce_instance()
            jobs = {}
            for k in num_maps:
                index = str(k)+str(j)
                jobs[index]=(job_for_reduces.get(k)).get(index)
            reducer.set_output_oder(j)
            for i in jobs.keys():
                reducer.reduce(i,jobs.get(i))
            reducer.write_Jason_result(self.output_base)

    def set_err_operation(self, i, mapper, split_hashmap):
        if int(split_hashmap[i].keys()[0] + split_hashmap[i].get(split_hashmap[i].keys()[0])) >= int(sys.argv[6]):
            mapper.set_err_pos(int(sys.argv[6]) - split_hashmap[i].keys()[0])


if __name__ == '__main__':
    class_name = sys.argv[1]
    split_size = sys.argv[2]
    num_reducer = sys.argv[3]
    in_filename = "./test/"+sys.argv[4]
    output_base = "./testoutput/"+sys.argv[5]

    if class_name == 'WordCount':
        engine = WordCountEngine(in_filename, split_size,class_name, num_reducer, output_base)
    elif class_name == 'Sort':
        engine = SortEngine(in_filename, split_size,class_name, num_reducer, output_base)
    else:
        engine = HammingEngine(in_filename, split_size,class_name, num_reducer, output_base)
    engine.execute()

    collector = collect_data.Collect_data(sys.argv[5],class_name+'_result')
    collector.collect()
