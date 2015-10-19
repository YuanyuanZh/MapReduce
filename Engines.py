import sys
import mr_classes
import input_split

class Engine(object):

    def __init__(self, input_file , num_reducers,output_base):
        self.input_file = input_file
        self.num_reducer = int(num_reducers)
        self.output_base = output_base

    def collect_jobs(self,job_list): #todo
        pass

    def create_map_instance(self,class_name):
        pass

    def create_reduce_instance(self,class_name):
        pass

    def read_input(self,file_object,split_dict):
        input = []
        values = split_dict.values()
        for v in values:
            for offset in v.keys():
                file_object.seek(offset)
                content = file_object.read(v[offset])
                input.append(content)
        return input


class WordCountEngine(Engine):

    def create_map_instance(self,split_order):
        return mr_classes.WordCountMap(split_order)

    def create_reduce_instance(self):
        return mr_classes.WordCountReduce()

    def WordCountMapExecute(self,assigned_split):

        #split_hashmap = input_split.Split(self.input_file,self.split_size,self.class_name).generate_split_info()
        mapper = self.create_map_instance(1) # todo need to replace 1 as split_# you get
        file_object = open(self.input_file)
        input = self.read_input(file_object,assigned_split)
        for j, v in enumerate(input):
                mapper.map(j, v)
        # Sort intermediate keys
        table = mapper.get_table()
        keys = table.keys()
        keys.sort()

        #partion
        job_for_reduces= mapper.partition(keys,self.num_reducer,mapper.get_split_order())

    def WordCountReduceExecute(self,job_list):

        job_for_reduces = self.collect_jobs(job_list)
        reducer = self.create_reduce_instance()
        reducer.set_output_oder((job_for_reduces.keys()[0])%10) # todo change (job_for_reduces.keys()[0]) to 编号
        for i in job_for_reduces.keys():
            keys = job_for_reduces[i].keys()
            keys.sort()
            for k in keys:
                reducer.reduce(i,k,job_for_reduces.get(i)[k])
        reducer.write_Jason_result(self.output_base)
        reducer.write_txt_result(self.output_base)