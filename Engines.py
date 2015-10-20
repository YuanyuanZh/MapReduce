import mr_classes

class Engine(object):

    def __init__(self, input_file , num_reducers,output_base):
        self.input_file = input_file
        self.num_reducer = int(num_reducers)
        self.output_base = output_base

    def collect_jobs(self,job_list): #todo
        pass

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


class WordCountEngine(Engine):

    def create_map_instance(self,split_id):
        return mr_classes.WordCountMap(split_id)

    def create_reduce_instance(self):
        return mr_classes.WordCountReduce()

    def WordCountMapExecute(self,assigned_split):

        mapper = self.create_map_instance(split_id) # todo need to replace 1 as split_# you get
        file_object = open(self.input_file)
        input = self.read_input(file_object,assigned_split)
        for j, v in enumerate(input):
            mapper.map(j, v)
        # Sort intermediate keys
        table = mapper.get_table()
        keys = table.keys()
        keys.sort()

        #partion
        job_for_reduces= mapper.partition(keys,self.num_reducer)

    def WordCountReduceExecute(self,job_list):

        job_for_reduces = self.collect_jobs(job_list)
        reducer = self.create_reduce_instance()
        reducer.set_output_oder((job_for_reduces.keys()[0])%10) # todo change (job_for_reduces.keys()[0]) to partionID
        for i in job_for_reduces.keys():
            keys = job_for_reduces[i].keys()
            keys.sort()
            for k in keys:
                reducer.reduce(i,k,job_for_reduces.get(i)[k])
        reducer.write_Jason_result(self.output_base)
        reducer.write_txt_result(self.output_base)

class HammingEngine(Engine):

    def create_map_instance(self,class_name,split_id):
        if class_name == "hammingEnc":
            return mr_classes.hammingEncMap(split_id)
        elif class_name == "hammingDec":
            return mr_classes.hammingDecMap(split_id)
        elif class_name == "hammingFix":
            return mr_classes.hammingFixMap(split_id)

    def create_reduce_instance(self):
        return mr_classes.hammingReduce()

    def read_input(self,file_object,split_dict):
        offset = split_dict.keys()[0]
        size = split_dict.get(offset)
        file_object.seek(offset)
        input = file_object.read(size)
        print input
        return input

    def HammingMapExecute(self,assigned_split):#assigned split means the {offset:size} info
        # Map phase
        mapper = self.create_map_instance(class_name,split_id)
        file_object = open(self.input_file)
        input = self.read_input(file_object,assigned_split)
        mapper.map(mapper.get_split_id(), input)

        # Sort intermediate keys
        table = mapper.get_table()
        keys = table.keys()
        keys.sort()

        #partion
        job_for_reduces= mapper.partition(keys,self.num_reducer)

    def HammingReduceExecute(self,job_list):
        #Reduce phase
        job_for_reduces = self.collect_jobs(job_list) # collect must contain {split_id : task}
        keys = job_for_reduces.keys()
        reducer = self.create_reduce_instance()
        reducer.set_output_oder(keys[0]%10) #todo change to partionID
        for k in keys:
            reducer.reduce(k,job_for_reduces.get(k))
        reducer.write_text_result(self.output_base)
        reducer.write_Jason_result(self.output_base)

class SortEngine(Engine):

    def create_map_instance(self,split_id):
        return mr_classes.SortMap(split_id)

    def create_reduce_instance(self):
        return mr_classes.SortReduce()

    def SortMapExecute(self,assigned_split):
        mapper = self.create_map_instance(split_id)
        file_object = open(self.input_file)
        input = self.read_input(file_object,assigned_split)
        for j, v in enumerate(input):
            mapper.map(j, v)

        # Sort intermediate keys
        table = mapper.get_table()
        keys = table.keys()
        keys.sort()

        #partion
        job_for_reduces= mapper.partition(keys,self.num_reducer)

        return job_for_reduces #todo store partition result

    def SortReduceExecute(self,job_list): #need to give job list to reduce to collect job from map

        job_for_reduces = self.collect_jobs(job_list)
        keys = job_for_reduces.keys()
        reducer = self.create_reduce_instance()
        reducer.set_output_oder(keys[0]%10) #keys[0] partionID
        for i in keys:
            reducer.reduce(i,job_for_reduces.get(i))
        reducer.write_Jason_result(self.output_base)
        reducer.write_txt_result(self.output_base)
