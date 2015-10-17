import sys

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

class Engine(object):

    def __init__(self, input_file,split_size, map_name, reducer_name,output_base):
        self.input_file = input_file
        self.split_size = split_size
        self.map_class = map_name
        self.reduce_class = reducer_name
        self.result_list = None
        self.output_base = output_base

    def create_class(self,class_name):
        if class_name == 'WordCountMap':
            return WordCountMap()
        if class_name == 'WordCountReduce':
            return WordCountReduce()

    def adjust_offset(self,file_object,start,end):
        newpos = start
        file_object.seek(start,0)
        s = int(self.split_size)
        while newpos < end and newpos < s-1 + start:
            file_object.readline()
            newpos = file_object.tell()
        return newpos

    def execute(self):
        # Map phase
        mapper = self.create_class(self.map_class)
        input = self.get_input()

        for j, v in enumerate(input):
                mapper.map(j, v)

        # Sort intermediate keys
        table = mapper.get_table()
        keys = table.keys()
        keys.sort()

        # Reduce phase
        reducer = self.create_class(self.reduce_class)
        for k in keys:
            reducer.reduce(k, table[k])
        self.result_list = reducer.get_result_list()

        out_file = open(self.output_base,'w')
        value = str(self.result_list)
        out_file.write(value)

    def get_input(self):
        file_object = open(self.input_file)
        file_object.seek(0, 2)
        end = file_object.tell()
        pos_list = []
        offset = 0
        while offset < end:
            pos_list.append(offset)
            offset = self.adjust_offset(file_object, offset, end)
        input = []
        for i in range(len(pos_list)):
            file_object.seek(pos_list[i])
            if i + 1 < len(pos_list):
                size = pos_list[i + 1] - pos_list[i]
                vi = file_object.read(size)
            else:
                vi = file_object.read(end - pos_list[i])
            input.append(vi)
        return input


if __name__ == '__main__':
    map_name = sys.argv[1]
    split_size = sys.argv[2]
    reducer_name = sys.argv[3]
    in_filename = sys.argv[4]
    output_base = sys.argv[5]
    engine = Engine(in_filename, split_size,map_name, reducer_name,output_base)
    engine.execute()
