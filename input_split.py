class Split:

    def __init__(self, input_file, split_size, class_name):
        self.input_file = input_file
        self.split_size = int(split_size)
        self.class_name = class_name


    def adjust_offset(self,file_object,start,end):
        newpos = start
        file_object.seek(start,0)
        while newpos < end and newpos < self.split_size-1 + start:
            file_object.readline()
            newpos = file_object.tell()
        return newpos

    def generate_split_info(self):
        file_object = open(self.input_file)
        file_object.seek(0, 2)
        end = file_object.tell()
        #split = {}
        split_map = {}
        offset = 0
        split_num = 0
        if 'hamming' in self.class_name:
            if self.class_name in ['hammingDec','hammingFix']:
                while (self.split_size *8)%12 != 0:
                    self.split_size = self.split_size + 1
            while offset < end:
                split_map[split_num] = {offset:self.split_size}
                split_num = split_num+1
                offset = offset+ self.split_size
        else:
            while offset < end:
                prev = offset
                offset = self.adjust_offset(file_object, offset, end)
                #split[prev] = offset - prev
                split_map[split_num] = {prev:offset - prev}
                split_num = split_num+1
        return split_map
