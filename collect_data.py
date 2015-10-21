import json
import sys
import os

class Collect_data(object):

    def __init__(self, filename_base, output_filename):
        self.filename_base = filename_base
        self.output_filename = output_filename
        self.path = "./testoutput/"

    def read_data(self,filename):
        with open(filename, 'r') as f:
            data = json.load(f)
        return data

    def merge_data(self,data_list,output_filename):
        data_dict = {}
        for k in data_list:
            keys = k.keys()
            for key in keys:
                data_dict[key]=k.get(key)

        keys = data_dict.keys()
        keys.sort()
        out_str = ''
        for key in keys:
            s = ''
            vaule = data_dict.get(key)
            for v in vaule:
                s += v
            out_str += s
        out_file = open(self.path+output_filename+'.txt','w')
        out_file.write(str(out_str))

    def collect_files(self,path):
        files = []
        data_list = []
        for file in os.listdir(path):
            if file.endswith(".json") and file.startswith(self.filename_base):
                files.append(file)
        for i in range(len(files)):
            data = self.read_data(self.path+files[i])
            data_list.append(data)
        return data_list

    def collect(self):
        data_list = self.collect_files(self.path)
        self.merge_data(data_list,self.output_filename)

