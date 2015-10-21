import collect_data
import sys

if __name__ == '__main__':
    filename_base = sys.argv[1]
    output_filename = sys.argv[2]
    collector = collect_data.Collect_data(filename_base,output_filename)
    collector.collect()

