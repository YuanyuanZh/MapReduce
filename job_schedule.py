class job_schedule:

    def __init__(self, num_maps, split_dict):
        self.num_maps = num_maps
        self.split_dict = split_dict

    def schedule(self):
        jobs_map = {}
        pos = []
        num_split = len(self.split_dict)
        split_keys = (self.split_dict).keys()
        split_keys.sort()
        div_unit = num_split/self.num_maps
        count = self.num_maps
        unit = div_unit
        while count > 1:
            pos.append(div_unit)
            div_unit = div_unit + unit
            count = count-1
        pos.append(len(split_keys))
        for i in split_keys:
            index = split_keys.index(i)
            for j in range(len(pos)):
                if index < pos[j]:
                    if j in jobs_map:
                        jobs_map[j][i] = self.split_dict[i]
                        break
                    else:
                        jobs_map[j] = {i:self.split_dict[i]}
                        break
        return jobs_map

if __name__ == '__main__':

    schedule = job_schedule(2,{0:12,13:12,25:14,29:18,45:13,57:15,78:13})
    dict = schedule.schedule()
    c = 1


