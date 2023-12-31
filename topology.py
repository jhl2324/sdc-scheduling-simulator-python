class Host:
    def __init__(self, host_type, queue_type, num_cores, avail_num_cores=None):
        self.host_type = host_type
        self.queue_type = queue_type
        self.num_cores = num_cores
        self.avail_num_cores = num_cores if avail_num_cores is None else avail_num_cores

    def __str__(self):
        return f'host_type : {self.host_type} |\
                queue_type : {self.queue_type} |\
                num_cores : {self.num_cores} |\
                avail_num_cores : {self.avail_num_cores}'


class Topology:
    def __init__(self, file_name):
        self.hosts = []
        self.num_host_types = 0
        self.num_queue_types = 0
        self.num_total_cores = 0

        self.parse_cluster_log(file_name)

    def parse_cluster_log(self, file_name):
        with open(file_name, 'r') as file:
            lines = file.read().split('\n')
            self.num_host_types, self.num_queue_types, self.num_total_cores = [int(e) for e in lines[1].split(' ')]
            print(self.num_host_types, self.num_queue_types, self.num_total_cores)

            for line in lines[2:]:
                line = [int(e) for e in line.split(' ')]
                self.hosts.append(Host(*line))


#topology = Topology('cluster')

#for host in topology.hosts:
    #print(host)
