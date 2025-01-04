

class Graph:
    nodes: list[list[int]] = []
    edges: list = [[], []]
    edge_feature: list = []
    label: list

    def __init__(self, filepath: str, delim1: str = '#', delim2: str = ','):
        self.nodes = []
        self.edges = [[], []]
        self.edge_feature = []
        with open(filepath, 'r') as file:
            for line in file:
                line = line.strip()
                if line:
                    row = line.split(delim1)
                    self.process_node_and_edge_list(row, delim2)

    def process_node_and_edge_list(self, row: list[str], delim: str) -> None:
        # process the node
        self.nodes.append([])
        # self.nodes[-1].append(int(row[0].split(delim)[1]))
        src = int(row[0].split(delim)[0])
        read_cnt, write_cnt = int(row[0].split(delim)[1]), int(row[0].split(delim)[2])
        latency = float(row[0].split(delim)[3])
        success = int(row[0].split(delim)[4])
        self.nodes[-1].append([read_cnt, write_cnt, latency, success])
        # process edge
        edges = row[1:]
        for edge in edges:
            if len(edge) <= 0:
                continue
            features = edge.split(delim)
            dst = int(features[0])
            ty = int(features[1])
            ta = int(features[2])
            self.edges[0].append(src)
            self.edges[1].append(dst)
            self.edge_feature.append([ty, ta])
        self.edges[0].append(1)
        self.edges[1].append(2)
        self.edge_feature.append([0, 1])
