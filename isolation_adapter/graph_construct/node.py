class Node:
    idx: int
    __type: int

    def __init__(self, idx, t: int):
        self.idx = idx
        self.__type = t

    def feature(self):
        return [self.__type]
