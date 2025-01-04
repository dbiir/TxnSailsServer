class Edge:
    __type: int
    __table: int
    src: int
    dst: int

    def __init__(self, t: int, table: int, src: int, dst: int):
        self.__type = t
        self.__table = table
        self.src = src
        self.dst = dst

    def feature(self):
        return [self.__type, self.__table]
