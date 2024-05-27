class DefaultList(list):
    """ adapted from https://stackoverflow.com/a/869901/8123"""

    def __init__(self, factory):
        self.factory = factory

    def __getitem__(self, index):
        size = len(self)
        if index >= size:
            self.extend(self.factory() for _ in range(size, index + 1))

        return list.__getitem__(self, index)

    def __setitem__(self, index, value):
        size = len(self)
        if index >= size:
            self.extend(self.factory() for _ in range(size, index + 1))

        list.__setitem__(self, index, value)

def defaultlist(factory):
    return DefaultList(factory)
