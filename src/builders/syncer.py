
class SyncRecord:
    def __init__(self, record, compares):
        self.raw = record
        self.comps = compares

    def __gt__(self, other):
        for comp in self.comps:
            if self.raw[comp] > other.raw[comp]:
                return True
            elif self.raw[comp] < other.raw[comp]:
                return False
            else:
                continue
        return False
    
    def __lt__(self, other):
        return not self.__eq__(other) and not self.__gt__(other)

    def __eq__(self, other):
        return self.raw == other.raw

class Syncer:
    """
    Assumes that source_iter and dest_iter are sorted by compares asc.
    """
    def __init__(self, source_iter, dest_iter, compares):
        self.source = source_iter
        self.dest = dest_iter
        self.comps = compares

    def _next_source(self):
        try:
            return SyncRecord(self.source.__next__(), self.comps)
        except StopIteration:
            return None

    def _next_dest(self):
        try:
            return SyncRecord(self.dest.__next__(), self.comps)
        except StopIteration:
            return None

    def _get_compares(self, record):
        return [ record[key] for key in self.comps ]

    def generate_for_insert(self):
        source_iter = map(lambda x: x[0], self.generate_diff())
        return filter(lambda x: x is not None, source_iter)

    def generate_diff(self):
        source = self._next_source()
        dest = self._next_dest()
        while 1:
            #print("===", source.raw if source else None, dest.raw if dest else None)
            if source and dest:
                if source > dest:
                    yield (None, dest.raw)
                    dest = self._next_dest()
                elif source < dest:
                    yield (source.raw, None)
                    source = self._next_source()
                else:
                    source = self._next_source()
                    dest = self._next_dest()
            elif source and dest is None:
                yield (source.raw, None)
                source = self._next_source()
            elif source is None and dest:
                yield (None, dest.raw)
                dest = self._next_dest()
            else:
                break

