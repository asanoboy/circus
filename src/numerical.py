import numpy as np
import scipy.sparse as sp
import scipy.io as io
import random
import itertools
import os.path
from debug import Lap
import nimfa
import time


def levenshtein(s1, s2):
    if len(s1) < len(s2):
        return levenshtein(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def add_suffix_if_not_exists(path, suffix):
    if not path.endswith('.' + suffix):
        path += '.' + suffix
    return path


def save_matrix(path, mat):
    if isinstance(mat, sp.csr_matrix):
        io.mmwrite(add_suffix_if_not_exists(path, 'mtx'), mat)
        with open(path + '.type', 'w') as f:
            f.write('csr')
    else:
        np.save(add_suffix_if_not_exists(path, 'npy'), mat)
        with open(path + '.type', 'w') as f:
            f.write('other')


def load_matrix(path):
    with open(path + '.type', 'r') as f:
        type_info = f.read()

    if type_info == 'csr':
        mat = io.mmread(add_suffix_if_not_exists(path, 'mtx'))
        return mat.tocsr()
    else:
        return np.load(add_suffix_if_not_exists(path, 'npy'))


def getCategoryRelationship(elemInclusiveFlagsIter, catNum, elemNum):
    print(catNum, elemNum)
    rows = []
    cols = []
    data = []
    for catIndex, flags in enumerate(elemInclusiveFlagsIter):
        for itemIndex, flag in enumerate(flags):
            if flag:
                data.append(1)
                rows.append(catIndex)
                cols.append(itemIndex)

    A = sp.csr_matrix((data, (rows, cols)), shape=(catNum, elemNum))
    R = A * sp.csr_matrix((data, (cols, rows)), shape=(elemNum, catNum))
    sums = A.sum(1)
    rows, cols = R.nonzero()

    childToParents = {}
    for (row, col) in zip(rows, cols):
        if row != col and sums[row] == R[row, col]:
            if row not in childToParents:
                childToParents[row] = [col]
            else:
                childToParents[row].append(col)

    return {
        child: min(parents, key=lambda x: sums[x])
        for child, parents in childToParents.items()}


class ErrorTracker(nimfa.models.mf_track.Mf_track):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._last_time = None
        self._cnt = 0

    def track_error(self, run, residuals):
        super().track_error(run, residuals)
        current_time = time.time()
        elapsed_time = None
        if self._last_time is not None:
            elapsed_time = current_time - self._last_time
        self._last_time = current_time

        self._cnt += 1
        if elapsed_time:
            print(
                'cnt:%d, error:%f, elapsed=%f' %
                (self._cnt, residuals, elapsed_time))
        else:
            print('cnt:%d, error:%f' % (self._cnt, residuals))


def calc_mf(mx, Track=ErrorTracker, Method=nimfa.Als, track_error=True, **kw):
    nmf = Method(mx.M, Track=Track, track_error=track_error, **kw)
    nmf_fit = nmf()

    fit = MfFit(mx, nmf_fit.fit.H, nmf_fit.fit.W)
    fit.raw = nmf_fit
    return fit


class IdAndIndex:
    def __init__(self):
        self._id_to_index = {}
        self._index_to_id = []  # index is sequencial.
        self.is_fixed = False

    @staticmethod
    def load(path):
        try:
            id2dx = load_matrix(path + '.id2dx')
            dx2id = load_matrix(path + '.dx2id')
            id_and_dx = IdAndIndex()
            id_and_dx._id_to_index = id2dx
            id_and_dx._index_to_id = dx2id
            return id_and_dx
        except:
            return None

    def save(self, path):
        save_matrix(path + '.dx2id', self._index_to_id)
        save_matrix(path + '.id2dx', self._id_to_index)

    def has(self, input_id):
        return input_id in self._id_to_index

    def fix(self):
        self.is_fixed = True

    def find_index(self, input_id, or_create=True):
        if or_create and self.is_fixed:
            raise Exception('Can\'t create after fixed.')

        if not self.has(input_id):
            if not or_create:
                raise Exception('No found index by id=%s' % (input_id,))
            self._index_to_id.append(input_id)
            self._id_to_index[input_id] = len(self._index_to_id)-1
        return self._id_to_index[input_id]

    def find_id(self, index):
        if index < 0 or len(self._index_to_id) <= index:
            raise Exception('No found id by index=%s' % (index,))
        return self._index_to_id[index]

    def size(self):
        return len(self._index_to_id)


class MfFit:
    def __init__(self, relationMx, H, W):
        self.V = relationMx
        self.H = H
        self.W = W

    @staticmethod
    def load(path):
        if not os.path.isdir(path):
            return None

        H = load_matrix(os.path.join(path, 'h'))
        W = load_matrix(os.path.join(path, 'w'))

        relationMx = RelationMatrix.load(os.path.join(path, 'v'))
        if relationMx:
            return MfFit(relationMx, H, W)
        return None

    def save(self, path):
        if os.path.exists(path):
            raise Exception('Already exists: %s' % (path,))

        os.mkdir(path)

        save_matrix(os.path.join(path, 'h'), self.H)
        save_matrix(os.path.join(path, 'w'), self.W)
        self.V.save(os.path.join(path, 'v'))

    def rank(self):
        return self.W.shape[1]

    def w_rows_iter(self):
        for idx in range(self.W.shape[0]):
            id = self.V.src_id_and_index.find_id(idx)
            yield id, list(self.W.getrow(idx).toarray()[0])


class RelationMatrix:
    def __init__(self, src=None, dst=None):
        self.src_id_and_index = IdAndIndex() if src is None else src
        self.dst_id_and_index = IdAndIndex() if dst is None else dst
        self.index_set_to_value = {}
        self.M = None

    @staticmethod
    def load(path):
        try:
            M = load_matrix(path + '.M')
            index_set_to_value = load_matrix(path + '.set')
        except:
            return None

        src = IdAndIndex.load(path + '.src')
        dst = IdAndIndex.load(path + '.dst')
        if src and dst:
            relMx = RelationMatrix(src, dst)
            relMx.M = M
            relMx.index_set_To_value = index_set_to_value
            return relMx

    def save(self, path):
        if self.M is None:
            raise Exception('Can\'t save RelationMatrix before call build().')
        save_matrix(path + '.M', self.M)
        save_matrix(path + '.set', self.index_set_to_value)
        self.src_id_and_index.save(path + '.src')
        self.dst_id_and_index.save(path + '.dst')

    def get_src(self):
        return self.src_id_and_index

    def get_dst(self):
        return self.dst_id_and_index

    def create_inverse(self):
        mtx = RelationMatrix(src=self.get_dst(), dst=self.get_src())
        mtx.M = self.M.transpose(True).tocsr()
        return mtx

    def append(self, id_src, id_dst, val):
        if self.M is not None:
            raise Exception('Can\'t append after built.')

        index_src = self.src_id_and_index.find_index(id_src)
        index_dst = self.dst_id_and_index.find_index(id_dst)
        key = (index_src, index_dst)
        if key in self.index_set_to_value:
            raise Exception(
                '(src, dst) = (%s, %s) already exists' %
                (id_src, id_dst))

        self.index_set_to_value[key] = val

    def build(self):
        data = []
        rows = []
        cols = []
        for src in range(self.src_id_and_index.size()):
            for dst in range(self.dst_id_and_index.size()):
                if (src, dst) in self.index_set_to_value:
                    data.append(self.index_set_to_value[(src, dst)])
                    rows.append(dst)
                    cols.append(src)

        self.M = sp.csr_matrix(
            (data, (rows, cols)),
            shape=(
                self.dst_id_and_index.size(),
                self.src_id_and_index.size()),
            dtype=np.float)

        self.dst_id_and_index.fix()
        self.src_id_and_index.fix()

    def __mul__(self, other):
        if self.M is None:
            raise Exception('Can\'t multiply before built.')

        if isinstance(other, dict):
            data = []
            rows = []
            cols = []
            for src_id, val in other.items():
                if self.src_id_and_index.has(src_id):
                    data.append(val)
                    rows.append(
                        self.src_id_and_index.find_index(src_id, False))
                    cols.append(0)

            A = sp.csr_matrix(
                (data, (rows, cols)),
                shape=(self.src_id_and_index.size(), 1),
                dtype=np.float)
            R = self.M * A

            rows, cols = R.nonzero()
            dst_id_to_value = {
                self.dst_id_and_index.find_id(row): R[row, col]
                for row, col in zip(rows, cols)}

            return dst_id_to_value

        elif isinstance(other, RelationMatrix):
            if self.get_src() != other.get_dst():
                raise Exception('Can\'t multiply matrixs with no match.')
            mtx = RelationMatrix(src=other.get_src(), dst=self.get_dst())
            mtx.M = self.M * other.M
            return mtx

        raise Exception('Input type is not supported: %s' % (other,))


if __name__ == '__main__':
    if 0:
        result = getCategoryRelationship([
            [1 if j % i == 0 else 0 for j in range(1, 9)]
            for i in range(1, 9)], 8, 8)
        print(result)

    if 0:
        matrix = RelationMatrix()
        for src in range(10):
            for dst in range(10):
                value = random.random()
                matrix.append(src, dst, value)

        matrix.build()
        orig = {i: random.random() for i in range(10) if random.random() < 0.5}
        result = matrix * orig
        print(result)

    if 1:
        num = 60
        mtx = RelationMatrix()
        mtx2 = RelationMatrix(src=mtx.get_dst(), dst=mtx.get_dst())

        with Lap('set data'):
            data = [(i, i*i, 1) for i in range(num)]
            for i, j, k in data:
                mtx.append(i, j, k)
            data2 = [
                (i, j, 1)
                for i, j in itertools.product(range(num*num), repeat=2)
                if abs(i-j) < num*2]
            for i, j, k in data2:
                mtx2.append(i, j, k)

        print('len', len(data), len(data2))

        mtx.build()
        inv = mtx.create_inverse()
        mtx2.build()
        with Lap('multiply'):
            mtx3 = inv * mtx2 * mtx
