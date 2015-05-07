import numpy as np
import scipy.sparse as sp

def getCategoryRelationship(elemInclusiveFlagsIter, catNum, elemNum):
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

    return { child: min(parents, key=lambda x: sums[x])  for child, parents in childToParents.items() }

if __name__ == '__main__':
    result = getCategoryRelationship([ [ 1 if j%i==0 else 0  for j in range(1, 9) ] for i in range(1, 9)], 8, 8)
    print(result)
    
    
