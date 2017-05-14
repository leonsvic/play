import sys
import numpy as np


# print("hello py!")

inputfile=sys.argv[1]

array=np.loadtxt(inputfile, dtype=float, delimiter=" ")

# format output
np.set_printoptions(formatter={'float': '{: .1f}'.format})

print np.sum(array, axis=0)
