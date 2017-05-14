import sys
import numpy as np


#print("hello py!")

inputfile=sys.argv[1]

np.set_printoptions(precision=2, suppress=True)
array=np.loadtxt(inputfile, dtype=float, delimiter=" ")

print np.mean(array, 0)
