import numpy as np
import os

matrix = np.load("tasks/matrix_80_2.npy")

size = len(matrix)

x = []
y = []
z = []

limit = 500 + 80

for i in range(0, size):
    for j in range(0, size):
        if matrix[i][j] > limit:
            x.append(i)
            y.append(j)
            z.append(matrix[i][j])

np.savez(file="results/points", x=x, y=y, z=z)
np.savez_compressed(file="results/points_zip", x=x, y=y, z=z)

print(f"points     = {os.path.getsize('results/points.npz')}")
print(f"points_zip = {os.path.getsize('results/points_zip.npz')}")
