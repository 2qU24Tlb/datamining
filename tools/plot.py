import matplotlib.pyplot as plt
import numpy as np


# svt = [1560, 1340, 900, 670, 340, 330]
# peclat = [3140, 2550, 980, 720, 420, 350]
# sup = [35, 40, 45, 50, 55, 60]
# title = "pumsb_star"

svt = [37, 45, 66, 72]
peclat = [52, 59, 90, 102]
sup = [0.5, 0.2, 0.1, 0.09]
title = "DBMS1"

plt.plot(sup, svt, 'b-o', label="SVT")
plt.plot(sup, peclat, 'r-^', label="Peclat")
plt.xlabel("MinSup (%)")
plt.ylabel("Time (sec)")
plt.legend(loc='best')
plt.title(title)

plt.show()
