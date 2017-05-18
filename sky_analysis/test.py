
import numpy as np
from matplotlib import pyplot as plt


plt.figure(figsize=(9,6))
n=10
#rand 均匀分布和 randn高斯分布
x=np.random.randn(1,n)
y=np.random.randn(1,n)


plt.scatter(x,y,s=25,alpha=0.4,marker='o')
#T:散点的颜色
#s：散点的大小
#alpha:是透明程度
plt.show()