def triangles():
    L = [1]
    while len(L)<20:
        yield L
        L.append(0)
        L = [L[i - 1] + L[i] for i in range(len(L))]


for i in triangles():
    print(i)