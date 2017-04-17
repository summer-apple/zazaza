def qsort(lst):
    if not lst:
        return []
    else:
        m = lst[0]
        l = [x for x in lst if x < m]
        r = [x for x in lst if x >= m]
        return qsort(l) + [m] + qsort(r)


a = [4, 65, 2, -31, 0, 99, 83, 782, 1]
print(qsort(a))
