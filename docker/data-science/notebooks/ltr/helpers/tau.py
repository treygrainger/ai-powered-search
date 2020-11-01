sign = lambda a: (a>0) - (a<0)

def pairs_in_order(ranking, both_ways=True):
    assert len(ranking) > 1
    for idx1, val1 in enumerate(ranking):
        for idx2, val2 in enumerate(ranking):
            if idx2 > idx1:
                yield val1, val2, sign(idx2-idx1)
                if both_ways:
                    yield val2, val1, sign(idx1-idx2)

def tau(rank1, rank2, at=4):
    rank1in = {}


    if len(rank1) < at or len(rank2) < at:
        raise ValueError("rankings must be larger than provided at param(%s)" % at)

    # Handle 1 as a special case
    if at == 1:
        if rank1[0] == rank2[0]:
            return 1
        return -1

    rank1 = rank1[:at]; rank2 = rank2[:at]

    # gather concordances/discords for rank1
    for val1, val2, order in pairs_in_order(rank1, both_ways=True):
        rank1in[(val1,val2)] = order

    # check rank2
    concords = 0
    discords = 0
    for val1, val2, order in pairs_in_order(rank2, both_ways=False):
        try:
            rank1order = rank1in[(val1,val2)]
            if order == rank1order:
                concords += 1
            else:
                discords += 1
        except KeyError:
            discords += 1

    return (concords - discords) / ((at * (at - 1)) / 2)

def avg_tau(rank1, rank2, at=4):
    if len(rank1) < at or len(rank2) < at:
        raise ValueError("rankings must be larger than provided at param(%s)" % at)

    rank1 = rank1[:at]; rank2 = rank2[:at]

    tot = 0
    for i in range(1,at+1):
        tot += tau(rank1,rank2,at=i)
    return tot / (at)

if __name__ == "__main__":
    print(tau([1,2,3,4],[4,3,2,1]))
    print(tau([1,2,3,4],[1,2,3,4]))
    print(tau([1,2,4,3],[1,2,3,4]))
    print(tau([5,6,7,8],[1,2,3,4]))
    print(tau([1,2,3,5],[1,2,3,4]))
    print(tau([5,3,2,1],[4,3,2,1]))
    l1=[1,2,4,3]; l2=[1,2,3,4]; l3=[2,1,3,4]
    print("avg_tau(%s,%s,at=4) %s" % (l1, l1, avg_tau(l1,l1)))
    print("avg_tau(%s,%s,at=4) %s" % (l1, l2, avg_tau(l1,l2)))
    print("avg_tau(%s,%s,at=4) %s" % (l2, l3, avg_tau(l1,l3)))
    print("tau(%s,%s,at=4) %s" % (l1, l2, tau(l1,l2)))
    print("tau(%s,%s,at=4) %s" % (l2, l3, tau(l1,l3)))

