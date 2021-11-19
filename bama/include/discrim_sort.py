from dataclasses import dataclass
from random import shuffle

PAGE_SIZE = 32


# def discriminate_buckets(A,p,B):
#     """Discriminate between bucket A, page p and semibucket B.

#     """
#     assert B.is_head_ready()
#     A.assert_is_bucket()
#     assert A.end == p.index
#     assert p.index + 1 == B.begin

#     if A.empty() and B.empty(): return
#     if len(A) == 1 and B.empty():
#         discriminate(A.head(),p)
#         return
#     if len(B) == 1 and A.empty():
#         discriminate(p,B.head())
#         return

#     if A.empty():
#         discriminate_buckets(p.as_bucket(), B.pop(), B)
#         return

#     if B.empty():
#         discriminate_buckets(A,A.rpop(),p.as_bucket())
#         return

#     while not A.empty() and p < B.head():
#         discriminate(A.pop(),p)

#     # Now p overlaps with B
#     B1 = B.copy()
#     discriminate_buckets(p.as_bucket(),B.pop(),B)
#     discriminate_buckets(A,p,B1)

# def discrim_sort(lst):
#     global ops
#     store = to_pages(lst)
#     for p in store:
#         p.sort_internal()

#     discrim_sort_internal(to_bucket(store))

#     return from_pages(store)

# def discrim_sort_internal(buck):
#     if len(buck) <= 1: return
#     if len(buck) == 2:
#         discriminate(buck.head(),buck.rpeek())
#         return

#     A,B = buck.split()
#     discrim_sort_internal(A.copy())
#     discrim_sort_internal(B.copy())
#     discriminate_buckets(A,B.pop(),B)


@dataclass
class Ops:
    reads = 0
    writes = 0
    discrims  = 0

ops = Ops()

@dataclass
class Bucket:
    begin : int
    end : int
    store : list
    def pop(self):
        if self.begin < self.end:
            self.begin += 1
            return self.store[self.begin - 1]
        return None

    def rpop(self):
        if self.begin < self.end:
            self.end -= 1
            return self.store[self.end]
        return None

    def head(self):
        if self.begin < self.end:
            return self.store[self.begin]
        return None

    def rpeek(self):
        if self.begin < self.end:
            return self.store[self.end - 1]
        return None

    def empty(self):
        return len(self) <= 0

    def tail(self):
        if self.begin < self.end:
            return Bucket(self.begin + 1,self.end, self.store)
        return None

    def __len__(self):
        return self.end - self.begin

    def is_head_ready(self):
        """The page at the top may overlap with other page but is
        rightfully at the top.

        """
        if self.tail():
            self.tail().assert_is_bucket()

        return len(self) < 2 or \
            self.store[self.begin].records[0] <= self.store[self.begin + 1].records[0]


    def __getitem__(self, index):
        i = index + self.begin
        if i < self.end:
            return self.store[i]

        raise IndexError

    def assert_is_bucket(self):
        for i,j in zip(range(len(self)),range(1,len(self))):
            assert self[i] < self[j]

        return True

    def copy(self):
        return Bucket(self.begin,self.end,self.store)

    def split(self):
        mid = (self.begin + self.end) // 2
        return Bucket(self.begin, mid,self.store), \
            Bucket(mid,self.end,self.store),

@dataclass
class Page(object):
    records : list
    index : int
    store : list
    def __lt__(self, p):
        if isinstance(p,Page):
            return self.records[-1] <= p.records[0]
        if isinstance(p,Bucket):
            return p.empty() or self < p.head()

    def as_bucket(self):
        return Bucket(self.index, self.index+1,self.store)

    def sort_internal(self):
        self.records.sort()


def to_pages(lst):
    store = []
    for j,i in enumerate(range(0,len(lst),PAGE_SIZE)):
        store.append(Page(lst[i:i+PAGE_SIZE],j,store))
    return store

def to_bucket(store):
    return Bucket(0,len(store),store)

def from_pages(store):
    ret = []
    for p in store:
        ret += p.records

    return ret

def discriminate(p1,p2):
    global ops
    ops.discrims += 1
    tmp = sorted(p1.records + p2.records)
    p1.records,p2.records = tmp[:PAGE_SIZE], tmp[PAGE_SIZE:]

def make_heap(B):
    if B.empty(): return

    h,t = B.head(), B.tail()
    h.sort_internal()
    for p in t:
        discriminate(h,p)

    l,r = t.split()
    make_heap(l)
    make_heap(r)

def merge_adjheaps(A,B):
    assert A.end == B.begin
    if A.empty() and B.empty(): return
    if A.empty():
        lB, rB = B.tail().split()
        merge_adjheaps(lB,rB)
        return

    if B.empty():
        lA, rA = A.tail().split()
        merge_adjheaps(lA,rA)
        return

    hA,tA, hB, tB = A.head(), A.tail(), B.head(), B.tail()
    lA,rA = tA.split()
    lB,rB = tB.split()
    discriminate(hA,hB)
    merge_adjheaps(lA,rA) # 2 log(n-1)
    if hB < lB and hB < rB:
        merge_adjheaps(tA,B) # tA is sorted so it's up to B
        return

    # This check is redundant because if B is not a heap it means that
    # the upper bound of hA spilled into the upper bound of the new hB
    # and therefore now hB is bounded by lA,lB
    if hB < lA and hB < rA:
        # B is no longer a heap so we have 5 different heaps that need
        # to be merged.
        merge_adjheaps(lB,rB)
        merge_adjheaps(hB.as_bucket(),tB)
        merge_adjheaps(tA,B)
        return

    assert False, (A,B)

def heap_sort(lst):
    A = to_bucket(to_pages(lst))
    make_heap(A)
    lA, rA = A.tail().split()
    merge_adjheaps(lA,rA)
    return from_pages(A)

def test(length):
    global ops
    ops.discrims = 0
    lst = list(range(length))
    shuffle(lst)
    # lst = discrim_sort(lst)
    lst = heap_sort(lst)
    assert lst == list(range(length)), "unordered: %s" % lst
    return ops.discrims

for i in range(1,10):
    test(i * 1000)
    print(i,ops.discrims / i)
