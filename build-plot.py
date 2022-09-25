#!/usr/bin/env python

import itertools as it
from typing import *

import pandas as pd
import matplotlib.pyplot as plt

class Line(object):
    def __init__(self, txt, num):
        self.txt = txt.strip()
        self.num = num

    def is_cpp_path(self) -> bool:
        return self.txt.endswith(".cpp")

    def get_begin(self) -> Optional[str]:
        if self.txt == "main {":
            return "main"
        elif self.txt == "baseline {":
            return "baseline"
        else:
            return None

    def is_workload_end(self) -> bool:
        return self.txt.startswith("}")

    def parse(self) -> (int, int):
        line = self.txt
        try:
            _q, _t, time, _r, read_s, _w, write_s = \
                it.chain(*[i.split(',') for i in line.split(':')])
        except ValueError:
            raise ValueError(repr(self))

        assert _r == "reads" and _w == "writes"
        return int(read_s), int(write_s)

    def __repr__(self):
        return f"Line({self.num}, {self.txt})"


class Workload(object):
    def __init__(self, wl_type: str, queries: List[Tuple[int, int]]):
        self.wl_type = wl_type
        self.queries = queries

    def __repr__(self):
        return f"Workload({self.wl_type}, {self.queries})"

    def ops(self, repeat_till: int) -> Iterable[int]:
        return it.islice(it.cycle((r + w for r, w in self.queries)), repeat_till)

    def __len__(self):
        return len(self.queries)

class LineIter(object):
    def __init__(self, path):
        self.it = (Line(l,i) for i,l in enumerate(open(path, 'r')))

    def parse_workload(self) -> Workload:
        """Get the reads and writes of each iteration."""

        try:
            l = next(self.it)
        except StopIteration:
            return

        wl_type = l.get_begin()
        assert wl_type, f"Not the beginning of body: {l}"

        try:
            l = next(self.it)
        except StopIteration:
            return

        fr = []
        while not l.is_workload_end():
            if not l.is_cpp_path():
                fr.append(l.parse())

            try:
                l = next(self.it)
            except StopIteration:
                return

        return Workload(wl_type, fr)

    def workloads(self) -> Iterable[Workload]:
        wl = self.parse_workload()
        while wl is not None:
            yield wl
            wl = self.parse_workload()

def dataframe(wls):
    wls = list(wls)
    data_len = max(map(len, wls))
    return pd.DataFrame({w.wl_type: w.ops(data_len) for w in wls})




def plot(budget: int, wls: Iterable[Workload], logarithmic: bool = False) -> str:
    from matplotlib.ticker import EngFormatter

    plt_data = dataframe(wls)
    ax = plt_data.plot(
        kind='bar',
        width=.8,
        ylabel='Page read/writes',
        xlabel='Query sequence',
        logy=logarithmic,
        title='FluiDB SSB-TPCH performance\n(page budget: %d)' % budget)

    # ax.yaxis.get_major_formatter().set_scientific(False)
    ax.yaxis.set_major_formatter(EngFormatter())
    ax.set_xticklabels(range(1, len(plt_data) + 1))
    fig = ax.get_figure()
    pdf_path = '/tmp/workload_%d%s.pdf' % (budget, "log" if logarithmic else "")
    fig.savefig(pdf_path)
    return pdf_path


# WL_PATH = "ssb-workload/op_perf_prev.txt"
WL_PATH = "ssb-workload/io_perf.txt"
plot(17000, LineIter(WL_PATH).workloads())
