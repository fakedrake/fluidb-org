#!/usr/bin/env python

import itertools as it

import pandas as pd
import matplotlib.pyplot as plt


data20500 = pd.DataFrame( {
    'baseline_reads': [18594944, 18605792, 18594944, 18593536, 18859136, 18859136, 18859136, 19631136, 19631136, 19631136, 19631136, 18886720, 18594944, 18605792, 18594944, 18593536, 18859136, 18859136, 18859136, 19631136, 19631136, 19631136, 19631136, 18886720, 18594944, 18605792, 18594944, 18593536, 18859136, 18859136],
    'baseline_writes': [18595936 ,18595712 ,18595936 ,18595104 ,18862464 ,18862464 ,18862464 ,18640032 ,18640032 ,18640032 ,18640032 ,18888704 ,18595936 ,18595712 ,18595936 ,18595104 ,18862464 ,18862464 ,18862464 ,18640032 ,18640032 ,18640032 ,18640032 ,18888704 ,18595936 ,18595712 ,18595936 ,18595104 ,18862464 ,18862464],
    'workload_reads': [18594944 ,18605792 ,0 ,18593536 ,18859136 ,256 ,256 ,19713600 ,2720 ,2720 ,2720 ,25120992 ,18602432 ,18613664 ,18600320 ,18601088 ,704 ,24914688 ,704 ,18729440 ,19738784 ,576 ,704 ,18737664 ,18610048 ,18621216 ,4864 ,18615520 ,576 ,704],
    'workload_writes': [18595936 ,18595712 ,0 ,18595104 ,18862464 ,288 ,288 ,18643392 ,2816 ,2816 ,2816 ,25119552 ,18603552 ,18603680 ,18601408 ,18602784 ,768 ,24837472 ,768 ,18654080 ,18670304 ,672 ,768 ,18662272 ,18611136 ,18611264 ,4928 ,18619072 ,672 ,768]
})

def till_bracket(i):
    line = next(i)
    while line != "}\n":
        ret.append(line)

    return ret

def parse_line(line):
    budget_s,budget_v,reads_s,reads_v,writes_s,writes_v = \
        it.chain(*[i.split(',') for i in line.split(':')])
    assert read_s == "reads" and writes_s == "writes"
    return budget_v,reads_v,writes_v

def parse_body(i):
    fr = []
    for l in till_bracket(i):
        budget,reads,writes = parse_line(l)
        fr.append((reads,writes))

    return fr

def merge_longest(d1,d2):
    if len(d1) < len(d2):
        return merge_longest(d1,d2)

    return zip(d1,it.repeat(d2))

def join_dicts(baselines,workloads):
    for budget,baseline in baselines.items():
        try:
            d = defaultdict(lambda : [])
            for bi,wi in zip(it.repeat(baseline),workloads[budget])
                breads, bwrites = bi
                wreads, wwrites = wi
                d['workload_reads'],append(wreads)
                d['workload_writes'],append(wwrites)
                d['baseline_reads'],append(breads)
                d['baseline_writes'],append(bwrites)

            yield d
        except:
            continue

def parse(path):
    workloads = dict()
    baselines = dict()
    with open(path,'r') as perf:
        lines = pref.getlines()
        i = iter(lines)
        while True:
            try:
                line = next(i)
            except:
                break

            if line == "main {\n":
                budget,d = parse_body(i)
                workloads[budget] = d
            elif line == "baseline {\n":
                budget,d = parse_body(i)
                baselines[budget] = d

    return pd.DataFrame(join_dicts(baselines,workloads))

def plot_wl(data,size,logarithmic=False):
  baseline = data['baseline_reads'] + data['baseline_writes']
  wl = data['workload_reads'] + data['workload_writes']
  plt_data = pd.DataFrame({'Baseline': baseline, 'Workload': wl})
  ax = plt_data.plot(
      ylabel='Page read/writes',
      xlabel='Query sequence',
      logy=logarithmic,
      title='FluiDB SSB-TPCH performance\n(page budget: %d)' % size)
  ax.get_figure()
  fig.savefig('/tmp/workload_%d%s.pdf' % (size,"log" if logarithmic else ""))

def plot_rel(data,size):
  baseline = data['baseline_reads'] + data['baseline_writes']
  wl = data['workload_reads'] + data['workload_writes']
  plt_data = pd.DataFrame({'Workload': wl / baseline})
  ax = plt_data.plot(
      ylabel='Page read/writes',
      xlabel='Query sequence',
      logy=False,
      title='FluiDB SSB-TPCH performance\n(page budget: %d)' % size)
  ax.get_figure()
  fig.savefig('/tmp/workload_rel_%d%s.pdf' % (size,"log" if logarithmic else ""))

plot_wl(data20500, 20500)
plot_wl(data20500, 20500, logarithmic=True)
