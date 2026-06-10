#!/usr/bin/env python3
"""Parse section 8 parameter tables from manual.md into CSVs."""

import re, csv

MD_FILE = "consumer/manual.md"

# ─── line cleaning ────────────────────────────────────────────────────────────

SKIP_RE = re.compile(
    r'Capsule Aggregator HL7 Interface Manual'
    r'|^\d+-\d+\s+Capsule'
    r'|^8-\d+\s'
    r'|^\s*Note:'
    r'|^8\.\d\s+CARESCAPE'
    r'|^mm\[Hg\]\^$'
    r'|^uV\^uV\^U$'
    r'|^Cel\^Cel\^$'
    r'|^\{beat\}/min\^\{$'
)

# checked on the FINAL clean text (after pipe-stripping)
SKIP_TEXT = [
    'MDS Name', 'VMD Metric', 'VMD Channel', 'Metric Data Unit',
    'Channel  Metric', 'Supported Parameters',
    'Metric Metric Data', 'Metric\nMetric',
]


def pipe_cells(line):
    cells = [c.strip() for c in line.split('|')]
    return [c for c in cells if c and not re.match(r'^-+$', c)]


def clean_line(raw):
    line = raw.strip()
    if not line:
        return ''
    if re.match(r'^[\|\s\-]+$', line):
        return ''
    if SKIP_RE.search(line):
        return ''
    if '|' in line:
        cells = pipe_cells(line)
        line = ' '.join(cells)
    if not line:
        return ''
    # skip header repeats and lone column-title words
    for s in SKIP_TEXT:
        if s in line:
            return ''
    if re.match(r'^Metric\s*$', line):
        return ''
    return line


def tok(text):
    return text.split() if text else []


def get_raw(all_lines, start, end):
    return [all_lines[i] for i in range(start - 1, min(end, len(all_lines)))]


def group_records(raw_lines):
    records, cur = [], []
    for raw in raw_lines:
        c = clean_line(raw)
        if not c:
            continue
        if '69965^MDC_DEV_MON' in c:
            if cur:
                records.append(cur)
            cur = [c]
        elif cur:
            cur.append(c)
    if cur:
        records.append(cur)
    return records


# ─── 8.1 Waveforms ───────────────────────────────────────────────────────────
# HDR: mds_name, vmd_name, vmd_id, channel_name, metric_id, data_type, unit_of_measure
#
# (4,7): std ECG leads
#   L1[mds1,vmd1,chan1,unit1]  L2[mds2,vmd2,vid,chan2,mid,dt,unit2]
# (5,8): pressure/temp channels — chan on both lines
#   L1[mds1,vmd1,chan1,metric1,unit1]  L2[mds2,vmd2,vid,chan2,MDC,mid,dt,unit2]

def parse_81(parts):
    T = [tok(p) for p in parts]
    # drop trailing stray 1-token lines; take first 2 meaningful lines only
    T = [t for t in T if len(t) > 1][:2]
    if len(T) != 2:
        return None
    L1, L2 = T
    n1, n2 = len(L1), len(L2)

    if n1 == 4 and n2 == 7:
        mds  = L1[0]+L2[0]; vmd = L1[1]+L2[1]; vid = L2[2]
        chan = L1[2]+L2[3]; mid = L2[4]; dt = L2[5]; unit = L1[3]+L2[6]
    elif n1 == 5 and n2 == 8:
        mds  = L1[0]+L2[0]; vmd = L1[1]+L2[1]; vid = L2[2]
        chan = L1[2]+L2[3]; mid = L2[5]; dt = L2[6]; unit = L1[4]+L2[7]
        # L1[3]+L2[4] = metric name but 8.1 header only has metric_id
    else:
        return None

    return [mds, vmd, vid, chan, mid, dt, unit]


# ─── 8.2 / 8.3 Measurements ──────────────────────────────────────────────────
# HDR: mds_name, vmd_name, vmd_id, channel_name, metric_name, data_type, unit_of_measure
#
# A (3,6): no chan, complete unit on L2
# B (4,6): no chan, split unit
# C (5,7): chan, split unit
# D (4,7): chan, complete unit on L2
# E (3,7): chan (from L1[2]+L2[3]), no metric prefix on L1, complete unit on L2

def parse_82_83(parts):
    T = [tok(p) for p in parts]
    T = [t for t in T if len(t) > 1]
    if len(T) != 2:
        return None
    L1, L2 = T
    n1, n2 = len(L1), len(L2)

    if   n1==3 and n2==6:  # A
        mds=L1[0]+L2[0]; vmd=L1[1]+L2[1]; vid=L2[2]; chan=''
        metric=L1[2]+L2[3]; dt=L2[4]; unit=L2[5]
    elif n1==4 and n2==6:  # B
        mds=L1[0]+L2[0]; vmd=L1[1]+L2[1]; vid=L2[2]; chan=''
        metric=L1[2]+L2[3]; dt=L2[4]; unit=L1[3]+L2[5]
    elif n1==5 and n2==7:  # C
        mds=L1[0]+L2[0]; vmd=L1[1]+L2[1]; vid=L2[2]
        chan=L1[2]+L2[3]; metric=L1[3]+L2[4]; dt=L2[5]; unit=L1[4]+L2[6]
    elif n1==4 and n2==7:  # D
        mds=L1[0]+L2[0]; vmd=L1[1]+L2[1]; vid=L2[2]
        chan=L1[2]+L2[3]; metric=L1[3]+L2[4]; dt=L2[5]; unit=L2[6]
    elif n1==3 and n2==7:  # E
        mds=L1[0]+L2[0]; vmd=L1[1]+L2[1]; vid=L2[2]
        chan=L1[2]+L2[3]; metric=L2[4]; dt=L2[5]; unit=L2[6]
    else:
        return None

    return [mds, vmd, vid, chan, metric, dt, unit]


# ─── 8.4 Alarms ──────────────────────────────────────────────────────────────
# HDR: mds_name, vmd_name, vmd_id, channel_name, channel_id, metric_name, data_type, unit_of_measure

def is_unit_frag(t):
    return bool(re.match(r'^(mm\[Hg\]|uV|Cel|\{beat\}|\{breath\}|%)', t))

def parse_84(parts):
    T = [tok(p) for p in parts]
    T = [t for t in T if len(t) > 0]
    if len(T) != 3:
        return None
    L1, L2, L3 = T
    n1, n2, n3 = len(L1), len(L2), len(L3)

    if L3[0] != 'MDC':
        return None

    unit = L3[-1]; dtype = L3[-2]

    # Collect unit fragments from earlier lines
    def ufrag(line_toks):
        return line_toks[-1] if line_toks and is_unit_frag(line_toks[-1]) else ''

    uf1 = ufrag(L1); uf2 = ufrag(L2)
    if uf1:
        unit = uf1 + uf2 + unit
    elif uf2:
        unit = uf2 + unit

    # strip unit frags from L1/L2 for positional parsing
    G1 = L1[:-1] if uf1 else L1
    G2 = L2[:-1] if uf2 else L2
    g1, g2 = len(G1), len(G2)

    # Patterns by (g1, g2, n3):
    if   g1==1 and g2==1 and n3==6:
        # (1,1,6): mds+cid+chan+metric in L3
        mds=G1[0]+G2[0]+L3[0]; vmd=''; vid=L3[1]; chan=''; cid=L3[2]; metric=L3[3]

    elif g1==1 and g2==2 and n3==7:
        # (1,2,7): vmd on L2+L3
        mds=G1[0]+G2[0]+L3[0]; vmd=G2[1]+L3[1]; vid=L3[2]; chan=''; cid=L3[3]; metric=L3[4]

    elif g1==1 and g2==3 and n3==7:
        # (1,3,7): metric prefix on L2
        mds=G1[0]+G2[0]+L3[0]; vmd=G2[1]+L3[1]; vid=L3[2]; chan=''; cid=L3[3]; metric=G2[2]+L3[4]

    elif g1==1 and g2==4 and n3==8:
        # (1,4,8): chan on L2, metric split
        mds=G1[0]+G2[0]+L3[0]; vmd=G2[1]+L3[1]; vid=L3[2]
        chan=G2[2]+L3[3]; cid=L3[4]; metric=G2[3]+L3[5]

    elif g1==2 and g2==2 and n3==7:
        # (2,2,7): vmd across 3 lines, no chan
        mds=G1[0]+G2[0]+L3[0]; vmd=G1[1]+G2[1]+L3[1]; vid=L3[2]; chan=''; cid=L3[3]; metric=L3[4]

    elif g1==2 and g2==3 and n3==8:
        # (2,3,8): chan on L2/L3, metric only on L3
        mds=G1[0]+G2[0]+L3[0]; vmd=G1[1]+G2[1]+L3[1]; vid=L3[2]
        chan=G2[2]+L3[3]; cid=L3[4]; metric=L3[5]

    elif g1==2 and g2==4 and n3==8:
        # (2,4,8): chan, metric on L2+L3
        mds=G1[0]+G2[0]+L3[0]; vmd=G1[1]+G2[1]+L3[1]; vid=L3[2]
        chan=G2[2]+L3[3]; cid=L3[4]; metric=G2[3]+L3[5]

    elif g1==2 and g2==4 and n3==9:
        # (2,4,9): chan, metric on L2+L3[5]+L3[6]
        mds=G1[0]+G2[0]+L3[0]; vmd=G1[1]+G2[1]+L3[1]; vid=L3[2]
        chan=G2[2]+L3[3]; cid=L3[4]; metric=G2[3]+L3[5]+L3[6]

    elif g1==3 and g2==3 and n3==8:
        # (3,3,8): NONINV — chan on all 3 lines, metric only on L3
        mds=G1[0]+G2[0]+L3[0]; vmd=G1[1]+G2[1]+L3[1]; vid=L3[2]
        chan=G1[2]+G2[2]+L3[3]; cid=L3[4]; metric=L3[5]

    elif g1==3 and g2==4 and n3==8:
        # (3,4,8): chan across 3 lines, metric split L2+L3
        mds=G1[0]+G2[0]+L3[0]; vmd=G1[1]+G2[1]+L3[1]; vid=L3[2]
        chan=G1[2]+G2[2]+L3[3]; cid=L3[4]; metric=G2[3]+L3[5]

    elif g1==4 and g2==4 and n3==8:
        # (4,4,8): vmd+chan across 3 lines (unit frags already stripped from G1/G2)
        mds=G1[0]+G2[0]+L3[0]; vmd=G1[1]+G2[1]+L3[1]; vid=L3[2]
        chan=G1[2]+G2[2]+L3[3]; cid=L3[4]; metric=L3[5]

    elif g1==2 and g2==3 and n3==7:
        # (2,3,7): unit frag on L1, metric continuation on L2, vmd on L2+L3
        mds=G1[0]+G2[0]+L3[0]; vmd=G2[1]+L3[1]; vid=L3[2]; chan=''; cid=L3[3]; metric=G2[2]+L3[4]

    else:
        return None

    return [mds, vmd, vid, chan, cid, metric, dtype, unit]


# ─── Main ─────────────────────────────────────────────────────────────────────

CONFIGS = {
    '8.1_waveforms':       {'range':(1090,1187),  'parser':parse_81,    'header':['mds_name','vmd_name','vmd_id','channel_name','metric_id','data_type','unit_of_measure']},
    '8.2_measurements':    {'range':(1188,1411),  'parser':parse_82_83, 'header':['mds_name','vmd_name','vmd_id','channel_name','metric_name','data_type','unit_of_measure']},
    '8.3_gw_measurements': {'range':(1412,1619),  'parser':parse_82_83, 'header':['mds_name','vmd_name','vmd_id','channel_name','metric_name','data_type','unit_of_measure']},
    '8.4_alarms':          {'range':(1620,2503),  'parser':parse_84,    'header':['mds_name','vmd_name','vmd_id','channel_name','channel_id','metric_name','data_type','unit_of_measure']},
}

def main():
    with open(MD_FILE) as f:
        all_lines = f.readlines()

    for name, cfg in CONFIGS.items():
        raw = get_raw(all_lines, *cfg['range'])
        records = group_records(raw)
        parsed, failed = [], []
        for rec in records:
            row = cfg['parser'](rec)
            (parsed if row else failed).append(row or rec)

        with open(f'consumer/{name}.csv', 'w', newline='') as f:
            w = csv.writer(f); w.writerow(cfg['header']); w.writerows(parsed)

        print(f'{name}: {len(parsed)} rows, {len(failed)} failed')
        seen = set()
        for rec in failed:
            T = [tok(p) for p in rec]
            pat = tuple(len(t) for t in T)
            if pat not in seen:
                seen.add(pat)
                print(f'  FAIL {list(pat)}: {rec[:2]}')

if __name__ == '__main__':
    main()
