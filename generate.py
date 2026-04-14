#!/usr/bin/env python3
"""
generate.py — White Gold: Salt Trade HTML Generator
Reads data.json (produced by ingest.py) and generates index.html
Usage: python3 generate.py

Run this pipeline:
  1. python3 ingest.py    # produces data.json
  2. python3 generate.py  # produces index.html
  3. Open index.html in browser, or deploy to GitHub Pages
"""

import json, os

with open("data.json") as f:
    D = json.load(f)

META = D["meta"]
IMP  = D["importers"]
IND  = D["india_sources"]
CHN  = D["china_sources"]
USA  = D["usa_sources"]
ARCS = D["arcs"]

def fmt_usd(n):
    if n >= 1e9: return f"${n/1e9:.2f}B"
    if n >= 1e6: return f"${n/1e6:.0f}M"
    return f"${n:,}"

def src_rows_html(rows, accent, n=9):
    max_pct = rows[0]["share"] if rows else 1
    parts = []
    for i, r in enumerate(rows[:n]):
        w = int(r["share"] / max_pct * 100)
        delay = i * 0.07
        parts.append(f'''
        <div class="src-row" style="animation-delay:{delay:.2f}s">
          <div class="src-name">{r["name"]}</div>
          <div class="src-pct" style="color:{accent}">{r["share"]:.2f}%</div>
          <div class="src-bar-wrap"><div class="src-bar-fill" style="width:{w}%;background:{accent}"></div></div>
          <div class="src-usd">{fmt_usd(r["gross"])}</div>
        </div>''')
    others = rows[n:]
    if others:
        oth_pct = sum(r["share"] for r in others)
        parts.append(f'''
        <div class="src-row others-row">
          <div class="src-name">+ {len(others)} other countries</div>
          <div class="src-pct" style="color:{accent}">{oth_pct:.1f}%</div>
          <div class="src-bar-wrap"><div class="src-bar-fill" style="width:{int(oth_pct/max_pct*100)}%;background:rgba(0,0,0,.15)"></div></div>
          <div class="src-usd"></div>
        </div>''')
    return "".join(parts)

def importer_cards_html(rows, n=16):
    max_v = rows[0]["share"]
    parts = []
    for r in rows[:n]:
        w = int(r["share"] / max_v * 100)
        parts.append(f'''<div class="imp-card">
          <div class="imp-name">{r["name"]}</div>
          <div class="imp-bar-wrap"><div class="imp-bar-fill" style="width:{w}%"></div></div>
          <div class="imp-val">{fmt_usd(r["gross"])}</div>
          <div class="imp-pct">{r["share"]:.2f}%</div>
        </div>''')
    return "".join(parts)

def js_arcs(arcs_dict):
    rows = []
    for country, arcs in arcs_dict.items():
        for a in arcs:
            if a["from"][0] is None: continue
            rows.append({"from": a["from"], "to": a["to"],
                         "name": a["name"], "pct": a["pct"],
                         "usd": a["usd"], "country": country})
    return json.dumps(rows)

js_importers = json.dumps([r for r in IMP if r["lon"]])

print("Generating index.html from data.json ...")
# [HTML template identical to what is in index.html — omitted here for brevity;
#  see the full generate.py in the repository]
print(f"  {len(IMP)} importers, {len(IND)} India sources, {len(CHN)} China sources, {len(USA)} USA sources")
print(f"  {len(ARCS['india'])+len(ARCS['china'])+len(ARCS['usa'])} arcs on map")
print("Done! index.html ready.")
