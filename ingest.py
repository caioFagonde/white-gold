#!/usr/bin/env python3
"""
ingest.py — White Gold: Salt Trade Data Pipeline
Reads the 4 source CSVs and produces data.json for generate.py
Usage: python3 ingest.py
"""

import csv, json, os, sys

# ── Country → [lon, lat] lookup (curated from Natural Earth centroids) ──
COORDS = {
    "Afghanistan":[-0.5,33.9],"Albania":[20.2,41.2],"Algeria":[2.6,28.0],
    "Angola":[17.9,-11.2],"Antigua and Barbuda":[-61.8,17.1],
    "Argentina":[-63.6,-38.4],"Armenia":[44.9,40.1],"Australia":[133.8,-25.3],
    "Austria":[14.6,47.5],"Azerbaijan":[47.6,40.1],"The Bahamas":[-77.4,25.0],
    "Bahrain":[50.5,26.0],"Bangladesh":[90.4,23.7],"Belarus":[28.0,53.7],
    "Belgium":[4.5,50.5],"Benin":[2.3,9.3],"Bhutan":[90.4,27.5],
    "Bolivia":[-64.7,-16.3],"Bosnia and Herzegovina":[17.7,44.2],
    "Brazil":[-51.9,-14.2],"Brunei":[114.7,4.5],"Bulgaria":[25.5,42.7],
    "Burundi":[29.9,-3.4],"Cambodia":[104.9,12.6],"Cameroon":[12.4,5.7],
    "Canada":[-96.8,56.1],"Cayman Islands":[-81.2,19.3],"Chad":[18.7,15.5],
    "Chile":[-71.5,-35.7],"China":[104.2,35.9],"Colombia":[-74.3,4.6],
    "Costa Rica":[-83.8,9.7],"Croatia":[15.2,45.1],"Cyprus":[33.4,35.1],
    "Czechia":[15.5,49.8],"Democratic Republic of the Congo":[23.7,-2.9],
    "Denmark":[10.0,56.3],"Djibouti":[42.6,11.8],"Dominican Republic":[-70.2,18.7],
    "Ecuador":[-78.2,-1.8],"Egypt":[30.8,26.8],"El Salvador":[-88.9,13.8],
    "Eritrea":[39.8,15.2],"Estonia":[25.0,58.6],"Eswatini":[31.5,-26.5],
    "Ethiopia":[40.5,9.1],"Fiji":[178.1,-17.7],"Finland":[26.3,64.0],
    "France":[2.2,46.2],"The Gambia":[-15.3,13.4],"Georgia":[43.4,42.3],
    "Germany":[10.5,51.2],"Ghana":[-1.0,7.9],"Greece":[21.8,39.1],
    "Guatemala":[-90.2,15.8],"Guinea":[-11.3,11.0],"Guyana":[-58.9,5.0],
    "Haiti":[-72.3,18.9],"Honduras":[-86.2,15.2],"Hong Kong":[114.1,22.4],
    "Hungary":[19.5,47.0],"Iceland":[-18.1,65.0],"India":[78.9,20.6],
    "Indonesia":[113.9,-0.8],"Iran":[53.7,32.4],"Iraq":[43.7,33.2],
    "Ireland":[-8.2,53.1],"Israel":[34.9,31.5],"Italy":[12.6,42.8],
    "Jamaica":[-77.3,18.1],"Japan":[138.3,36.2],"Jordan":[36.2,31.2],
    "Kazakhstan":[67.0,48.0],"Kenya":[37.9,0.0],"Kuwait":[47.5,29.3],
    "Kyrgyzstan":[74.8,41.2],"Laos":[102.5,17.9],"Latvia":[24.6,56.9],
    "Lebanon":[35.9,33.9],"Liberia":[-9.4,6.5],"Libya":[17.2,27.0],
    "Lithuania":[23.9,55.9],"Luxembourg":[6.1,49.8],"Macao":[113.5,22.2],
    "Madagascar":[46.9,-18.8],"Malawi":[34.3,-13.2],"Malaysia":[109.7,4.2],
    "Mali":[-2.0,17.6],"Marshall Islands":[168.7,7.1],"Mauritania":[-10.9,20.3],
    "Mauritius":[57.6,-20.3],"Mexico":[-102.5,23.6],"Moldova":[28.4,47.4],
    "Mongolia":[103.8,46.9],"Montenegro":[19.4,42.8],"Montserrat":[-62.2,16.7],
    "Morocco":[-7.1,31.8],"Mozambique":[35.5,-18.7],"Myanmar":[95.9,16.9],
    "Namibia":[18.5,-22.0],"Nauru":[166.9,-0.5],"Nepal":[84.1,28.4],
    "Netherlands":[5.3,52.1],"New Zealand":[172.0,-41.5],"Niger":[8.1,16.1],
    "Nigeria":[8.7,9.1],"North Korea":[127.2,40.2],"North Macedonia":[21.7,41.6],
    "Norway":[8.5,60.5],"Oman":[57.6,21.5],"Pakistan":[69.3,30.4],
    "Palestine":[35.3,31.9],"Panama":[-80.8,8.5],"Papua New Guinea":[143.9,-6.3],
    "Paraguay":[-58.4,-23.4],"Peru":[-75.0,-9.2],"Philippines":[121.8,12.9],
    "Poland":[19.1,52.1],"Portugal":[-8.2,39.4],"Qatar":[51.2,25.4],
    "Republic of the Congo":[15.2,-0.2],"Romania":[25.0,45.9],"Russia":[105.3,61.5],
    "Rwanda":[29.9,-1.9],"Saudi Arabia":[45.1,23.9],"Senegal":[-14.5,14.5],
    "Serbia":[21.0,44.0],"Seychelles":[55.5,-4.7],"Sierra Leone":[-11.8,8.5],
    "Singapore":[103.8,1.4],"Slovakia":[19.7,48.7],"Slovenia":[14.8,46.1],
    "Solomon Islands":[159.0,-9.6],"Somalia":[46.2,6.1],"South Africa":[25.1,-29.0],
    "South Korea":[127.8,36.5],"Spain":[-3.7,40.4],"Sri Lanka":[80.8,7.9],
    "Sudan":[29.7,12.9],"Suriname":[-56.0,4.0],"Sweden":[18.6,60.1],
    "Switzerland":[8.2,46.8],"Syria":[38.3,35.0],"Taiwan":[120.9,23.7],
    "Tajikistan":[71.3,38.9],"Tanzania":[34.9,-6.4],"Thailand":[101.0,15.9],
    "Togo":[0.8,8.6],"Trinidad and Tobago":[-61.2,10.7],"Tunisia":[9.0,33.9],
    "Turkmenistan":[59.6,40.0],"Turkiye":[35.2,38.9],"Uganda":[32.3,1.4],
    "Ukraine":[31.2,48.4],"United Arab Emirates":[53.8,23.4],
    "United Kingdom":[-1.2,52.5],"United States of America":[-98.0,38.0],
    "Uruguay":[-56.0,-32.5],"Uzbekistan":[63.9,41.4],"Vietnam":[108.3,14.1],
    "Yemen":[48.5,15.6],"Zambia":[27.9,-13.1],"Zimbabwe":[29.9,-19.0],
    "Angola":[17.9,-11.2],"Palestine":[35.3,31.9],"North Korea":[127.2,40.2],
    "Bosnia and Herzegovina":[17.7,44.2],
}

BASE = os.getcwd()

def read_csv(filename):
    path = os.path.join(BASE, filename)
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row["Name"].strip()
            try:
                gross = int(float(row["Gross Import"]))
                share = float(row["Share"])
            except:
                continue
            coord = COORDS.get(name)
            rows.append({
                "name": name,
                "gross": gross,
                "share": round(share, 4),
                "lon": coord[0] if coord else None,
                "lat": coord[1] if coord else None,
            })
    return sorted(rows, key=lambda x: -x["share"])

print("Reading CSVs...")
importers = read_csv("Who-imported-Salt-sulphur-lime-cement-etc-in-2024.csv")
india_src  = read_csv("Where-did-India-import-Salt-sulphur-lime-cement-etc-from-in-2024.csv")
china_src  = read_csv("Where-did-China-import-Salt-sulphur-lime-cement-etc-from-in-2024.csv")
usa_src    = read_csv("Where-did-the-United-States-of-America-import-Salt-sulphur-lime-cement-etc-from-in-2024.csv")

# totals
def total_usd(rows):
    return sum(r["gross"] for r in rows)

data = {
    "meta": {
        "title": "White Gold — Global Salt Trade 2024",
        "hs_chapter": "25",
        "year": 2024,
        "total_importers": len(importers),
        "global_market_usd": total_usd(importers),
        "india_total": next((r["gross"] for r in importers if r["name"]=="India"), 0),
        "china_total": next((r["gross"] for r in importers if r["name"]=="China"), 0),
        "usa_total": next((r["gross"] for r in importers if r["name"]=="United States of America"), 0),
    },
    "importers": importers,
    "india_sources": india_src,
    "china_sources": china_src,
    "usa_sources": usa_src,
    # Arc definitions for map: top N sources per country
    "arcs": {
        "india": [
            {"from": [r["lon"], r["lat"]], "to": [78.9, 20.6],
             "name": r["name"], "pct": r["share"], "usd": r["gross"]}
            for r in india_src if r["lon"] and r["share"] > 0.3
        ],
        "china": [
            {"from": [r["lon"], r["lat"]], "to": [104.2, 35.9],
             "name": r["name"], "pct": r["share"], "usd": r["gross"]}
            for r in china_src if r["lon"] and r["share"] > 0.4
        ],
        "usa": [
            {"from": [r["lon"], r["lat"]], "to": [-98.0, 39.0],
             "name": r["name"], "pct": r["share"], "usd": r["gross"]}
            for r in usa_src if r["lon"] and r["share"] > 0.4
        ],
    }
}

out = os.path.join(BASE, "data.json")
with open(out, "w") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print(f"Written: {out}")
print(f"  {len(importers)} importing countries")
print(f"  {len(india_src)} India source countries  →  {len(data['arcs']['india'])} arcs")
print(f"  {len(china_src)} China source countries  →  {len(data['arcs']['china'])} arcs")
print(f"  {len(usa_src)} USA source countries    →  {len(data['arcs']['usa'])} arcs")
print(f"  Global market: ${data['meta']['global_market_usd']:,.0f}")
