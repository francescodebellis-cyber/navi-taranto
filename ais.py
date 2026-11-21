"""Raccoglie dati AIS da aisstream.io per qualche decina di secondi e
scrive site/data.json per l'app Navi per Taranto (GitHub Pages).
Gira su GitHub Actions: la chiave resta nel secret AISSTREAM_KEY."""
import asyncio, json, os, shutil, time, urllib.request
from datetime import datetime, timezone

import websockets

KEY = os.environ["AISSTREAM_KEY"].strip()
SITE = os.environ.get("SITE_URL", "").rstrip("/")
DUR = int(os.environ.get("DURATA", "200"))
# Mediterraneo centro-orientale + Adriatico e Ionio (lat, lon)
BOXES = [[[30.0, 7.5], [46.5, 36.5]]]
TARANTO = (40.38, 17.05, 40.52, 17.32)  # area porto e rada: qui si tiene tutto
NOW = time.time()


def load_prev():
    if not SITE:
        return {}
    try:
        with urllib.request.urlopen(f"{SITE}/data.json?t={int(NOW)}", timeout=20) as r:
            d = json.load(r)
        return {str(s["mmsi"]): s for s in d.get("ships", [])}
    except Exception as e:  # primo giro o sito non ancora pubblicato
        print("nessun data.json precedente:", e)
        return {}


def g(d, *keys):
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] not in (None, ""):
            return d[k]
    return None


def in_taranto(lat, lon):
    return lat is not None and TARANTO[0] <= lat <= TARANTO[2] and TARANTO[1] <= lon <= TARANTO[3]


async def collect(ships):
    url = "wss://stream.aisstream.io/v0/stream"
    sub = {"APIKey": KEY, "BoundingBoxes": BOXES,
           "FilterMessageTypes": ["PositionReport", "ShipStaticData"]}
    n = 0
    async with websockets.connect(url, compression="deflate", max_size=2**22, open_timeout=20) as ws:
        await ws.send(json.dumps(sub))
        end = time.time() + DUR
        while time.time() < end:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=max(1, end - time.time()))
            except asyncio.TimeoutError:
                break
            try:
                m = json.loads(raw)
            except ValueError:
                continue
            if "error" in m:
                raise SystemExit("aisstream: " + str(m["error"]))
            md = m.get("MetaData", {})
            mmsi = str(g(md, "MMSI", "MMSI_String") or "")
            if not mmsi:
                continue
            s = ships.setdefault(mmsi, {"mmsi": mmsi})
            name = (g(md, "ShipName") or "").strip()
            if name:
                s["name"] = name
            t = m.get("MessageType")
            body = m.get("Message", {}).get(t, {})
            if t == "PositionReport":
                lat = g(body, "Latitude") or g(md, "latitude", "Latitude")
                lon = g(body, "Longitude") or g(md, "longitude", "Longitude")
                if lat is None or lon is None or abs(lat) > 90 or abs(lon) > 180:
                    continue
                s.update(lat=round(lat, 5), lon=round(lon, 5), t=int(time.time()),
                         sog=g(body, "Sog"), cog=g(body, "Cog"),
                         hdg=None if g(body, "TrueHeading") in (None, 511) else g(body, "TrueHeading"),
                         nav=g(body, "NavigationalStatus"))
            elif t == "ShipStaticData":
                dim = body.get("Dimension") or {}
                eta = body.get("Eta") or {}
                s.update(
                    imo=g(body, "ImoNumber") or s.get("imo"),
                    cs=(g(body, "CallSign") or "").strip() or s.get("cs"),
                    type=g(body, "Type"),
                    len=(dim.get("A", 0) or 0) + (dim.get("B", 0) or 0) or s.get("len"),
                    beam=(dim.get("C", 0) or 0) + (dim.get("D", 0) or 0) or s.get("beam"),
                    draught=g(body, "MaximumStaticDraught"),
                    dest=(g(body, "Destination") or "").strip().strip("@ ") or None,
                    eta=[eta.get("Month"), eta.get("Day"), eta.get("Hour"), eta.get("Minute")] if eta.get("Month") else None,
                    ts=int(time.time()))
            n += 1
    return n


def keep(s):
    typ = s.get("type")
    age = NOW - (s.get("t") or s.get("ts") or 0)
    tanker = isinstance(typ, int) and 80 <= typ <= 89
    if in_taranto(s.get("lat"), s.get("lon")):
        return age < 2 * 86400
    if tanker:
        return age < 4 * 86400
    if typ is None:  # tipo non ancora noto: lo teniamo poco (solo vicino a Taranto), in attesa dei dati statici
        lat, lon = s.get("lat"), s.get("lon")
        near = lat is not None and 37.5 <= lat <= 41.5 and 15.5 <= lon <= 20.5
        return near and age < 3 * 3600
    return False


def main():
    ships = load_prev()
    n = 0
    for attempt in range(2):
        try:
            n = asyncio.run(collect(ships))
            break
        except SystemExit:
            raise
        except Exception as e:
            print("errore connessione:", e)
            time.sleep(5)
    out = [s for s in ships.values() if keep(s)]
    data = {"updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "messages": n, "ships": out}
    os.makedirs("site", exist_ok=True)
    with open("site/data.json", "w") as f:
        json.dump(data, f, separators=(",", ":"))
    shutil.copy("index.html", "site/index.html")
    if os.path.exists("particulars.json"):
        shutil.copy("particulars.json", "site/particulars.json")
    open("site/.nojekyll", "w").close()
    print(f"messaggi {n}, navi salvate {len(out)}")


if __name__ == "__main__":
    main()
