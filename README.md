# bpq-spots

Live band-activity "spots" app for BPQ32 packet radio nodes, fed by the
PSKReporter MQTT firehose.

Users connect to your node, type `SPOTS`, enter their Maidenhead grid once,
and get a live glance of how many unique stations were spotted on each band
near them in the last 15 minutes — plus per-band propagation analysis.

```
From Past 15 Min, Grid EL96XL
BAND/SPOTS
80m/0|40m/8|30m/3|20m/23|17m/1|15m/7|12m/1
10m/15|6m/0|2m/0
```

`ANALYZE 20m` names where the signals come from, with direction and miles:

```
Top paths:
  PLACE              DIR MILES SPOTS
  Baltimore MD       N     920     4
  Dallas TX          NW   1100     3
  Bogota Colombia    S    1590     2
```

## Features

- **Live PSKReporter data** over MQTT — no polling, no API keys
- **Near-me filtering** — counts stations heard by receivers within 300 miles
  of the user's grid
- **Per-user memory** — each callsign's grid is remembered between connects
- **ANALYZE <band>** — synopsis (regional vs long-haul vs intercontinental,
  rough MUF read) plus the top paths: the biggest town in each grid square
  (`Chicago IL` rather than `EN61`), compass direction and miles
- Compact deduplicated in-memory index with background pruning — runs for
  weeks without growing
- RF-friendly output: short lines, plain ASCII

## Requirements

- Python 3.10+
- `pip install paho-mqtt` (and optionally `psutil` for memory stats)
- A BPQ32 node with a Telnet port

## Install

1. Clone this repo somewhere on the node PC.
2. Run it once to generate `spots_config.json`:
   ```
   python bpq_spots.py
   ```
   The defaults subscribe to PSKReporter's public MQTT broker
   (`mqtt.pskreporter.info`) for 80m-2m. Adjust bands/port in the config if
   you like.
3. Add the app to `BPQ32.cfg`:

   In your **Telnet port** `CONFIG` block, add the listen port to `CMDPORT`
   (space-separated list; note its zero-based position — that's the HOST
   number):
   ```
   CMDPORT=63050
   ```
   In the **APPLICATIONS** section (adjust the application number, HOST
   index, and your callsign/alias):
   ```
   APPLICATION 4,SPOTS,C 7 HOST 0 S,MYCALL-17,NODESPT,255
   ```
   `C 7` is your Telnet port number; `HOST 0` is the CMDPORT position; `S`
   returns the user to the node prompt when the app exits. BPQ sends the
   connecting user's callsign to the app automatically.
4. Restart BPQ32, start the app (a startup batch file or a Windows Terminal
   tab works well), and type `SPOTS` at your node prompt.

## Data source

Spot data comes from [PSKReporter](https://pskreporter.info/)'s MQTT feed.
Please be a good citizen of their free service.

Place names come from [GeoNames](https://www.geonames.org/) (towns of
15,000+, CC BY 4.0), reduced to `places.tsv` by `tools/build_places.py`.
Without `places.tsv` the app shows grid squares instead.

## License

MIT — see [LICENSE](LICENSE).
