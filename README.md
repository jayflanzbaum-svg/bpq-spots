# bpq-spots

Live band-activity "spots" app for BPQ32 packet radio nodes, fed by the
PSKReporter MQTT firehose.

Users connect to your node, type `SPOTS`, enter their Maidenhead grid once,
and get a live glance of how many unique stations were spotted on each band
near them in the last 15 minutes — plus per-band propagation analysis.

```
LIVE SPOTS-15minutes  Grid:EL96
80m:12  | 40m:47
30m:9   | 20m:83
17m:21  | 15m:34
12m:8   | 10m:26
6m:3    | 2m:1
<ENTER> | GRID <FM19ab> | CLEAR | ANALYZE <band> | HELP | QUIT
```

## Features

- **Live PSKReporter data** over MQTT — no polling, no API keys
- **Near-me filtering** — counts stations heard by receivers within 300 miles
  of the user's grid
- **Per-user memory** — each callsign's grid is remembered between connects
- **ANALYZE <band>** — synopsis (regional vs long-haul vs intercontinental,
  rough MUF read) plus the top paths with distances
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
   `C 7` is your Telnet port number; `HOST 0` is the CMDPORT position; the
   `S` flag makes BPQ send the connecting user's callsign to the app.
4. Restart BPQ32, start the app (a startup batch file or a Windows Terminal
   tab works well), and type `SPOTS` at your node prompt.

## Data source

Spot data comes from [PSKReporter](https://pskreporter.info/)'s MQTT feed.
Please be a good citizen of their free service.

## License

MIT — see [LICENSE](LICENSE).
