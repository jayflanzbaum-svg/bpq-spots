"""Build places.tsv (used to name grid squares) from GeoNames.

    1. Download https://download.geonames.org/export/dump/cities15000.zip
    2. Unzip it and run:  py tools/build_places.py <path>/cities15000.txt

Writes places.tsv next to bpq_spots.py: one town per line,
name <TAB> region <TAB> lat <TAB> lon <TAB> population, where region is the
state for US towns ("IL") and the country code elsewhere ("JP"), spelled
out where it is also a state code ("Colombia", not "CO").
GeoNames data is CC BY 4.0 (https://www.geonames.org/).
"""
import sys
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "places.tsv"

# Country codes that are also US state codes: "Villavicencio CO" would read
# as Colorado, so these countries are spelled out.
SPELLED = {
    "AL": "Albania", "AR": "Argentina", "AZ": "Azerbaijan", "CA": "Canada",
    "CO": "Colombia", "DE": "Germany", "GA": "Gabon", "ID": "Indonesia",
    "IL": "Israel", "IN": "India", "KY": "Cayman", "LA": "Laos",
    "MA": "Morocco", "MD": "Moldova", "ME": "Montenegro", "MN": "Mongolia",
    "MO": "Macao", "MS": "Montserrat", "MT": "Malta", "NC": "NewCaledonia",
    "NE": "Niger", "PA": "Panama", "SC": "Seychelles", "SD": "Sudan",
    "TN": "Tunisia", "VA": "Vatican",
}


def main(src: str) -> None:
    rows = []
    with open(src, encoding="utf-8") as f:
        for line in f:
            c = line.rstrip("\n").split("\t")
            if len(c) < 15:
                continue
            name, lat, lon, cc, admin1, pop = c[2], c[4], c[5], c[8], c[10], c[14]
            region = admin1 if cc == "US" and admin1 else SPELLED.get(cc, cc)
            if not name or not region:
                continue
            rows.append(f"{name}\t{region}\t{float(lat):.2f}\t{float(lon):.2f}\t{int(pop or 0)}")
    OUT.write_text("\n".join(rows) + "\n", encoding="utf-8", newline="\n")
    print(f"wrote {len(rows)} places to {OUT}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
