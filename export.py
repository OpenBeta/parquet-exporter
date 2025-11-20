#!/usr/bin/env python3
"""
OpenBeta Parquet Exporter
Exports climbing route data from OpenBeta GraphQL API to Parquet format.
"""

import json
import requests
import duckdb
import yaml
from pathlib import Path
from typing import Dict, List, Any
import sys

# GraphQL query to fetch all countries
COUNTRIES_QUERY = """
query GetCountries {
  countries {
    areaName
  }
}
"""

# GraphQL query to fetch sub-regions when a country times out
SUBREGIONS_QUERY = """
query GetSubregions($country: String!) {
  areas(filter: {path_tokens: {tokens: [$country], exactMatch: false}}) {
    pathTokens
  }
}
"""

# GraphQL query to fetch areas with climbs for a specific country or region
AREAS_QUERY = """
query GetAreas($tokens: [String!]!) {
  areas(filter: {leaf_status: {isLeaf: true}, path_tokens: {tokens: $tokens}}) {
    uuid
    area_name
    pathTokens
    metadata {
      lat
      lng
    }
    climbs {
      uuid
      name
      fa
      length
      boltsCount
      grades {
        yds
        vscale
        french
      }
      type {
        sport
        trad
        bouldering
        alpine
        tr
      }
      safety
      metadata {
        lat
        lng
      }
      content {
        description
      }
      pathTokens
    }
  }
}
"""

def load_config() -> Dict[str, Any]:
    """Load configuration from config.yaml"""
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)

def load_schema() -> str:
    """Load SQL schema from schema.sql"""
    schema_path = Path(__file__).parent / "schema.sql"
    return schema_path.read_text()

def fetch_subregions(api_url: str, country: str) -> List[List[str]]:
    """Fetch sub-regions for a country that's too large"""
    response = requests.post(
        api_url,
        json={"query": SUBREGIONS_QUERY, "variables": {"country": country}},
        headers={"Content-Type": "application/json"},
        timeout=120
    )

    if response.status_code != 200:
        return []

    data = response.json()
    if "errors" in data:
        return []

    # Extract unique next-level paths
    # e.g., [["USA"], ["USA", "California"], ["USA", "California", "Yosemite"]]
    # We want [["USA", "California"], ["USA", "Texas"], ...]
    areas = data.get("data", {}).get("areas", [])
    subregions = set()

    for area in areas:
        tokens = area.get("pathTokens", [])
        # Get the next level: [country, state/province]
        if len(tokens) >= 2:
            subregions.add(tuple(tokens[:2]))

    return [list(sr) for sr in sorted(subregions)]

def fetch_region_climbs(api_url: str, tokens: List[str]) -> List[Dict]:
    """Fetch climbs for a specific region (country or sub-region)"""
    response = requests.post(
        api_url,
        json={"query": AREAS_QUERY, "variables": {"tokens": tokens}},
        headers={"Content-Type": "application/json"},
        timeout=120
    )

    if response.status_code != 200:
        return None, response.status_code

    data = response.json()
    if "errors" in data:
        return None, "GraphQL Error"

    areas = data.get("data", {}).get("areas", [])
    climbs = []

    # Extract climbs from areas and flatten
    for area in areas:
        for climb in area.get("climbs", []):
            # Use area pathTokens if climb doesn't have them
            if not climb.get("pathTokens"):
                climb["pathTokens"] = area.get("pathTokens", [])

            # Add area coordinates if climb doesn't have them
            if not climb.get("metadata", {}).get("lat"):
                if area.get("metadata", {}).get("lat"):
                    climb.setdefault("metadata", {})["lat"] = area["metadata"]["lat"]
                    climb["metadata"]["lng"] = area["metadata"]["lng"]

            climbs.append(climb)

    return climbs, None

def fetch_all_climbs(api_url: str) -> List[Dict]:
    """Fetch all climbs from GraphQL API by querying each country separately"""
    print(f"Fetching countries from {api_url}...")

    # Countries known to be too large - always split these
    LARGE_COUNTRIES = {"USA", "Canada"}

    # Get all countries
    response = requests.post(
        api_url,
        json={"query": COUNTRIES_QUERY},
        headers={"Content-Type": "application/json"}
    )

    if response.status_code != 200:
        raise Exception(f"Countries query failed: {response.status_code} {response.text[:500]}")

    data = response.json()
    if "errors" in data:
        raise Exception(f"GraphQL errors: {data['errors']}")

    countries = [c["areaName"] for c in data.get("data", {}).get("countries", [])]
    print(f"Found {len(countries)} countries")

    all_climbs = []

    # Fetch climbs for each country
    for i, country in enumerate(countries, 1):
        print(f"  [{i}/{len(countries)}] Fetching {country}...")

        # Skip timeout for known large countries - go straight to sub-regions
        if country in LARGE_COUNTRIES:
            print(f"    Large country detected, fetching sub-regions...")
            subregions = fetch_subregions(api_url, country)

            if not subregions:
                print(f"    WARNING: Could not fetch sub-regions for {country}")
                continue

            print(f"    Found {len(subregions)} sub-regions")
            country_climbs = 0

            for subregion in subregions:
                region_name = " > ".join(subregion)
                sub_climbs, sub_error = fetch_region_climbs(api_url, subregion)

                if sub_error:
                    print(f"      WARNING: Failed to fetch {region_name}: {sub_error}")
                    continue

                all_climbs.extend(sub_climbs)
                country_climbs += len(sub_climbs)
                print(f"      {region_name}: {len(sub_climbs)} climbs")

            print(f"    {country} total: {country_climbs} climbs")
            continue

        # Try fetching the whole country first
        climbs, error = fetch_region_climbs(api_url, [country])

        # If timeout (502/504), break into sub-regions
        if error in [502, 504]:
            print(f"    Country too large, fetching sub-regions...")
            print(f"    NOTE: Consider adding '{country}' to LARGE_COUNTRIES list in export.py to skip this timeout in future runs")
            subregions = fetch_subregions(api_url, country)

            if not subregions:
                print(f"    WARNING: Could not fetch sub-regions for {country}")
                continue

            print(f"    Found {len(subregions)} sub-regions")
            country_climbs = 0

            for subregion in subregions:
                region_name = " > ".join(subregion)
                sub_climbs, sub_error = fetch_region_climbs(api_url, subregion)

                if sub_error:
                    print(f"      WARNING: Failed to fetch {region_name}: {sub_error}")
                    continue

                all_climbs.extend(sub_climbs)
                country_climbs += len(sub_climbs)
                print(f"      {region_name}: {len(sub_climbs)} climbs")

            print(f"    {country} total: {country_climbs} climbs")

        elif error:
            print(f"    WARNING: Failed to fetch {country}: {error}")
            continue
        else:
            # Success - got all climbs for this country
            all_climbs.extend(climbs)
            print(f"    {country}: {len(climbs)} climbs")

    print(f"\nTotal climbs fetched: {len(all_climbs)}")
    return all_climbs

def filter_climbs(climbs: List[Dict], config: Dict) -> List[Dict]:
    """Apply filters from config"""
    filters = config.get("export", {}).get("filters", {})
    regions = config.get("export", {}).get("regions", [])

    filtered = climbs

    # Filter by regions
    if regions:
        filtered = [
            c for c in filtered
            if c.get("pathTokens") and len(c["pathTokens"]) > 0 and c["pathTokens"][0] in regions
        ]
        print(f"Filtered to regions {regions}: {len(filtered)} climbs")

    return filtered

def export_to_parquet(climbs: List[Dict], config: Dict):
    """Convert climbs to Parquet using DuckDB"""
    output_config = config.get("export", {}).get("output", {})
    filename = output_config.get("filename", "openbeta-climbs.parquet")
    compression = output_config.get("compression", "snappy")

    print(f"\nTransforming data with DuckDB...")

    # Initialize DuckDB
    con = duckdb.connect(database=":memory:")

    # Load climbs as JSON via temp file
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
        json.dump(climbs, tmp)
        tmp_path = tmp.name

    try:
        # Measure JSON size for comparison
        json_size_mb = Path(tmp_path).stat().st_size / (1024 * 1024)
        print(f"  JSON intermediate size: {json_size_mb:.2f} MB")

        con.execute(f"CREATE TABLE climbs AS SELECT * FROM read_json_auto('{tmp_path}')")
        print(f"  Loaded {len(climbs)} climbs into DuckDB")
    finally:
        Path(tmp_path).unlink()  # Clean up temp file

    # Load and execute schema transformation
    schema_sql = load_schema()
    print(f"  Applying schema transformation...")

    result = con.execute(schema_sql)

    # Export to Parquet
    output_path = Path(filename)
    print(f"\nExporting to {output_path}...")

    con.execute(f"""
        COPY ({schema_sql})
        TO '{output_path}'
        (FORMAT PARQUET, COMPRESSION '{compression}')
    """)

    # Get file size and show comparison
    parquet_size_mb = output_path.stat().st_size / (1024 * 1024)
    compression_ratio = json_size_mb / parquet_size_mb if parquet_size_mb > 0 else 0
    space_saved_pct = (1 - parquet_size_mb / json_size_mb) * 100 if json_size_mb > 0 else 0

    print(f"Export complete: {output_path} ({parquet_size_mb:.2f} MB)")
    print(f"  Size comparison: JSON {json_size_mb:.2f} MB → Parquet {parquet_size_mb:.2f} MB")
    print(f"  Compression: {compression_ratio:.1f}x smaller ({space_saved_pct:.1f}% space saved)")

    # Write stats for GitHub Actions
    stats = {
        "total_climbs": len(climbs),
        "json_size_mb": round(json_size_mb, 2),
        "parquet_size_mb": round(parquet_size_mb, 2),
        "compression_ratio": round(compression_ratio, 1),
        "space_saved_pct": round(space_saved_pct, 1)
    }
    stats_path = Path("export-stats.json")
    stats_path.write_text(json.dumps(stats, indent=2))

    # Show sample
    print(f"\nSample data (first 5 rows):")
    sample = con.execute(f"SELECT * FROM ({schema_sql}) LIMIT 5").fetchdf()
    print(sample.to_string())

    con.close()

def main():
    """Main export process"""
    print("=" * 60)
    print("OpenBeta Parquet Exporter")
    print("=" * 60)

    try:
        # Load configuration
        config = load_config()
        api_url = config["export"]["api_url"]

        # Fetch data
        climbs = fetch_all_climbs(api_url)

        if not climbs:
            print("WARNING: No climbs found!")
            sys.exit(1)

        # Apply filters
        climbs = filter_climbs(climbs, config)

        if not climbs:
            print("WARNING: No climbs remained after filtering!")
            sys.exit(1)

        # Export to Parquet
        export_to_parquet(climbs, config)

        print("\nExport successful!")

    except Exception as e:
        print(f"\nERROR: Export failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
