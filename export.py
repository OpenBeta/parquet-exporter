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

# GraphQL query to fetch areas with climbs
AREAS_QUERY = """
query GetAreas {
  areas(filter: {leaf_status: {isLeaf: true}}) {
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

def fetch_all_climbs(api_url: str) -> List[Dict]:
    """Fetch all climbs from GraphQL API"""
    print(f"Fetching climbs from {api_url}...")

    response = requests.post(
        api_url,
        json={"query": AREAS_QUERY},
        headers={"Content-Type": "application/json"}
    )

    if response.status_code != 200:
        raise Exception(f"GraphQL query failed: {response.status_code} {response.text}")

    data = response.json()

    if "errors" in data:
        raise Exception(f"GraphQL errors: {data['errors']}")

    areas = data.get("data", {}).get("areas", [])
    all_climbs = []

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

            all_climbs.append(climb)

    print(f"✓ Total climbs fetched: {len(all_climbs)}")
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
        print(f"✓ Filtered to regions {regions}: {len(filtered)} climbs")

    return filtered

def export_to_parquet(climbs: List[Dict], config: Dict):
    """Convert climbs to Parquet using DuckDB"""
    output_config = config.get("export", {}).get("output", {})
    filename = output_config.get("filename", "openbeta-climbs.parquet")
    compression = output_config.get("compression", "snappy")

    print(f"\nTransforming data with DuckDB...")

    # Initialize DuckDB
    con = duckdb.connect(database=":memory:")

    # Load climbs as JSON
    con.execute("CREATE TABLE climbs AS SELECT * FROM read_json_auto(?)", [json.dumps(climbs)])

    print(f"  Loaded {len(climbs)} climbs into DuckDB")

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

    # Get file size
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"✓ Export complete: {output_path} ({size_mb:.2f} MB)")

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
            print("⚠ No climbs found!")
            sys.exit(1)

        # Apply filters
        climbs = filter_climbs(climbs, config)

        if not climbs:
            print("⚠ No climbs remained after filtering!")
            sys.exit(1)

        # Export to Parquet
        export_to_parquet(climbs, config)

        print("\n✓ Export successful!")

    except Exception as e:
        print(f"\n✗ Export failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
