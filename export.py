#!/usr/bin/env python3
"""
OpenBeta Parquet Exporter
Exports climbing route data from OpenBeta GraphQL API to Parquet format.
"""

import json
import requests
import duckdb
import yaml
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
import sys

# Countries known to be too large for single query - skip straight to children
LARGE_REGIONS = {"USA", "Canada"}

# GraphQL query to fetch all countries with UUIDs
COUNTRIES_QUERY = """
query GetCountries {
  countries {
    areaName
    uuid
  }
}
"""

# GraphQL query to get children by UUID
CHILDREN_BY_UUID_QUERY = """
query GetChildren($uuid: ID!) {
  area(uuid: $uuid) {
    children {
      areaName
    }
  }
}
"""

# GraphQL query to get area UUID and children by path
CHILDREN_BY_PATH_QUERY = """
query GetAreaByPath($tokens: [String!]!) {
  areas(filter: {path_tokens: {tokens: $tokens, exactMatch: true}, leaf_status: {isLeaf: false}}) {
    uuid
    children {
      areaName
    }
  }
}
"""

# GraphQL query to fetch areas with climbs for a specific country or region
# Uses pagination (limit/offset) since the API defaults to 50 results
AREAS_QUERY = """
query GetAreas($tokens: [String!]!, $limit: Int!, $offset: Int!) {
  areas(filter: {leaf_status: {isLeaf: true}, path_tokens: {tokens: $tokens}}, limit: $limit, offset: $offset) {
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

# Pagination settings
AREAS_PAGE_SIZE = 500  # Max allowed by API

def load_config() -> Dict[str, Any]:
    """Load configuration from config.yaml"""
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)

def load_schema() -> str:
    """Load SQL schema from schema.sql"""
    schema_path = Path(__file__).parent / "schema.sql"
    return schema_path.read_text()

def fetch_children_by_uuid(api_url: str, uuid: str) -> List[str]:
    """Fetch child area names using UUID"""
    try:
        response = requests.post(
            api_url,
            json={"query": CHILDREN_BY_UUID_QUERY, "variables": {"uuid": uuid}},
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        if response.status_code != 200:
            return []
        data = response.json()
        if "errors" in data:
            return []
        children = data.get("data", {}).get("area", {}).get("children", [])
        return [c["areaName"] for c in children]
    except:
        return []

def fetch_children_by_path(api_url: str, tokens: List[str]) -> List[str]:
    """Fetch child area names using path tokens"""
    try:
        response = requests.post(
            api_url,
            json={"query": CHILDREN_BY_PATH_QUERY, "variables": {"tokens": tokens}},
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        if response.status_code != 200:
            return []
        data = response.json()
        if "errors" in data:
            return []
        areas = data.get("data", {}).get("areas", [])
        if not areas:
            return []
        return [c["areaName"] for c in areas[0].get("children", [])]
    except:
        return []

def fetch_region_climbs(api_url: str, tokens: List[str]) -> Tuple[Optional[List[Dict]], Optional[Any]]:
    """Fetch climbs for a specific region (country or sub-region) with pagination"""
    all_climbs = []
    offset = 0
    total_areas = 0

    while True:
        try:
            response = requests.post(
                api_url,
                json={
                    "query": AREAS_QUERY,
                    "variables": {
                        "tokens": tokens,
                        "limit": AREAS_PAGE_SIZE,
                        "offset": offset
                    }
                },
                headers={"Content-Type": "application/json"},
                timeout=120
            )
        except requests.Timeout:
            if offset == 0:
                # First page timed out - signal to split into children
                return None, 504
            else:
                # Already got some data, return what we have
                print(f"    WARNING: Timeout at offset {offset}, returning {len(all_climbs)} climbs")
                return all_climbs, None

        if response.status_code != 200:
            if offset == 0:
                return None, response.status_code
            else:
                print(f"    WARNING: Error at offset {offset}, returning {len(all_climbs)} climbs")
                return all_climbs, None

        data = response.json()
        if "errors" in data:
            if offset == 0:
                return None, "GraphQL Error"
            else:
                print(f"    WARNING: GraphQL error at offset {offset}, returning {len(all_climbs)} climbs")
                return all_climbs, None

        areas = data.get("data", {}).get("areas", [])
        total_areas += len(areas)

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

        # Check if we've fetched all pages
        if len(areas) < AREAS_PAGE_SIZE:
            break

        offset += AREAS_PAGE_SIZE

        # Progress indicator for large regions
        if offset % 1000 == 0:
            print(f"    ... fetched {total_areas} areas, {len(all_climbs)} climbs so far")

    return all_climbs, None

def fetch_region(api_url: str, tokens: List[str], uuid: str = None, depth: int = 0) -> List[Dict]:
    """Recursively fetch climbs, splitting into children on timeout"""
    indent = "  " * (depth + 1)
    region_name = " > ".join(tokens)

    # known large regions - skip straight to children
    if tokens[-1] in LARGE_REGIONS and uuid:
        print(f"{indent}{region_name}: splitting (known large region)")
        children = fetch_children_by_uuid(api_url, uuid)
        if not children:
            print(f"{indent}  WARNING: no children found")
            return []
        print(f"{indent}  found {len(children)} children")
        all_climbs = []
        for child in children:
            all_climbs.extend(fetch_region(api_url, tokens + [child], depth=depth + 1))
        return all_climbs

    # try fetching climbs directly
    climbs, error = fetch_region_climbs(api_url, tokens)

    if error not in [502, 504]:
        if error:
            print(f"{indent}{region_name}: failed ({error})")
            return []
        print(f"{indent}{region_name}: {len(climbs):,} climbs")
        return climbs

    # timeout - split into children
    print(f"{indent}{region_name}: timeout, splitting into children...")

    if uuid:
        children = fetch_children_by_uuid(api_url, uuid)
    else:
        children = fetch_children_by_path(api_url, tokens)

    if not children:
        print(f"{indent}  WARNING: no children found for {region_name}")
        return []

    print(f"{indent}  found {len(children)} children")
    all_climbs = []
    for child in children:
        all_climbs.extend(fetch_region(api_url, tokens + [child], depth=depth + 1))
    return all_climbs

def fetch_all_climbs(api_url: str) -> List[Dict]:
    """Fetch all climbs from GraphQL API"""
    print(f"Fetching countries from {api_url}...")

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

    countries = data.get("data", {}).get("countries", [])
    print(f"Found {len(countries)} countries")

    all_climbs = []
    for i, country in enumerate(countries, 1):
        name, uuid = country["areaName"], country["uuid"]
        print(f"[{i}/{len(countries)}] {name}")
        climbs = fetch_region(api_url, [name], uuid=uuid, depth=0)
        all_climbs.extend(climbs)

    print(f"\nTotal climbs fetched: {len(all_climbs)}")
    return all_climbs

def filter_climbs(climbs: List[Dict], config: Dict) -> List[Dict]:
    """Filter climbs by configured regions"""
    regions = config.get("export", {}).get("regions", [])
    if not regions:
        return climbs

    filtered = [c for c in climbs if c.get("pathTokens") and c["pathTokens"][0] in regions]
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
    result = con.execute(f"SELECT * FROM ({schema_sql}) LIMIT 5")
    cols = [d[0] for d in result.description]
    rows = result.fetchall()
    print(" | ".join(cols))
    print("-" * min(120, len(" | ".join(cols))))
    for row in rows:
        print(" | ".join(str(v)[:30] for v in row))

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
