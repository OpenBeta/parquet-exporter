# OpenBeta Parquet Exporter

Export climbing route data from [OpenBeta](https://openbeta.io) to Apache Parquet format.

## Quick Start - Download Data

**Latest export:** [Releases](https://github.com/OpenBeta/parquet-exporter/releases/latest)

Download `openbeta-climbs.parquet` and use it with any Parquet-compatible tool:

```python
# Python with DuckDB
import duckdb
df = duckdb.execute("SELECT * FROM 'openbeta-climbs.parquet' LIMIT 10").fetchdf()
print(df)
```

```r
# R with arrow
library(arrow)
df <- read_parquet('openbeta-climbs.parquet')
```

```sql
-- DuckDB
SELECT * FROM 'openbeta-climbs.parquet'
WHERE country = 'USA' AND state_province = 'California'
LIMIT 10;
```

## Data Format

Each row represents one climbing route with:

| Column | Type | Description |
|--------|------|-------------|
| `climb_id` | string | Unique identifier (UUID) |
| `climb_name` | string | Route name |
| `grade_yds` | string | Yosemite Decimal System grade (e.g., "5.10a") |
| `grade_vscale` | string | V-scale boulder grade (e.g., "V5") |
| `is_sport` | boolean | Sport climbing route |
| `is_trad` | boolean | Traditional climbing route |
| `is_boulder` | boolean | Boulder problem |
| `country` | string | Country name |
| `state_province` | string | State or province |
| `region` | string | Geographic region |
| `area` | string | Climbing area |
| `crag` | string | Specific crag or cliff |
| `latitude` | float | GPS latitude |
| `longitude` | float | GPS longitude |
| `length_meters` | int | Route length in meters |
| `bolts_count` | int | Number of bolts (sport routes) |
| `first_ascent` | string | First ascent information |
| `safety` | string | Safety rating |
| `description` | string | Route description |

See [schema.sql](schema.sql) for the complete definition.

## Customizing the Export

Want different fields or filters? You can customize and run your own export!

### Prerequisites

- Python 3.9+
- pip

### Installation

```bash
git clone https://github.com/OpenBeta/parquet-exporter.git
cd parquet-exporter
pip install -r requirements.txt
```

### Option 1: Edit Configuration

Edit `config.yaml` to change:
- Geographic regions to export
- Output filename
- Compression type (snappy, gzip, zstd)

```yaml
export:
  regions:
    - USA        # Change to your preferred regions
    - Canada

  output:
    filename: "my-custom-export.parquet"
    compression: "zstd"  # or snappy, gzip
```

### Option 2: Custom SQL Schema

Edit `schema.sql` to reshape the data:

```sql
-- Example: Filter to sport routes only
SELECT
    uuid AS climb_id,
    name AS climb_name,
    grades.yds AS grade,
    metadata.lat AS latitude,
    metadata.lng AS longitude

FROM climbs
WHERE type.sport = true
```

### Run Your Custom Export

```bash
python export.py
```

Output will be saved to the filename specified in `config.yaml`.

## Example Schemas

The `examples/` directory contains ready-to-use schema variations:

### Minimal (smallest file)
```bash
cp examples/schema-minimal.sql schema.sql
python export.py
```
Just climb name, grade, and coordinates. Smallest possible file size.

### Extended (all metadata)
```bash
cp examples/schema-extended.sql schema.sql
python export.py
```
All available fields including descriptions, multiple grade systems, and full location hierarchy.

### USA Sport Routes Only
```bash
cp examples/schema-usa-sport-only.sql schema.sql
python export.py
```
Filtered to just sport climbing routes in the United States.

## File Sizes

Approximate sizes for different configurations:

| Schema | Rows | File Size | Use Case |
|--------|------|-----------|----------|
| Minimal | 200k climbs | ~15 MB | Lightweight applications |
| Standard | 200k climbs | ~40 MB | General analysis |
| Extended | 200k climbs | ~80 MB | Comprehensive research |

All with Snappy compression. Use `zstd` in config for 20-30% smaller files.

## Data Updates

This export runs **weekly** (Sundays at midnight UTC). Each release is versioned by date: `v2024-11-19`.

To get the latest:
```bash
# Download latest release programmatically
curl -s https://api.github.com/repos/OpenBeta/parquet-exporter/releases/latest \
  | grep "browser_download_url.*parquet" \
  | cut -d : -f 2,3 \
  | tr -d \" \
  | wget -qi -
```

## Data Source

All data comes from the [OpenBeta GraphQL API](https://api.openbeta.io/graphql).

OpenBeta is a free, crowd-sourced climbing route database. Learn more at [openbeta.io](https://openbeta.io).

## License

- **Code:** MIT License
- **Data:** OpenBeta data is available under [CC0 (Public Domain)](https://creativecommons.org/publicdomain/zero/1.0/)

You can use this data for any purpose, including commercial applications, without restriction.

## Support

- **Issues:** [GitHub Issues](https://github.com/OpenBeta/parquet-exporter/issues)
- **OpenBeta Discord:** [discord.gg/ptpnWWNkJx](https://discord.gg/ptpnWWNkJx)
- **API Docs:** [docs.openbeta.io](https://docs.openbeta.io)

## Contributing

PRs welcome! Especially for:
- Additional example schemas
- Performance improvements
- Better error handling
- Documentation improvements

---

Built with ❤️ by the OpenBeta community
