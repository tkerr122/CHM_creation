# Create_CHM

> ⚠️ **Work in Progress** — This script is currently under active development and testing. The full processing pipeline is implemented but not yet fully enabled (see [Current Status](#current-status) below).

A Python script for creating Canopy Height Models (CHMs) from raw LiDAR `.laz` files using the `pylasr` library.

---

## Scripts

| Script | Purpose |
|---|---|
| `create_CHM.py` | Processes a LiDAR survey directory into a single CHM GeoTIFF |

---

## Intended Processing Pipeline

Once complete, the script will execute the following steps in sequence:

1. **Load point cloud** — reads all `.laz` files in the input directory
2. **Remove noise** — deletes points classified as low noise (class 7) and high noise (class 18)
3. **Remove tall outliers** — deletes points above 50 m
4. **Generate DTM** — triangulates ground points (class 2) to produce a Digital Terrain Model
5. **Normalize point cloud** — subtracts the DTM to produce height-above-ground values
6. **Generate CHM** — rasterizes the normalized cloud at 4 m resolution using a 2 m window and max operator, saved as a GeoTIFF

---

## Current Status

The full pipeline is defined in code but currently commented out. The script presently executes only the **reader and info steps**, which loads the point cloud and returns metadata without producing a CHM output.

The active pipeline:
```python
pipeline = las + info  # Testing only
```

The intended pipeline (currently commented out):
```python
pipeline = las + low_noise + high_noise + above_threshold + dtm + nlas + chm
pipeline.set_concurrent_files_strategy(ncores=cores)
```

---

## CLI Flags

```
python create_CHM.py -s <survey_name> [options]
```

| Flag | Long form | Default | Required | Description |
|---|---|---|---|---|
| `-s` | `--survey` | — | Yes | Survey folder name |
| `-c` | `--cores` | `30` | No | Number of cores for parallel processing |

---

## Input / Output Paths

Input and output paths are currently hardcoded:

| | Path |
|---|---|
| **Input LAZ files** | `/gpfs/glad1/Theo/Data/Lidar/LAZ/<survey>/` |
| **Output CHM** | `/gpfs/glad1/Theo/Data/Lidar/CHMs_raw/<survey>/<survey>.tif` |

The output directory is created automatically if it doesn't exist.

---

## Dependencies

- `pylasr` — LiDAR point cloud processing pipeline
- `argparse`, `os` — standard library

---

## Environment

Designed to run on a Linux cluster. Ensure `pylasr` is installed in your environment before running.