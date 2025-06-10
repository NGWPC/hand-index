INSTALL httpfs;
LOAD httpfs;

INSTALL aws; -- This extension handles S3 authentication and specific S3 requests
LOAD aws;

INSTALL spatial;
LOAD spatial;

INSTALL h3 FROM community;
LOAD h3;

CREATE OR REPLACE TABLE catchments AS
SELECT * FROM read_parquet('s3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/catchments.geoparquet');

CREATE OR REPLACE TABLE hydrotables AS
SELECT * FROM read_parquet('s3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/hydrotables.parquet');

CREATE OR REPLACE TABLE hand_catchment_rasters AS
SELECT * FROM read_parquet('s3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/hand_catchment_rasters.parquet');

CREATE OR REPLACE TABLE hand_rem_rasters AS
SELECT * FROM read_parquet('s3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/hand_rem_rasters.parquet');

-- create an rtree index on the catchment geometries
CREATE INDEX if not exists catchments_geom_idx
  ON catchments
  USING RTREE (geometry);
CREATE INDEX IF NOT EXISTS idx_hydro_catchment_id ON hydrotables (catchment_id);
CREATE INDEX IF NOT EXISTS idx_hrr_catchment_id ON hand_rem_rasters (catchment_id);
CREATE INDEX IF NOT EXISTS idx_hcr_rem_raster_id ON hand_catchment_rasters (rem_raster_id);

-- begin partitioning process to partition tables and create H3 to catchment id 
-- lookup table
SET VARIABLE h3_resolution = 1; -- Resolution for both centroid partitioning and lookup table

COPY (
    SELECT
        c.*,
        -- Get centroid, transform to EPSG:4326, then get H3 cell at resolution 1
        h3_latlng_to_cell(
            ST_Y(ST_Transform(ST_Centroid(c.geometry), 'EPSG:5070', 'EPSG:4326', true)), -- Latitude
            ST_X(ST_Transform(ST_Centroid(c.geometry), 'EPSG:5070', 'EPSG:4326', true)), -- Longitude
            getvariable('h3_resolution')
        ) AS h3_partition_key
    FROM catchments c
) TO 's3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/partitioned/catchments/'
WITH (FORMAT PARQUET, PARTITION_BY (h3_partition_key), OVERWRITE_OR_IGNORE 1);

CREATE TEMP TABLE catchment_h3_map AS
SELECT
    catchment_id,
    h3_latlng_to_cell(
        ST_Y(ST_Transform(ST_Centroid(geometry), 'EPSG:5070', 'EPSG:4326', true)),
        ST_X(ST_Transform(ST_Centroid(geometry), 'EPSG:5070', 'EPSG:4326', true)),
        getvariable('h3_resolution')
    ) AS h3_partition_key
FROM catchments;

COPY (
    SELECT
        ht.*,
        chm.h3_partition_key
    FROM hydrotables ht
    JOIN catchment_h3_map chm ON ht.catchment_id = chm.catchment_id
) TO 's3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/partitioned/hydrotables/'
WITH (FORMAT PARQUET, PARTITION_BY (h3_partition_key), OVERWRITE_OR_IGNORE 1);

COPY (
    SELECT
        hrr.*,
        chm.h3_partition_key
    FROM hand_rem_rasters hrr
    JOIN catchment_h3_map chm ON hrr.catchment_id = chm.catchment_id
) TO 's3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/partitioned/hand_rem_rasters.parquet'

-- create a map from rem_raster_id to h3_partition_key
CREATE TEMP TABLE rem_raster_h3_map AS
SELECT DISTINCT
    hrr.rem_raster_id,
    chm.h3_partition_key
FROM hand_rem_rasters hrr
JOIN catchment_h3_map chm ON hrr.catchment_id = chm.catchment_id;

COPY (
    SELECT
        hcr.*,
        rhm.h3_partition_key
    FROM hand_catchment_rasters hcr
    JOIN rem_raster_h3_map rhm ON hcr.rem_raster_id = rhm.rem_raster_id
) TO 's3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/partitioned/hand_catchment_rasters.parquet';

DROP TABLE catchment_h3_map;
DROP TABLE rem_raster_h3_map;

-- Create a lookup table mapping each catchment to all H3 cells that contain the centroid or are adjacent to the centroid containing cell.
COPY (
    WITH CatchmentCentroids AS (
        -- First, get the H3 cell for the centroid of each catchment
        SELECT
            c.catchment_id,
            h3_latlng_to_cell(
                ST_Y(ST_Transform(ST_Centroid(c.geometry), 'EPSG:5070', 'EPSG:4326', true)), -- Latitude
                ST_X(ST_Transform(ST_Centroid(c.geometry), 'EPSG:5070', 'EPSG:4326', true)), -- Longitude
                getvariable('h3_resolution')
            ) as h3_centroid_cell
        FROM catchments c
        WHERE c.geometry IS NOT NULL AND NOT ST_IsEmpty(c.geometry)
    )
    SELECT
        cc.catchment_id,
        -- For each centroid cell, get the cell itself and all neighbors in a 1-cell radius (k=1)
        unnest(h3_grid_disk(cc.h3_centroid_cell, 1)) AS h3_covering_cell_key
    FROM
        CatchmentCentroids cc
) TO 's3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/partitioned/catchment_h3_lookup.parquet'
WITH (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1);

