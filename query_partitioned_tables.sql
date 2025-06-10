-- Define views for your partitioned tables
CREATE OR REPLACE VIEW catchments1 AS
SELECT * FROM read_parquet('s3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/partitioned/catchments/*/*.parquet', hive_partitioning = 1);

CREATE OR REPLACE VIEW hydrotables1 AS
SELECT * FROM read_parquet('s3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/partitioned/hydrotables/*/*.parquet', hive_partitioning = 1);

CREATE OR REPLACE VIEW hand_rem_rasters1 AS
SELECT * FROM read_parquet('s3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/partitioned/hand_rem_rasters.parquet');

CREATE OR REPLACE VIEW hand_catchment_rasters1 AS
SELECT * FROM read_parquet('s3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/partitioned/hand_catchment_rasters.parquet');

CREATE OR REPLACE VIEW catchment_h3_lookup_partitioned AS
SELECT * FROM read_parquet('s3://fimc-data/autoeval/hand_output_indices/PI3_uat_and_alpha_domain_3m_wbt/partitioned/catchment_h3_lookup.parquet');


-- query that uses catchment to h3 lookup table. Find all data for catchments intersecting an ROI 

-- Define the ROI
CREATE or replace TEMP TABLE query_envelope_geom AS
SELECT ST_Transform(
    ST_MakeEnvelope(-85.605165, 30.357851, -80.839729, 35.000659),
    'EPSG:4326',
    'EPSG:5070', -- The CRS of your 'catchments' geometry
    true
) AS geom;

-- Get all H3 cells that cover the ROI at the resolution of your lookup table.
CREATE or replace TEMP TABLE query_h3_cells AS
SELECT unnest(
    h3_polygon_wkt_to_cells(
        ST_AsText(ST_Transform((SELECT geom FROM query_envelope_geom), 'EPSG:5070', 'EPSG:4326', true)),
        1 -- Must match the resolution used to create the lookup table
    )
) AS h3_query_cell_id;

WITH
-- Find candidate catchments.
-- These are catchments whose covering H3 cells from the lookup table
-- intersect with the ROI's H3 cells.
candidate_catchment_ids AS (
    SELECT DISTINCT chl.catchment_id
    FROM catchment_h3_lookup_partitioned chl
    JOIN query_h3_cells qhc ON chl.h3_covering_cell_key = qhc.h3_query_cell_id
),

-- Perform the full-geometry intersection on pruned set of partitioned tables
intersecting_catchments AS (
    SELECT
        c.catchment_id,
        c.h3_partition_key -- This is the partition key, we need it for subsequent joins
    FROM catchments1 c
    -- First, filter down to our candidate set. This is a fast operation.
    JOIN candidate_catchment_ids cc ON c.catchment_id = cc.catchment_id
    WHERE
        -- Now, perform the expensive, exact intersection test ONLY on the candidate geometries.
        ST_Intersects(c.geometry, (SELECT geom FROM query_envelope_geom))
)

-- Join the final, filtered catchment IDs with the other partitioned tables.
-- We use the `h3_partition_key` from the `intersecting_catchments` CTE to ensure
-- partition pruning is also applied to these joins.
SELECT
    i.catchment_id,
    h.* EXCLUDE (catchment_id, h3_partition_key),
    hrr.rem_raster_id, -- Keep the rem_raster_id for the next join
    hcr.raster_path AS catchment_raster_path
FROM
    intersecting_catchments i
-- Join with hydrotables, using the partition key for pruning.
LEFT JOIN
    hydrotables1 h ON i.catchment_id = h.catchment_id
    AND i.h3_partition_key = h.h3_partition_key

-- Join with hand_rem_rasters. 
LEFT JOIN
    hand_rem_rasters1 hrr ON i.catchment_id = hrr.catchment_id

-- Join with hand_catchment_rasters.
LEFT JOIN
    hand_catchment_rasters1 hcr ON hrr.rem_raster_id = hcr.rem_raster_id
;
