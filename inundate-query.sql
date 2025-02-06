WITH intersecting_catchments AS (
    SELECT DISTINCT c.catchment_id
    FROM Catchments c
    WHERE ST_Intersects(
        c.geometry, 
        ST_Transform(
            ST_SetSRID(ST_GeomFromText(:'polygon'), :srid),
            ST_SRID(c.geometry)
        )
    )
),
hydrotable_arrays AS (
    SELECT 
        h.catchment_id,
        h.HydroID,
        jsonb_build_object(
            'stage', array_agg(stage ORDER BY stage),
            'discharge_cms', array_agg(discharge_cms ORDER BY stage),
            'nwm_feature_id', MIN(nwm_feature_id)::integer,
            'lake_id', MIN(lake_id)::integer
        ) as hydro_data
    FROM Hydrotables h
    WHERE h.hand_version_id = :'hand_version'
    GROUP BY h.catchment_id, h.HydroID
)
SELECT jsonb_pretty(
    jsonb_build_object(
        'hand_version', :'hand_version',
        'catchments', COALESCE(jsonb_object_agg(
            c.catchment_id,
            jsonb_build_object(
                'hydrotable_entries', COALESCE((
                    SELECT jsonb_object_agg(HydroID, hydro_data)
                    FROM hydrotable_arrays h
                    WHERE h.catchment_id = c.catchment_id
                ), '{}'::jsonb),
                'raster_pair', COALESCE((
                    SELECT jsonb_build_object(
                        'rem_raster_path', r.raster_path,
                        'catchment_raster_path', cr.raster_path
                    )
                    FROM HAND_REM_Rasters r
                    LEFT JOIN HAND_Catchment_Rasters cr ON r.rem_raster_id = cr.rem_raster_id
                    WHERE r.catchment_id = c.catchment_id
                    AND r.hand_version_id = :'hand_version'
                    LIMIT 1
                ), '{}'::jsonb)
            )
        ), '{}'::jsonb)
    )
) AS result
FROM intersecting_catchments ic
JOIN Catchments c ON c.catchment_id = ic.catchment_id;
