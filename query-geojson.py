import duckdb
import os
import geopandas as gpd
from shapely.geometry import shape
from shapely.wkt import dumps  # To convert shapely geometry to WKT
import json


def get_catchment_data_for_geojson_poly(
    geojson_filepath: str, duckdb_con
) -> gpd.GeoDataFrame:
    """
    Reads a polygon from a GeoJSON file, queries DuckDB for intersecting catchments
    and their associated data, and returns the result as a GeoDataFrame.

    Args:
        geojson_filepath (str): Path to the GeoJSON file containing the query polygon.
        duckdb_con: An active DuckDB connection object.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame with the query results, CRS set to EPSG:5070.
                           Returns an empty GeoDataFrame if no results or error.
    """
    try:
        # 1. Read GeoJSON and convert the polygon to WKT
        # Using geopandas.read_file is robust for various GeoJSON structures
        gdf_query_poly_4326 = gpd.read_file(geojson_filepath)

        if gdf_query_poly_4326.empty:
            print("GeoJSON file is empty or could not be read as a GeoDataFrame.")
            return gpd.GeoDataFrame()

        # Assuming we use the first geometry from the GeoJSON
        # Ensure it's projected to EPSG:4326 if it has a different CRS
        if gdf_query_poly_4326.crs and gdf_query_poly_4326.crs.to_epsg() != 4326:
            print(
                f"Query polygon CRS is {gdf_query_poly_4326.crs}. Re-projecting to EPSG:4326."
            )
            gdf_query_poly_4326 = gdf_query_poly_4326.to_crs(epsg=4326)
        elif not gdf_query_poly_4326.crs:
            print("Query polygon has no CRS defined. Assuming EPSG:4326.")
            # If no CRS, GeoPandas might not set it. We proceed assuming it's 4326 for WKT.

        query_shapely_geom_4326 = gdf_query_poly_4326.geometry.iloc[0]
        wkt_polygon_epsg4326 = dumps(query_shapely_geom_4326)

        # 2. Construct the SQL query with a placeholder for WKT
        # We output the catchment geometry as WKB for easy conversion in GeoPandas
        sql_query = """
        LOAD spatial;
        WITH input_query_polygon_wkt AS (
            SELECT ? AS wkt_string -- Placeholder for the WKT string
        ),
        transformed_query_geometry AS (
            SELECT
                ST_Transform(
                    ST_GeomFromText(iqpw.wkt_string), -- DuckDB infers WKT, SRID will be set by ST_Transform
                    'EPSG:4326',  -- Source SRID
                    'EPSG:5070',  -- Target SRID
                    true          -- always_xy: Assume input (4326) is Lon/Lat and output (5070) is Easting/Northing
                ) AS query_geom_epsg5070
            FROM
                input_query_polygon_wkt iqpw
        ),
        filtered_catchments AS (
            SELECT DISTINCT
                c.catchment_id,
                ST_AsWKB(c.geometry) AS geometry_wkb -- Output geometry as WKB
            FROM
                catchments c, transformed_query_geometry tqg
            WHERE
                ST_Intersects(c.geometry, tqg.query_geom_epsg5070)
        )
        SELECT
            fc.catchment_id,
            fc.geometry_wkb, -- This will be our geometry column
            ht.* EXCLUDE (catchment_id),
            hrr.raster_path as rem_raster_path,
            hcr.raster_path AS catchment_raster_path
        FROM
            filtered_catchments fc
        LEFT JOIN
            hydrotables ht ON fc.catchment_id = ht.catchment_id
        LEFT JOIN
            hand_rem_rasters hrr ON fc.catchment_id = hrr.catchment_id
        LEFT JOIN
            hand_catchment_rasters hcr ON hrr.rem_raster_id = hcr.rem_raster_id
        LIMIT 1;
        """

        # 3. Execute the query
        print(
            f"Querying with WKT: {wkt_polygon_epsg4326[:100]}..."
        )  # Print start of WKT for sanity check
        result_df = duckdb_con.execute(sql_query, [wkt_polygon_epsg4326]).fetch_df()

        if result_df.empty:
            print("No catchments found intersecting the provided polygon.")
            return gpd.GeoDataFrame()

        # 4. Convert the Pandas DataFrame to a GeoPandas GeoDataFrame
        # The 'geometry_wkb' column contains WKB (Well-Known Binary)
        wkb_series = result_df["geometry_wkb"].apply(
            lambda x: bytes(x) if isinstance(x, bytearray) else x
        )
        geometries = gpd.GeoSeries.from_wkb(wkb_series)
        gdf_results = gpd.GeoDataFrame(result_df, geometry=geometries, crs="EPSG:5070")

        # Optional: drop the WKB column if you only want the geometry object column
        gdf_results = gdf_results.drop(columns=["geometry_wkb"])

        return gdf_results

    except Exception as e:
        print(f"An error occurred: {e}")
        return gpd.GeoDataFrame()  # Return an empty GeoDataFrame on error


# --- Example Usage ---
if __name__ == "__main__":
    # Create a dummy GeoJSON file for testing
    geojson_content = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-85.605165, 30.357851],  # lon_min, lat_min
                            [-80.839729, 30.357851],  # lon_max, lat_min
                            [-80.839729, 35.000659],  # lon_max, lat_max
                            [-85.605165, 35.000659],  # lon_min, lat_max
                            [-85.605165, 30.357851],  # lon_min, lat_min (closed)
                        ]
                    ],
                },
            }
        ],
    }
    dummy_geojson_path = "test_data/georgia_query_polygon.geojson"
    with open(dummy_geojson_path, "w") as f:
        json.dump(geojson_content, f)

    con = duckdb.connect(database="test_data/test-hand-index.ddb", read_only=True)

    # Run the function
    results_gdf = get_catchment_data_for_geojson_poly(dummy_geojson_path, con)

    if not results_gdf.empty:
        print("\n--- Query Results (GeoDataFrame) ---")
        print(results_gdf.head())
        print(f"\nCRS of results: {results_gdf.crs}")
    else:
        print("\nNo results returned or an error occurred.")

    # Clean up dummy file and close connection
    os.remove(dummy_geojson_path)
    con.close()
