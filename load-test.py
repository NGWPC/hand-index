import os
import json
import uuid
import geopandas as gpd
import rasterio
import hashlib
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime

class DatabaseLoader:
    def __init__(self, db_connection_string):
        """Initialize database connection and basic settings."""
        self.engine = create_engine(db_connection_string)
        self.hand_version_id = None  # Will be set after loading HAND versions
        self.nwm_version_id = "3.0"

    def generate_deterministic_uuid(self, input_string):
        """Generate a deterministic UUID from an input string."""
        hash_object = hashlib.md5(input_string.encode())
        return str(uuid.UUID(hash_object.hexdigest()))

    def load_general_data(self, general_dir):
        """Load data from the general directory."""
        print("Loading data from general directory...")
        
        # Load HAND versions first as other functions depend on it
        self.load_hand_versions(os.path.join(general_dir, 'hand_version_info.json'))
        
        # Load other general data
        self.load_nwm_lakes(os.path.join(general_dir, 'nwm_lakes.gpkg'))
        self.load_hucs(os.path.join(general_dir, 'WBD_National.gpkg'))
        self.load_levees(os.path.join(general_dir, 'Levee_protected_areas.gpkg'))
        self.load_nwm_features(os.path.join(general_dir, 'nwm_flows.gpkg'))

    def load_catchment_data(self, catchment_dir):
        """Load data from a catchment directory and its subdirectories."""
        print(f"Loading data from catchment directory: {catchment_dir}")
        
        # First load catchments as other tables depend on catchment_ids
        catchment_ids = self.load_catchments(catchment_dir)
        
        # Then load raster-related tables
        self.load_hand_rem_rasters(catchment_dir, catchment_ids)
        self.load_hand_catchment_rasters(catchment_dir, catchment_ids)
        
        # Finally load hydrotables
        self.load_hydrotables(catchment_dir, catchment_ids)

    def load_hand_versions(self, json_path):
        """Load HAND versions from JSON file."""
        print("Loading HAND versions...")
        with open(json_path, 'r') as f:
            versions = json.load(f)
            
        with self.engine.begin() as conn:
            for version in versions:
                conn.execute(text("""
                    INSERT INTO HAND_Versions (hand_version_id, release_date, description)
                    VALUES (:id, :date, :desc)
                """), version)
        
        # Store the first hand_version_id for use in other functions
        self.hand_version_id = versions[0]['hand_version_id']

    def load_nwm_lakes(self, gpkg_path):
        """Load NWM lakes from geopackage."""
        print("Loading NWM lakes...")
        gdf = gpd.read_file(gpkg_path)
        
        # Convert to PostGIS format and insert
        with self.engine.begin() as conn:
            for _, row in gdf.iterrows():
                conn.execute(text("""
                    INSERT INTO NWM_Lakes (nwm_lake_id, geometry, shape_area)
                    VALUES (:id, ST_GeomFromText(:geom, 5070), :area)
                """), {
                    'id': row['newID'],
                    'geom': row['geometry'].wkt,
                    'area': row['Shape_Area']
                })

    def load_hucs(self, gpkg_path):
        """Load HUCs from geopackage."""
        print("Loading HUCs...")
        
        for huc_level in [2, 4, 6, 8]:
            layer_name = f'WBDHU{huc_level}'
            gdf = gpd.read_file(gpkg_path, layer=layer_name)
            
            with self.engine.begin() as conn:
                for _, row in gdf.iterrows():
                    conn.execute(text("""
                        INSERT INTO HUCS (huc_id, level, geometry, area_sq_km, states)
                        VALUES (:huc_id, :level, ST_GeomFromText(:geom, 5070), :area, :states)
                    """), {
                        'huc_id': row[f'HUC{huc_level}'],
                        'level': huc_level,
                        'geom': row['geometry'].wkt,
                        'area': row['areasqkm'],
                        'states': row['states']
                    })

    def load_levees(self, gpkg_path):
        """Load levees from geopackage."""
        print("Loading levees...")
        gdf = gpd.read_file(gpkg_path)
        
        with self.engine.begin() as conn:
            for _, row in gdf.iterrows():
                conn.execute(text("""
                    INSERT INTO Levees (levee_id, geometry, name, systemID, systemName, 
                                      areaSquareMiles, leveedAreaSource)
                    VALUES (:id, ST_GeomFromText(:geom, 5070), :name, :sys_id, :sys_name,
                            :area, :source)
                """), {
                    'id': row['id'],
                    'geom': row['geometry'].wkt,
                    'name': row['name'],
                    'sys_id': row['systemId'],
                    'sys_name': row['systemName'],
                    'area': row['areaSquareMiles'],
                    'source': row['leveedAreaSource']
                })

    def load_nwm_features(self, gpkg_path):
        """Load NWM features from geopackage."""
        print("Loading NWM features...")
        gdf = gpd.read_file(gpkg_path, layer='nwm_streams')
        
        with self.engine.begin() as conn:
            for _, row in gdf.iterrows():
                conn.execute(text("""
                    INSERT INTO NWM_Features (nwm_feature_id, nwm_version_id, geometry,
                                            to, stream_order, lake, gages, slope, mainstem)
                    VALUES (:feat_id, :ver_id, ST_GeomFromText(:geom, 5070),
                            :to, :order, :lake, :gages, :slope, :mainstem)
                """), {
                    'feat_id': row['ID'],
                    'ver_id': self.nwm_version_id,
                    'geom': row['geometry'].wkt,
                    'to': row['to'],
                    'order': row['order_'],
                    'lake': row['Lake'],
                    'gages': row['gages'],
                    'slope': row['Slope'],
                    'mainstem': row['mainstem']
                })

    def load_catchments(self, catchment_dir):
        """Load and process catchments from directory structure."""
        print("Loading catchments...")
        catchment_ids = {}
        
        for subdir in os.listdir(catchment_dir):
            subdir_path = os.path.join(catchment_dir, subdir)
            if not os.path.isdir(subdir_path):
                continue
                
            gpkg_files = [f for f in os.listdir(subdir_path) 
                         if f.startswith('gw_catchments_reaches_filtered_addedAttributes_crosswalked_')]
            
            for gpkg_file in gpkg_files:
                gdf = gpd.read_file(os.path.join(subdir_path, gpkg_file))
                
                # Merge all polygons into one
                merged_geom = gdf.unary_union
                
                # Generate deterministic UUID from merged geometry WKT
                catchment_id = self.generate_deterministic_uuid(merged_geom.wkt)
                catchment_ids[subdir] = catchment_id
                
                with self.engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO Catchments (catchment_id, geometry)
                        VALUES (:id, ST_GeomFromText(:geom, 5070))
                    """), {
                        'id': catchment_id,
                        'geom': merged_geom.wkt
                    })
        
        return catchment_ids

    def load_hand_rem_rasters(self, catchment_dir, catchment_ids):
        """Load HAND REM raster information."""
        print("Loading HAND REM rasters...")
        
        for subdir, catchment_id in catchment_ids.items():
            subdir_path = os.path.join(catchment_dir, subdir)
            rem_files = [f for f in os.listdir(subdir_path) 
                        if f.startswith('rem_zeroed_masked_')]
            
            for rem_file in rem_files:
                raster_path = os.path.join(subdir_path, rem_file)
                rem_raster_id = self.generate_deterministic_uuid(raster_path)
                
                with self.engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO HAND_REM_Rasters (rem_raster_id, catchment_id,
                                                     hand_version_id, raster_path)
                        VALUES (:rem_id, :catch_id, :hand_id, :path)
                    """), {
                        'rem_id': rem_raster_id,
                        'catch_id': catchment_id,
                        'hand_id': self.hand_version_id,
                        'path': raster_path
                    })

    def load_hand_catchment_rasters(self, catchment_dir, catchment_ids):
        """Load HAND catchment raster information."""
        print("Loading HAND catchment rasters...")
        
        for subdir, catchment_id in catchment_ids.items():
            subdir_path = os.path.join(catchment_dir, subdir)
            catchment_files = [f for f in os.listdir(subdir_path) 
                             if f.endswith('.tif') and 'gw_catchments_reaches_filtered_addedAttributes_' in f]
            
            for catchment_file in catchment_files:
                raster_path = os.path.join(subdir_path, catchment_file)
                catchment_raster_id = self.generate_deterministic_uuid(raster_path)
                
                # Get associated rem_raster_id
                rem_file = f"rem_zeroed_masked_{subdir}.tif"
                rem_path = os.path.join(subdir_path, rem_file)
                rem_raster_id = self.generate_deterministic_uuid(rem_path)
                
                with self.engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO HAND_Catchment_Rasters (catchment_raster_id,
                                                          rem_raster_id, raster_path)
                        VALUES (:catch_raster_id, :rem_id, :path)
                    """), {
                        'catch_raster_id': catchment_raster_id,
                        'rem_id': rem_raster_id,
                        'path': raster_path
                    })

    def load_hydrotables(self, catchment_dir, catchment_ids):
        """Load hydrotable information from CSV files."""
        print("Loading hydrotables...")
        
        for subdir, catchment_id in catchment_ids.items():
            subdir_path = os.path.join(catchment_dir, subdir)
            hydro_files = [f for f in os.listdir(subdir_path) 
                          if f.startswith('hydroTable_')]
            
            for hydro_file in hydro_files:
                csv_path = os.path.join(subdir_path, hydro_file)
                
                # Read CSV using pandas
                import pandas as pd
                df = pd.read_csv(csv_path)
                
                with self.engine.begin() as conn:
                    for _, row in df.iterrows():
                        # Map CSV columns to database columns
                        conn.execute(text("""
                            INSERT INTO Hydrotables (
                                catchment_id, hand_version_id, HydroID, nwm_version_id,
                                nwm_feature_id, order_id, number_of_cells, surface_area_m2,
                                bed_area_m2, top_width_m, length_km, area_sq_km,
                                wetted_perimeter_m, hydraulic_radius_m, wet_area_m2,
                                volume_m3, slope, manning_n, stage, discharge_cms,
                                default_discharge_cms, default_volume_m3, default_wet_area_m2,
                                default_hydraulic_radius_m, default_manning_n,
                                bathymetry_source, subdiv_applied, overbank_n, channel_n,
                                subdiv_discharge_cms, calb_applied, lake_id
                            )
                            VALUES (
                                :catch_id, :hand_id, :hydro_id, :nwm_ver, :feat_id,
                                :order_id, :num_cells, :surf_area, :bed_area, :top_width,
                                :length, :area, :wet_perim, :hydraulic_r, :wet_area,
                                :volume, :slope, :manning_n, :stage, :discharge,
                                :def_discharge, :def_volume, :def_wet_area,
                                :def_hydraulic_r, :def_manning_n, :bath_source,
                                :subdiv_applied, :overbank_n, :channel_n,
                                :subdiv_discharge, :calb_applied, :lake_id
                            )
                        """), {
                            'catch_id': catchment_id,
                            'hand_id': self.hand_version_id,
                            'hydro_id': row['HydroID'],
                            'nwm_ver': self.nwm_version_id,
                            'feat_id': row['feature_id'],
                            'order_id': row['order_'],
                            'num_cells': row['Number of Cells'],
                            'surf_area': row['SurfaceArea (m2)'],
                            'bed_area': row['BedArea (m2)'],
                            'top_width': row['TopWidth (m)'],
                            'length': row['LENGTHKM'],
                            'area': row['AREASQKM'],
                            'wet_perim': row['WettedPerimeter (m)'],
                            'hydraulic_r': row['HydraulicRadius (m)'],
                            'wet_area': row['WetArea (m2)'],
                            'volume': row['Volume (m3)'],
                            'slope': row['SLOPE'],
                            'manning_n': row['ManningN'],
                            'stage': row['stage'],
                            'discharge': row['discharge_cms'],
                            'def_discharge': row['default_discharge_cms'],
                            'def_volume': row['default_Volume (m3)'],
                            'def_wet_area': row['default_WetArea (m2)'],
                            'def_hydraulic_r': row['default_HydraulicRadius (m)'],
                            'def_manning_n': row['default_ManningN'],
                            'bath_source': row.get('Bathymetry_source'),
                            'subdiv_applied': row.get('subdiv_applied', False),
                            'overbank_n': row.get('overbank_n'),
                            'channel_n': row.get('channel_n'),
                            'subdiv_discharge': row.get('subdiv_discharge_cms'),
                            'calb_applied': row.get('calb_applied', False),
                            'lake_id': row.get('LakeID')
                        })

def main():
    """Main function to run the database loader."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Load data into HAND database')
    parser.add_argument('--db-connection', required=True,
                        help='Database connection string')
    parser.add_argument('--general-dir', help='Directory containing general data files')
    parser.add_argument('--catchment-dir', help='Directory containing catchment data')
    
    args = parser.parse_args()
    
    # Initialize loader
    loader = DatabaseLoader(args.db_connection)
    
    # Load general data if directory provided
    if args.general_dir:
        loader.load_general_data(args.general_dir)
    
    # Load catchment data if directory provided
    if args.catchment_dir:
        loader.load_catchment_data(args.catchment_dir)

if __name__ == '__main__':
    main()
