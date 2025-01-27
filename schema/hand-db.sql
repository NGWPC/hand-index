-- Enable PostGIS extension if not already enabled
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create tables
CREATE TABLE Catchments (
    catchment_id UUID PRIMARY KEY,
    geometry GEOMETRY,
    additional_attributes JSONB,
    CONSTRAINT enforce_geom_type CHECK (ST_GeometryType(geometry) IN ('ST_MultiPolygon', 'ST_Polygon'))
);

CREATE TABLE HAND_Versions (
    hand_version_id TEXT PRIMARY KEY,
    release_date DATE,
    description TEXT
);

CREATE TABLE HAND_REM_Rasters (
    rem_raster_id UUID PRIMARY KEY,
    catchment_id UUID REFERENCES Catchments(catchment_id),
    hand_version_id TEXT REFERENCES HAND_Versions(hand_version_id),
    raster_path TEXT,
    metadata JSONB
);

CREATE TABLE HAND_Catchment_Rasters (
    catchment_raster_id UUID PRIMARY KEY,
    rem_raster_id UUID REFERENCES HAND_REM_Rasters(rem_raster_id),
    raster_path TEXT,
    metadata JSONB
);

CREATE TABLE NWM_Features (
    nwm_feature_id INTEGER,
    nwm_version_id TEXT,
    geometry GEOMETRY,
    to INTEGER,
    stream_order INTEGER,
    lake INTEGER,
    gages TEXT,
    slope REAL,
    mainstem INTEGER,
    PRIMARY KEY (nwm_feature_id, nwm_version_id),
    CONSTRAINT enforce_geom_type CHECK (ST_GeometryType(geometry) IN ('ST_LineString', 'ST_MultiLineString'))
);

CREATE TABLE NWM_Lakes (
    nwm_lake_id INTEGER PRIMARY KEY,
    geometry GEOMETRY,
    shape_area DECIMAL,
    CONSTRAINT enforce_geom_type CHECK (ST_GeometryType(geometry) IN ('ST_MultiPolygon', 'ST_Polygon'))
);

CREATE TABLE Levees (
    levee_id INTEGER PRIMARY KEY,
    geometry GEOMETRY,
    name TEXT,
    systemID INTEGER,
    systemName TEXT,
    areaSquareMiles DECIMAL,
    leveedAreaSource TEXT,
    CONSTRAINT enforce_geom_type CHECK (ST_GeometryType(geometry) IN ('ST_MultiPolygon', 'ST_Polygon'))
);

CREATE TABLE HUCS (
    huc_id TEXT PRIMARY KEY,
    level INTEGER,
    geometry GEOMETRY,
    area_sq_km DECIMAL,
    states TEXT,
    CONSTRAINT enforce_geom_type CHECK (ST_GeometryType(geometry) IN ('ST_MultiPolygon', 'ST_Polygon'))
);

CREATE TABLE Benchmark_HUC_Relations (
    relation_id UUID PRIMARY KEY,
    benchmark_geom GEOMETRY,
    huc_id TEXT REFERENCES HUCS(huc_id),
    CONSTRAINT enforce_geom_type CHECK (ST_GeometryType(benchmark_geom) IN ('ST_MultiPolygon', 'ST_Polygon'))
);

CREATE TABLE Metrics (
    metric_id UUID PRIMARY KEY,
    hand_version_id TEXT REFERENCES HAND_Versions(hand_version_id),
    relation_id UUID REFERENCES Benchmark_HUC_Relations(relation_id),
    benchmark_geom GEOMETRY,
    ver_env TEXT,
    lid TEXT,
    magnitude TEXT,
    benchmark_source TEXT,
    extent_config TEXT,
    calibrated BOOLEAN,
    false_negatives_count INTEGER,
    ACC DECIMAL,
    OTHER_METRICS TEXT,
    CONSTRAINT enforce_geom_type CHECK (ST_GeometryType(benchmark_geom) IN ('ST_MultiPolygon', 'ST_Polygon'))
);

CREATE TABLE Hydrotables (
    catchment_id UUID PRIMARY KEY REFERENCES Catchments(catchment_id),
    hand_version_id TEXT REFERENCES HAND_Versions(hand_version_id),
    HydroID TEXT,
    nwm_version_id INTEGER,
    nwm_feature_id INTEGER,
    order_id INTEGER,
    number_of_cells INTEGER,
    surface_area_m2 DECIMAL,
    bed_area_m2 DECIMAL,
    top_width_m DECIMAL,
    length_km DECIMAL,
    area_sq_km DECIMAL,
    wetted_perimeter_m DECIMAL,
    hydraulic_radius_m DECIMAL,
    wet_area_m2 DECIMAL,
    volume_m3 DECIMAL,
    slope DECIMAL,
    manning_n DECIMAL,
    stage DECIMAL,
    discharge_cms DECIMAL,
    default_discharge_cms DECIMAL,
    default_volume_m3 DECIMAL,
    default_wet_area_m2 DECIMAL,
    default_hydraulic_radius_m DECIMAL,
    default_manning_n DECIMAL,
    bathymetry_source TEXT,
    subdiv_applied BOOLEAN,
    overbank_n DECIMAL,
    channel_n DECIMAL,
    subdiv_discharge_cms DECIMAL,
    calb_applied BOOLEAN,
    last_updated TIMESTAMP,
    submitter TEXT,
    obs_source TEXT,
    precalb_discharge_cms DECIMAL,
    calb_coef_usgs DECIMAL,
    calb_coef_ras2fim DECIMAL,
    calb_coef_spatial DECIMAL,
    calb_coef_final DECIMAL,
    huc_id TEXT REFERENCES HUCS(huc_id),
    lake_id TEXT
);

-- Create indexes
CREATE INDEX idx_hydrotables_nwm_feature ON Hydrotables(nwm_feature_id, nwm_version_id);

-- Create spatial indexes
CREATE INDEX idx_catchments_geometry ON Catchments USING GIST (geometry);
CREATE INDEX idx_nwm_features_geometry ON NWM_Features USING GIST (geometry);
CREATE INDEX idx_nwm_lakes_geometry ON NWM_Lakes USING GIST (geometry);
CREATE INDEX idx_levees_geometry ON Levees USING GIST (geometry);
CREATE INDEX idx_hucs_geometry ON HUCS USING GIST (geometry);
CREATE INDEX idx_benchmark_huc_relations_geometry ON Benchmark_HUC_Relations USING GIST (benchmark_geom);
CREATE INDEX idx_metrics_geometry ON Metrics USING GIST (benchmark_geom);
