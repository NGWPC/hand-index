-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE Hand_Versions (
    hand_version_id TEXT PRIMARY KEY
);

CREATE TABLE Catchments (
    catchment_id UUID PRIMARY KEY,
    hand_version_id TEXT REFERENCES Hand_Versions(hand_version_id),
    geometry GEOMETRY,
    additional_attributes JSONB,
    CONSTRAINT enforce_geom_type CHECK (ST_GeometryType(geometry) IN ('ST_MultiPolygon', 'ST_Polygon'))
);

CREATE TABLE NWM_Features (
    nwm_feature_id BIGINT,
    nwm_version_id DECIMAL,
    geometry GEOMETRY,
    to_feature INTEGER,
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
    levee_id BIGINT PRIMARY KEY,
    geometry GEOMETRY,
    name TEXT,
    systemID BIGINT,
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

CREATE TABLE Hydrotables (
    catchment_id UUID REFERENCES Catchments(catchment_id),
    hand_version_id TEXT REFERENCES Hand_Versions(hand_version_id),
    HydroID TEXT,
    nwm_version_id DECIMAL,
    nwm_feature_id BIGINT,
    stage DECIMAL[], 
    discharge_cms DECIMAL[], 
    huc_id TEXT, 
    lake_id TEXT,
    PRIMARY KEY (catchment_id, hand_version_id, HydroID)
);

CREATE TABLE Benchmark_HUC_Relations (
    relation_id UUID PRIMARY KEY,
    benchmark_geom GEOMETRY,
    huc_id TEXT REFERENCES HUCS(huc_id),
    CONSTRAINT enforce_geom_type CHECK (ST_GeometryType(benchmark_geom) IN ('ST_MultiPolygon', 'ST_Polygon'))
);

CREATE TABLE HAND_REM_Rasters (
    rem_raster_id UUID PRIMARY KEY,
    catchment_id UUID REFERENCES Catchments(catchment_id),
    hand_version_id TEXT REFERENCES Hand_Versions(hand_version_id),
    raster_path TEXT,
    metadata JSONB
);

CREATE TABLE HAND_Catchment_Rasters (
    catchment_raster_id UUID PRIMARY KEY,
    rem_raster_id UUID REFERENCES HAND_REM_Rasters(rem_raster_id),
    raster_path TEXT,
    metadata JSONB
);

CREATE TABLE Metrics (
    metric_id UUID PRIMARY KEY,
    hand_version_id TEXT REFERENCES Hand_Versions(hand_version_id),
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
