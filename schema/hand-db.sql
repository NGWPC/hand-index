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
    lake_id TEXT,
    PRIMARY KEY (catchment_id, hand_version_id, HydroID, stage)
);

-- Store benchmark geometries only once
CREATE TABLE Benchmarks (
    benchmark_id UUID PRIMARY KEY,
    geometry GEOMETRY,
    source TEXT,
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT enforce_geom_type CHECK (ST_GeometryType(geometry) IN ('ST_MultiPolygon', 'ST_Polygon'))
);

-- Junction table for the many-to-many relationship
CREATE TABLE Benchmark_Catchment_Relations (
    relation_id UUID PRIMARY KEY,
    benchmark_id UUID REFERENCES Benchmarks(benchmark_id),
    catchment_id UUID REFERENCES Catchments(catchment_id),
    relation_type TEXT, -- Optional: can indicate the type of spatial relationship (contains, intersects, etc.)
    intersection_area DECIMAL -- Optional: can store the area of intersection
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
    benchmark_id UUID REFERENCES Benchmarks(benchmark_id), -- Changed to reference Benchmarks
    ver_env TEXT,
    lid TEXT,
    magnitude TEXT,
    benchmark_source TEXT,
    extent_config TEXT,
    calibrated BOOLEAN,
    false_negatives_count INTEGER,
    ACC DECIMAL,
    OTHER_METRICS TEXT
);

-- Create indexes
CREATE INDEX idx_hydrotables_nwm_feature ON Hydrotables(nwm_feature_id, nwm_version_id);
CREATE INDEX idx_benchmark_catchment_benchmark_id ON Benchmark_Catchment_Relations(benchmark_id);
CREATE INDEX idx_benchmark_catchment_catchment_id ON Benchmark_Catchment_Relations(catchment_id);

-- Create spatial indexes
CREATE INDEX idx_catchments_geometry ON Catchments USING GIST (geometry);
CREATE INDEX idx_nwm_features_geometry ON NWM_Features USING GIST (geometry);
CREATE INDEX idx_nwm_lakes_geometry ON NWM_Lakes USING GIST (geometry);
CREATE INDEX idx_levees_geometry ON Levees USING GIST (geometry);
CREATE INDEX idx_hucs_geometry ON HUCS USING GIST (geometry);
CREATE INDEX idx_benchmarks_geometry ON Benchmarks USING GIST (geometry);
