-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE Catchments (
    catchment_id UUID PRIMARY KEY,
    hand_version_id TEXT REFERENCES Hand_Versions(hand_version_id),
    geometry GEOMETRY,
    additional_attributes JSONB,
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
    default_discharge_cms DECIMAL[], 
    precalib_discharge_cms DECIMAL[],
    subdiv_discharge_cms DECIMAL[],
    calb_coef_usgs DECIMAL,
    calb_coef_ras2fim DECIMAL,
    calb_coef_spatial DECIMAL,
    calb_coef_final DECIMAL,
    huc_id TEXT, 
    lake_id TEXT,
    PRIMARY KEY (catchment_id, hand_version_id, HydroID)
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

-- Create indexes
CREATE INDEX idx_hydrotables_nwm_feature ON Hydrotables(nwm_feature_id, nwm_version_id);

-- Create spatial indexes
CREATE INDEX idx_catchments_geometry ON Catchments USING GIST (geometry);
