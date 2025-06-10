-- Enable DuckDB spatial extension
INSTALL spatial;
LOAD spatial;

CREATE TABLE Catchments (
    catchment_id UUID PRIMARY KEY,
    hand_version_id TEXT, 
    geometry GEOMETRY,
    additional_attributes JSON
);

CREATE TABLE Hydrotables (
    catchment_id UUID REFERENCES Catchments(catchment_id),
    hand_version_id TEXT,
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
    hand_version_id TEXT,
    raster_path TEXT,
    metadata JSON
);

CREATE TABLE HAND_Catchment_Rasters (
    catchment_raster_id UUID PRIMARY KEY,
    rem_raster_id UUID REFERENCES HAND_REM_Rasters(rem_raster_id),
    raster_path TEXT,
    metadata JSON
);

-- Create indexes
CREATE INDEX idx_hydrotables_nwm_feature ON Hydrotables(nwm_feature_id, nwm_version_id);
CREATE INDEX idx_hydrotables_catchment ON Hydrotables(catchment_id);
CREATE INDEX idx_rem_rasters_catchment ON HAND_REM_Rasters(catchment_id);
CREATE INDEX idx_catchment_rasters_rem ON HAND_Catchment_Rasters(rem_raster_id);

-- Create spatial index for geometry column
CREATE INDEX idx_catchments_geometry ON Catchments USING RTREE (geometry);
