-- Enable DuckDB spatial extension
INSTALL spatial;
LOAD spatial;

CREATE TABLE Catchments (
    catchment_id UUID PRIMARY KEY,
    hand_version_id TEXT, 
    geometry VARCHAR, -- storing geometry as WKT
    h3_index BIGINT,
    branch_path TEXT
);

CREATE TABLE Hydrotables (
    catchment_id UUID REFERENCES Catchments(catchment_id),
    hand_version_id TEXT,
    HydroID TEXT,
    nwm_version_id DECIMAL,
    h3_index BIGINT,
    feature_id BIGINT,
    NextDownID BIGINT,
    order_ INTEGER,
    "Number of Cells" INTEGER[],
    "SurfaceArea (m2)" DOUBLE[],
    "BedArea (m2)" DOUBLE[],
    "TopWidth (m)" DOUBLE[],
    LENGTHKM DOUBLE,
    AREASQKM DOUBLE,
    "WettedPerimeter (m)" DOUBLE[],
    "HydraulicRadius (m)" DOUBLE[],
    "WetArea (m2)" DOUBLE[],
    "Volume (m3)" DOUBLE[],
    SLOPE DOUBLE,
    ManningN DOUBLE,
    stage DECIMAL[], 
    discharge_cms DECIMAL[], 
    default_discharge_cms DECIMAL[], 
    "default_Volume (m3)" DOUBLE[],
    "default_WetArea (m2)" DOUBLE[],
    "default_HydraulicRadius (m)" DOUBLE[],
    default_ManningN DOUBLE,
    Bathymetry_source TEXT,
    subdiv_applied BOOLEAN,
    overbank_n DOUBLE,
    channel_n DOUBLE,
    subdiv_discharge_cms DECIMAL[],
    calb_applied BOOLEAN,
    last_updated TEXT,
    submitter TEXT,
    obs_source TEXT,
    precalb_discharge_cms DECIMAL[],
    calb_coef_usgs DOUBLE,
    calb_coef_ras2fim DOUBLE,
    calb_coef_spatial DOUBLE,
    calb_coef_final DOUBLE,
    HUC TEXT, 
    LakeID TEXT,
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
CREATE INDEX idx_hydrotables_nwm_feature ON Hydrotables(feature_id, nwm_version_id);
CREATE INDEX idx_hydrotables_catchment ON Hydrotables(catchment_id);
CREATE INDEX idx_rem_rasters_catchment ON HAND_REM_Rasters(catchment_id);
CREATE INDEX idx_catchment_rasters_rem ON HAND_Catchment_Rasters(rem_raster_id);
CREATE INDEX idx_catchments_branch_path ON Catchments(branch_path);
