-- =============================================================================
--  FINANCIAL INTELLIGENCE PLATFORM
--  Schema: BRONZE — Immutable Raw Landing Zone
--  Version 1.0 · 2026 · HU GAAP
--
--  EXECUTION ORDER: 3 of 6
--
--  WHO WRITES INTO THESE TABLES:
--    ADF ingestion pipelines — never manually.
--    The Bronze zone is append-only and immutable after landing.
--
-- =============================================================================


-- =============================================================================
--  SCHEMA DEFINITION
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS bronze;   -- immutable raw landing zone

COMMENT ON SCHEMA bronze IS 'Immutable raw snapshots exactly as received from source systems. Append-only. Never modified after landing.';


-- =============================================================================
--  6.1 INGESTION MANIFEST
--  Every source file is registered here before it is processed.
--  This provides a complete inventory of everything that has ever
--  been received, even if it was subsequently rejected.
-- =============================================================================

CREATE TABLE IF NOT EXISTS bronze.ingestion_manifest (
    manifest_id             UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id               UUID            NOT NULL REFERENCES config.ref_entity_master(entity_id),
    source_system           VARCHAR(50)     NOT NULL,
    source_file_name        VARCHAR(500)    NOT NULL,
    source_file_path        TEXT            NOT NULL,               -- full ADLS path
    source_file_size_bytes  BIGINT,
    source_file_hash        CHAR(64)        NOT NULL,               -- SHA-256; used for duplicate detection
    source_file_format      VARCHAR(20)     NOT NULL,               -- 'CSV' | 'XML' | 'XBRL' | 'OFX' | 'JSON' | 'PARQUET'
    period_id_detected      INT,                                    -- YYYYMM detected from file content or filename
    row_count_raw           INT,
    received_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    processing_status       VARCHAR(20)     NOT NULL DEFAULT 'PENDING', -- 'PENDING' | 'PROCESSING' | 'COMPLETED' | 'FAILED' | 'DUPLICATE'
    batch_id                UUID            REFERENCES audit.batch_log(batch_id),
    duplicate_of_manifest_id UUID           REFERENCES bronze.ingestion_manifest(manifest_id)
);

CREATE INDEX IF NOT EXISTS idx_manifest_entity_status  ON bronze.ingestion_manifest (entity_id, processing_status);
CREATE INDEX IF NOT EXISTS idx_manifest_hash           ON bronze.ingestion_manifest (source_file_hash);  -- fast duplicate detection

COMMENT ON TABLE  bronze.ingestion_manifest IS 'Registry of every file ever received by the platform. The SHA-256 hash enables instant duplicate file detection (DQ-008). Immutable once written. If the same file is sent twice, the second row gets processing_status=DUPLICATE and points to the original via duplicate_of_manifest_id.';


-- =============================================================================
--  END OF fip_schema_bronze.sql
--  Next file to run: fip_schema_silver.sql
-- =============================================================================
