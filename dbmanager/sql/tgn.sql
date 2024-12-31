--- Version 0.2.0
--- 2024-12-30
-- All places are stored in this table
CREATE TABLE IF NOT EXISTS places (
    place_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    original_source_id BIGINT,
    source VARCHAR(50),
    place_name VARCHAR(255) NOT NULL,
    place_type VARCHAR(255),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    parent_id BIGINT,
    alternate_names TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_source_id (original_source_id, source)
);

CREATE INDEX IF NOT EXISTS idx_place_type ON places(place_type);

CREATE INDEX IF NOT EXISTS idx_parent_id ON places(parent_id);
CREATE INDEX IF NOT EXISTS idx_coordinates ON places(latitude, longitude);