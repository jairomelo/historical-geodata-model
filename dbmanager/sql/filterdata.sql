(SELECT 
    'place_id', 'original_source_id', 'source', 'place_name', 'place_type',
    'latitude', 'longitude', 'parent_id', 'alternate_names', 'created_at', 'updated_at'
UNION ALL
SELECT *
FROM places
WHERE place_type IN (
    'inhabited place', 'lake', 'island', 'river', 'village', 'Population Center',
    'administrative division', 'islands', 'general region', 'fort', 'Rural Area',
    'locality', 'region (administrative division)', 'Town', 'Partial Jurisdiction',
    'city', 'nation', 'port', 'historical region', 'sea', 'region (geographic)', 'continent'
)) INTO OUTFILE '/var/lib/mysql-files/filtered_places.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';