UPDATE places
SET place_type = CASE
    WHEN place_type = 'Fuerte' THEN 'Fort'
    WHEN place_type = 'Parcialidad' THEN 'Partial Jurisdiction'
    WHEN place_type = 'Ciudad' THEN 'City'
    WHEN place_type = 'Villa' THEN 'Town'
    WHEN place_type = 'Pueblo' THEN 'Village'
    WHEN place_type = 'Poblacion' THEN 'Population Center'
    WHEN place_type = 'Localidad' THEN 'Locality'
    WHEN place_type = 'Rural' THEN 'Rural Area'
    WHEN place_type = '[-]' THEN 'Unspecified'
    ELSE place_type
END
WHERE source = 'HGIS';