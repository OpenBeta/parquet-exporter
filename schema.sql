-- OpenBeta Climb Export Schema
-- Transforms raw GraphQL data into a flat table

SELECT
    uuid AS climb_id,
    name AS climb_name,

    -- Grades (cast required - DuckDB infers as JSON)
    CAST(grades.yds AS VARCHAR) AS grade_yds,
    CAST(grades.vscale AS VARCHAR) AS grade_vscale,
    CAST(grades.french AS VARCHAR) AS grade_french,

    -- Climbing type
    type.sport AS is_sport,
    type.trad AS is_trad,
    type.bouldering AS is_boulder,
    type.alpine AS is_alpine,
    type.tr AS is_top_rope,

    -- Location hierarchy (pathTokens: [0]=Country, [1]=State, [2]=Region, [3]=Area, [4]=Crag)
    list_element(pathTokens, 1) AS country,
    list_element(pathTokens, 2) AS state_province,
    list_element(pathTokens, 3) AS region,
    list_element(pathTokens, 4) AS area,
    list_element(pathTokens, 5) AS crag,

    -- Coordinates
    metadata.lat AS latitude,
    metadata.lng AS longitude,

    -- Route metadata
    length AS length_meters,
    boltsCount AS bolts_count,
    fa AS first_ascent,
    CAST(safety AS VARCHAR) AS safety,

    -- Description (comment out if not needed - makes file larger)
    content.description AS description

FROM climbs

-- Example filters (uncomment to use):
-- WHERE list_element(pathTokens, 1) IN ('USA', 'Canada')
-- AND type.sport = true
