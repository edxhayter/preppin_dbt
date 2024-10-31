-- bring in the staged model

WITH stage AS (

    SELECT * FROM {{ ref('stg_2024wk42__input') }}

),

-- split out solo dances and joint dances, remove duplicate rows from joint dances and union back on
solos AS (

    SELECT

        year,
        series,
        week,
        stage,
        theme,
        theme_detail,
        couple,
        dance,
        music,
        result,
        overall_score,
        judges_scores

    FROM stage

    WHERE contains(dance, 'Group') = FALSE OR contains(dance, 'Marathon') = FALSE -- AND week = '12 (Semi-final)'

),

joint AS (

    SELECT

        year,
        series,
        week,
        stage,
        theme,
        theme_detail,
        'Group' AS couple,
        dance,
        music,
        result,
        NULL AS overall_score,
        judges_scores

    FROM stage
    WHERE contains(dance, 'Group') OR contains(dance, 'Marathon')
    GROUP BY ALL

),

suppressed AS (

    SELECT * FROM solos
    UNION ALL
    SELECT * FROM joint
)

-- flatten out the song column when a dance has multiple songs so that there is a row for each song

-- all songs are in speech marks in the music section, sometimes split by ',' sometimes '&' 
-- but song names and artists also use these characters so they arent a delimiter 
SELECT 

    *,
    regexp_substr_all(music, '\\"(.*?)\\".*?[, &].*?\\"(.*?)\\"', 1, 1, 'e')

FROM suppressed
WHERE regexp_count(music, '\\".*?\\",|&.*?\\".*?\\"') >= 1


