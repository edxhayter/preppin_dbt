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
),

-- flatten out the song column when a dance has multiple songs so that there is a row for each song

piped AS (
    SELECT 

        * EXCLUDE music,
        REPLACE(REPLACE(REPLACE(music, ',"', '|'), '&"', '|'), '& "', '|') AS music


    FROM suppressed

),

split AS (

    SELECT 

        * EXCLUDE music,
        split_part(music, '|', 1) AS music_1,
        split_part(music, '|', 2) AS music_2,
        split_part(music, '|', 3) AS music_3,
        split_part(music, '|', 4) AS music_4


    FROM piped

),

long_unpivot AS (

    SELECT 
        *
    FROM split
    UNPIVOT (song_artist FOR music_piece IN (music_1, music_2, music_3, music_4))
    WHERE song_artist != ''
        
),

final AS (

    SELECT

    * EXCLUDE (music_piece, song_artist),
    replace(split_part(song_artist, '—', 1), '"', '') AS song,
    split_part(song_artist, '—', 2) AS artist 

    FROM long_unpivot

)

SELECT * FROM final