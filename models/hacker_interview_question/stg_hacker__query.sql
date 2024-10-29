WITH submissions AS (

    SELECT * FROM {{ source('hacker_interview', 'submissions') }}

),

hackers AS (

    SELECT * FROM {{ source('hacker_interview', 'hackers') }}

),

stage AS (
    
    SELECT
    
        submission_date,
        submission_id,
        hacker_id,
        dense_rank() OVER (partition by hacker_id order by submission_date asc) as streak
        
    FROM submissions
),

suppressed AS (

    SELECT *  FROM stage
    WHERE streak =  (DATEDIFF(day,'2016-03-01', submission_date) + 1)
),

-- Find count of submissions by each hacker each day
sub_counts AS(

    SELECT
    submission_date,
    hacker_id,
    
    count(submission_id) AS no_submissions
    
    FROM submissions
    GROUP BY submission_date, hacker_id
),

-- rank submission counts  
rank_sub AS (

    SELECT
    
    submission_date,
    hacker_id,
    rank() over (partition by submission_date order by no_submissions desc, hacker_id asc) as rank_submissions
    
    FROM sub_counts
),

-- top poster

top_poster AS (

    SELECT
        
        rank_sub.submission_date,
        rank_sub.hacker_id,
        
        hackers.name

    FROM rank_sub
    INNER JOIN hackers ON rank_sub.hacker_id = hackers.hacker_id
    WHERE rank_sub.rank_submissions = 1
    
),

-- count the hacker_ids each day from suppressed and join to top poster
num_ids AS (
    
    SELECT

        submission_date,
    
        count(distinct hacker_id) as hacker_count

    FROM suppressed
    GROUP BY submission_date

)

SELECT 

    num_ids.submission_date,
    num_ids.hacker_count,
    
    top_poster.hacker_id,
    top_poster.name
    
FROM num_ids
INNER JOIN top_poster on num_ids.submission_date = top_poster.submission_date
ORDER BY num_ids.submission_date asc