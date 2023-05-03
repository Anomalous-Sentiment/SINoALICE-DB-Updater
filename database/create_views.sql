CREATE EXTENSION IF NOT EXISTS tablefunc;

DROP VIEW IF EXISTS gc_matchups_id;
-- View to get table of guild matchups
CREATE OR REPLACE VIEW gc_matchups_id AS
    SELECT * from crosstab(
        $$
            SELECT ARRAY[base.gvgeventid, base.guilddataid]::text[], base.gvgeventid, transition.gvgtimetype, base.guilddataid, base.gcday, base.opponentguilddataid
            FROM gc_predictions base
            INNER JOIN
            (
              SELECT gd.guilddataid, gd.gvgeventid, gd.gcday, t.gvgtimetype
              FROM gc_data gd
              INNER JOIN timeslots t USING (gvgtimetype)
            ) transition USING (guilddataid, gvgeventid)
            ORDER BY 1,2
        $$,
        $$
            SELECT DISTINCT gcday FROM gc_days ORDER BY 1
        $$
    ) as c(rn TEXT[], gvgeventid SMALLINT, gvgtimetype SMALLINT, guilddataid INTEGER, day_1 INTEGER, day_2 INTEGER, day_3 INTEGER, day_4 INTEGER, day_5 INTEGER, day_6 INTEGER);

    
    
DROP VIEW IF EXISTS gc_matchups;
-- View to get table of guild matchups
CREATE OR REPLACE VIEW gc_matchups AS 
-- CTE to fill in missing GC days
 WITH cte AS (
      SELECT DISTINCT ON(gvgeventid, in_gld.guilddataid, gdays.gcday) in_gld.guilddataid, gdays.gcday, FIRST_VALUE(gt.gvgeventid) OVER (PARTITION BY gt.gvgeventid, gt.guilddataid ORDER BY gt.gvgeventid IS NULL RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) gvgeventid, LAST_VALUE(gt.gcday) OVER (PARTITION BY gt.gvgeventid, gt.gvgtimetype ORDER BY gt.gcday RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_day
      FROM gc_days gdays
      CROSS JOIN guilds in_gld
      LEFT JOIN gc_data gt USING (guilddataid)
      ORDER BY gvgeventid, in_gld.guilddataid, gdays.gcday
    )
    SELECT c.*, ARRAY_AGG(COALESCE(dt.point, 0) ORDER BY dt.updated_at) FILTER (WHERE cte.gcday <= cte.last_day AND cte.gvgeventid = c.gc_num) daily_lf, daily_data.opp_lf from crosstab(
        -- crosstab to get all guild matchups each day in own column
        $$
            SELECT ARRAY[transition.gvgeventid, transition.guilddataid]::text[], transition.gvgeventid, transition.guilddataid, transition.timeslot, g.guildname, transition.points AS "total_lf", base.gcday, og.guildname
            FROM gc_predictions base
            RIGHT JOIN
            (
                -- Query to get the current highest LF (Latest LF, as LF can only go up over time, the highest LF will be the most up to date as well)
                SELECT gd.guilddataid, gd.gvgeventid, t.timeslot, MAX(point) AS points
                FROM gc_data gd
                INNER JOIN timeslots t USING (gvgtimetype)
                GROUP BY gd.guilddataid, gd.gvgeventid, t.timeslot
            ) transition USING (guilddataid, gvgeventid)
            -- Left join with guilds table to get current guild names
            LEFT JOIN guilds g ON g.guilddataid = base.guilddataid
            LEFT JOIN guilds og ON base.opponentguilddataid = og.guilddataid
            ORDER BY 1,2
        $$,
        $$
            -- Query to get the days of GC (Acts as the column headers of the crosstab table)
            SELECT DISTINCT gcday FROM gc_days ORDER BY 1
        $$
    ) as c(rn TEXT[], gc_num SMALLINT, guild_id INTEGER, timeslot SMALLINT, guild TEXT, total_lf BIGINT, day_1 TEXT, day_2 TEXT, day_3 TEXT, day_4 TEXT, day_5 TEXT, day_6 TEXT)
    RIGHT JOIN cte ON cte.guilddataid = c.guild_id
-- Left join with GC data to get the LF of the guilds during each day of GC
LEFT JOIN gc_data dt ON cte.gvgeventid = dt.gvgeventid AND dt.guilddataid = cte.guilddataid AND dt.gcday = cte.gcday

-- Inner join to get the array of LF of the opponents each guild faced for each day of GC
INNER JOIN (
    -- Table aggregating the opponent LF into a sub array
    SELECT cte.gvgeventid, cte.guilddataid, array_agg(COALESCE(lf_data.lf_gain, 0) ORDER BY cte.gcday ASC) AS opp_lf 
    FROM cte
    LEFT JOIN gc_predictions pred USING (gvgeventid, guilddataid, gcday) 
    -- Join to get the LF gain of the opponent guild for the day (Inclusive of Win/loss bonus)
    LEFT JOIN (
        -- Query to get the LF gain of each guild
        SELECT cte.guilddataid, cte.gcday, FIRST_VALUE(sub_gc.gvgeventid) OVER (PARTITION BY sub_gc.gvgeventid, cte.guilddataid ORDER BY sub_gc.gvgeventid IS NULL) AS gvgeventid, COALESCE(sub_gc.point, 0) AS lf, COALESCE(MAX(sub_gc.point) OVER (PARTITION BY (sub_gc.gvgeventid, cte.guilddataid) ORDER BY cte.guilddataid, cte.gcday) - MAX(sub_gc.point) OVER (PARTITION BY (sub_gc.gvgeventid, cte.guilddataid) ORDER BY cte.guilddataid, cte.gcday ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), sub_gc.point) AS "lf_gain" 
        FROM cte
        LEFT JOIN gc_data sub_gc USING (gvgeventid, guilddataid, gcday)
    ) lf_data ON pred.opponentguilddataid = lf_data.guilddataid AND pred.gvgeventid = lf_data.gvgeventid AND lf_data.gcday = pred.gcday
    -- Group by for aggregating opponent LF into array
    GROUP BY cte.gvgeventid, cte.guilddataid
) daily_data ON c.gc_num = daily_data.gvgeventid AND c.guild_id = daily_data.guilddataid
GROUP BY c.rn, c.gc_num, c.guild_id, c.timeslot, c.guild, c.total_lf, c.day_1, c.day_2, c.day_3, c.day_4, c.day_5, c.day_6, daily_data.opp_lf
ORDER BY c.total_lf DESC;

-- View to display players logged in since a specified date
CREATE OR REPLACE VIEW login_activity AS
WITH day_intervals AS (
    SELECT
        (SELECT MIN(lastAccessTime)::DATE FROM extra_player_data) + ( n    || ' days')::interval start_time,
        (SELECT MIN(lastAccessTime)::DATE FROM extra_player_data) + ((n+1) || ' days')::interval end_time
    from generate_series(0, ((select NOW()::date - min(lastAccessTime)::date from extra_player_data)), 1) n
)
SELECT d.start_time::DATE, COUNT(ex.userId)
FROM extra_player_data ex
RIGHT JOIN day_intervals d
    ON ex.lastAccessTime::DATE >= d.start_time
GROUP BY d.start_time
ORDER BY d.start_time ASC;


CREATE OR REPLACE FUNCTION update_player_activity() RETURNS TRIGGER AS $activity_update$
    BEGIN
        --
        -- Create rows in activity_update to reflect the operations performed on emp,
        -- making use of the special variable TG_OP to work out the operation.
        --
        INSERT INTO player_activity
            SELECT *
            FROM crosstab(
                $$
                    WITH day_intervals AS (
                        SELECT
                            (SELECT MIN(lastAccessTime)::DATE FROM extra_player_data) + ( n    || ' days')::interval start_time,
                            (SELECT MIN(lastAccessTime)::DATE FROM extra_player_data) + ((n+1) || ' days')::interval end_time
                        from generate_series(0, ((select NOW()::date - min(lastAccessTime)::date from extra_player_data)), 1) n
                    )
                    SELECT CURRENT_DATE, d.start_time ::TEXT, COUNT(ex.userId)
                    FROM extra_player_data ex
                    RIGHT JOIN day_intervals d
                        ON ex.lastAccessTime::DATE >= d.start_time
                    WHERE 
                        d.start_time = CURRENT_DATE - INTERVAL '1 day' OR
                        d.start_time = CURRENT_DATE - INTERVAL '3 day' OR
                        d.start_time = CURRENT_DATE - INTERVAL '5 day' OR
                        d.start_time = CURRENT_DATE - INTERVAL '7 day' OR
                        d.start_time = CURRENT_DATE - INTERVAL '14 day'
                    GROUP BY d.start_time
                    ORDER BY 1,2
                $$,
                $$
                    WITH day_intervals AS (
                        SELECT
                            (SELECT MIN(lastAccessTime)::DATE FROM extra_player_data) + ( n    || ' days')::interval start_time,
                            (SELECT MIN(lastAccessTime)::DATE FROM extra_player_data) + ((n+1) || ' days')::interval end_time
                        from generate_series(0, ((select NOW()::date - min(lastAccessTime)::date from extra_player_data)), 1) n
                    )
                    SELECT d.start_time ::TEXT
                    FROM extra_player_data ex
                    RIGHT JOIN day_intervals d
                        ON ex.lastAccessTime::DATE >= d.start_time
                    WHERE 
                        d.start_time = CURRENT_DATE - INTERVAL '1 day' OR
                        d.start_time = CURRENT_DATE - INTERVAL '3 day' OR
                        d.start_time = CURRENT_DATE - INTERVAL '5 day' OR
                        d.start_time = CURRENT_DATE - INTERVAL '7 day' OR
                        d.start_time = CURRENT_DATE - INTERVAL '14 day'
                    GROUP BY d.start_time
                    ORDER BY 1 DESC
                $$
            ) AS ct("curr_date" DATE, "since_1_day" INTEGER, "since_3_days" INTEGER, "since_5_days" INTEGER, "since_7_days" INTEGER, "since_14_days" INT)
            ON CONFLICT (snapshot_date) DO UPDATE SET 
            logged_within_1_day = EXCLUDED.logged_within_1_day,
            logged_within_3_days = EXCLUDED.logged_within_3_days,
            logged_within_5_days = EXCLUDED.logged_within_5_days,
            logged_within_7_days = EXCLUDED.logged_within_7_days,
            logged_within_14_days = EXCLUDED.logged_within_14_days;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$activity_update$ LANGUAGE plpgsql;

-- It seems that for upserts, both triggers insert and update triggers end up firing, so the function gets called twice?

DROP TRIGGER IF EXISTS player_data_ins ON extra_player_data;
CREATE TRIGGER player_data_ins
    AFTER INSERT ON extra_player_data
    REFERENCING NEW TABLE AS new_table
    FOR EACH STATEMENT EXECUTE FUNCTION update_player_activity();

DROP TRIGGER IF EXISTS player_data_upd ON extra_player_data;
CREATE TRIGGER player_data_upd
    AFTER UPDATE ON extra_player_data
    REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
    FOR EACH STATEMENT EXECUTE FUNCTION update_player_activity();

DROP VIEW IF EXISTS human_guild_list;
CREATE OR REPLACE VIEW human_guild_list AS
SELECT 
    gld.guilddataid AS "Guild ID",
    gld.guildname AS "Guild",
    gld.mastername AS "Guild Master",
    ts.timeslot AS "Timeslot",
    rks.rank_letter AS "Rank",
    gld.ranking AS "Overall Rank",
    gld.gvgwin AS "Wins",
    gld.gvglose AS "Losses",
    gld.gvgdraw AS "Draws",
    COUNT(players.userid) AS "Members",
    SUM(players.maxhp) AS "Total HP",
    ROUND(AVG(players.maxhp)) AS "Average Member HP",
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY players.maxhp)) AS "Median Member HP",
    SUM(players.totalpower) AS "Total Estimated CP",
    ROUND(AVG(players.totalpower)) AS "Average Member CP",
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY players.totalpower)) AS "Median Member CP",
    date_trunc('second', gld.updated_at) AS "Last Updated"

FROM base_player_data players
INNER JOIN guilds gld USING (guilddataid)
INNER JOIN guild_ranks rks USING (guildrank)
INNER JOIN timeslots ts USING (gvgtimetype)
WHERE gld.ranking > 0
GROUP BY gld.guilddataid, rks.rank_letter, gld.mastername, ts.gvgtimetype
ORDER BY gld.ranking ASC;