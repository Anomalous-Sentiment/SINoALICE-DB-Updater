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
    SELECT * from crosstab(
        $$
            SELECT ARRAY[base.gvgeventid, base.guilddataid]::text[], base.gvgeventid, base.guilddataid, transition.timeslot, g.guildname, base.gcday, og.guildname
            FROM gc_predictions base
            INNER JOIN
            (
              SELECT gd.guilddataid, gd.gvgeventid, t.timeslot 
              FROM gc_data gd
              INNER JOIN timeslots t USING (gvgtimetype)
            ) transition USING (guilddataid, gvgeventid)
            LEFT JOIN guilds g ON g.guilddataid = base.guilddataid
            LEFT JOIN guilds og ON base.opponentguilddataid = og.guilddataid
            ORDER BY 1,2
        $$,
        $$
            SELECT DISTINCT gcday FROM gc_days ORDER BY 1
        $$
    ) as c(rn TEXT[], gc_num SMALLINT, guild_id INTEGER, timeslot SMALLINT, guild TEXT, day_1 TEXT, day_2 TEXT, day_3 TEXT, day_4 TEXT, day_5 TEXT, day_6 TEXT);

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
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY players.totalpower)) AS "Median Member CP"
FROM base_player_data players
INNER JOIN guilds gld USING (guilddataid)
INNER JOIN guild_ranks rks USING (guildrank)
INNER JOIN timeslots ts USING (gvgtimetype)
WHERE gld.ranking > 0
GROUP BY gld.guilddataid, rks.rank_letter, gld.mastername, ts.gvgtimetype
ORDER BY gld.ranking ASC;