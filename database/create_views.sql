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

    
    
    SELECT c.*, ARRAY_AGG(COALESCE(dt.point, 0) ORDER BY dt.gcday ASC) FILTER (WHERE dt.gcday <= cte.last_day AND dt.gvgeventid = c.gc_num) daily_lf, daily_data.opp_lf from crosstab(
        -- crosstab to get all guild matchups each day in own column
        $$
            SELECT ARRAY[transition.gvgeventid, transition.guilddataid]::text[], transition.gvgeventid, transition.guilddataid, transition.timeslot, g.guildname, transition.points AS "total_lf", base.gcday, og.guildname
            FROM gc_predictions base
            RIGHT JOIN
            (
                -- Query to get the current highest LF (Latest LF, as LF can only go up over time, the highest LF will be the most up to date as well)
                SELECT COALESCE(gd.guilddataid, in_pred.guilddataid) AS guilddataid, COALESCE(gd.gvgeventid, in_pred.gvgeventid) AS gvgeventid, COALESCE(t.timeslot, t2.timeslot) AS timeslot, MAX(gd.point) AS points
                FROM gc_data gd
                FULL OUTER JOIN gc_predictions in_pred ON in_pred.guilddataid = gd.guilddataid AND gd.gvgeventid = in_pred.gvgeventid
                LEFT JOIN timeslots t USING (gvgtimetype)
                LEFT JOIN guilds in_gld ON in_gld.guilddataid = gd.guilddataid OR in_gld.guilddataid = in_pred.guilddataid
                LEFT JOIN timeslots t2 ON in_gld.gvgtimetype = t2.gvgtimetype
                WHERE (gd.gvgeventid IS NOT NULL OR in_pred.gvgeventid IS NOT NULL) AND (gd.guilddataid IS NOT NULL OR in_pred.guilddataid IS NOT NULL)
                GROUP BY gd.guilddataid, in_pred.guilddataid, gd.gvgeventid, in_pred.gvgeventid, t.timeslot, t2.timeslot
                ORDER BY in_pred.gvgeventid DESC
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

DROP VIEW IF EXISTS player_cp_list;
CREATE OR REPLACE VIEW player_cp_list AS
	SELECT 
		base.userid,
		base.username, 
		players.level, 
		gld.guildname,
		greatest(players.totalpower, extra_cp.currenttotalpower) highest_cp, 
		players.maxhp,
		players.totalpower AS "main_set_cp", 
		extra_cp.currenttotalpower AS "last_set_cp", 
		CASE 
			WHEN players.totalpower > extra_cp.currenttotalpower THEN players.attacktotalpower
			ELSE NULL
		END atk,
				CASE 
			WHEN players.totalpower > extra_cp.currenttotalpower THEN players.defenceTotalPower
			ELSE NULL
		END pdef,
				CASE 
			WHEN players.totalpower > extra_cp.currenttotalpower THEN players.magicAttackTotalPower
			ELSE NULL
		END matk,
				CASE 
			WHEN players.totalpower > extra_cp.currenttotalpower THEN players.magicDefenceTotalPower
			ELSE NULL
		END mdef
	FROM base_player_data base
	INNER JOIN players_max_cp players USING (userid)
	INNER JOIN extra_players_max_cp extra_cp USING (userid)
	INNER JOIN guilds gld USING (guilddataid);


DROP VIEW IF EXISTS new_human_guild_list;
CREATE OR REPLACE VIEW new_human_guild_list AS
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
    SUM(players.highest_cp) AS "Total Estimated CP",
    ROUND(AVG(players.highest_cp)) AS "Average Member CP",
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY players.highest_cp)) AS "Median Member CP",
    date_trunc('second', gld.updated_at) AS "Last Updated"
FROM player_cp_list players
INNER JOIN base_player_data base_players USING (userid)
INNER JOIN guilds gld USING (guilddataid)
INNER JOIN guild_ranks rks USING (guildrank)
INNER JOIN timeslots ts USING (gvgtimetype)
WHERE gld.ranking > 0
GROUP BY gld.guilddataid, rks.rank_letter, gld.mastername, ts.gvgtimetype
ORDER BY gld.ranking ASC;