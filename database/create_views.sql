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
    SELECT c.*, ARRAY_AGG(COALESCE(dt.point, 0) ORDER BY dt.gcday ASC) FILTER (WHERE dt.gcday <= cte.last_day AND dt.gvgeventid = c.gc_num) daily_lf, daily_data.opp_lf, opp_ids.opp_ids from crosstab(
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
    RIGHT JOIN cte ON cte.guilddataid = c.guild_id
-- Left join with GC data to get the LF of the guilds during each day of GC
LEFT JOIN gc_data dt ON cte.gvgeventid = dt.gvgeventid AND dt.guilddataid = cte.guilddataid AND dt.gcday = cte.gcday
-- Inner join to get the array of LF of the opponents each guild faced for each day of GC
LEFT JOIN (
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
LEFT JOIN (
    SELECT last_cte.gvgeventid, last_cte.guilddataid, ARRAY_AGG(COALESCE(opp_pred_id.opponentguilddataid, 0) ORDER BY opp_pred_id.gcday ASC) AS "opp_ids"
    FROM gc_predictions opp_pred_id
    RIGHT JOIN cte last_cte USING (guilddataid, gvgeventid, gcday)
    GROUP BY last_cte.guilddataid, last_cte.gvgeventid
) opp_ids ON c.gc_num = opp_ids.gvgeventid AND c.guild_id = opp_ids.guilddataid
GROUP BY c.rn, c.gc_num, c.guild_id, c.timeslot, c.guild, c.total_lf, c.day_1, c.day_2, c.day_3, c.day_4, c.day_5, c.day_6, daily_data.opp_lf, opp_ids.opp_ids
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
		base.guilddataid,
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
	LEFT JOIN extra_players_max_cp extra_cp USING (userid);

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
INNER JOIN guilds gld USING (guilddataid)
INNER JOIN base_player_data base_players USING (userid)
INNER JOIN guild_ranks rks USING (guildrank)
INNER JOIN timeslots ts USING (gvgtimetype)
WHERE gld.ranking > 0
GROUP BY gld.guilddataid, rks.rank_letter, gld.mastername, ts.gvgtimetype
ORDER BY gld.ranking ASC;

DROP VIEW IF EXISTS gc_stats;
CREATE OR REPLACE VIEW gc_stats AS
SELECT gc_data.gvgeventid, gc_data.ranking, gc_data.point
FROM gc_data gc_data
INNER JOIN gc_events events USING (gvgeventid)
WHERE gc_data.updated_at > events.prelim_end AND gc_data.gcday = 6;

DROP VIEW IF EXISTS guild_summary;
CREATE OR REPLACE VIEW guild_summary AS
SELECT gld.guilddataid, gld.guildname AS "Guild Name", gld.mastername AS "Guild Master", gld.siegehp + gld.siegehpbonus AS "Ship HP", gld.ranking AS "Ranking", ranks.rank_letter AS "Rank", ts.timeslot AS "Timeslot", gld.guilddescription AS "Description", gld.subscriptioncomment AS "Recruitment Msg", SUM(player_cp.highest_cp) AS "Total Estimated CP"
FROM guilds gld
INNER JOIN timeslots ts USING (gvgtimetype)
INNER JOIN base_player_data players USING (guilddataid)
INNER JOIN player_cp_list player_cp USING (userid)
INNER JOIN guild_ranks ranks USING (guildrank)
GROUP BY gld.guilddataid, gld.guildname, gld.mastername, gld.siegehp + gld.siegehpbonus, gld.ranking, ranks.rank_letter, ts.timeslot, gld.guilddescription, gld.subscriptioncomment;

DROP VIEW IF EXISTS guild_members;
CREATE OR REPLACE VIEW guild_members AS
SELECT players.playerid, players.guilddataid AS guild_id, extra.gvgcharactermstid AS class_id, players.username AS member, players.level, player_cp.highest_cp AS estimated_cp, players.totalpower AS current_cp
FROM base_player_data players
INNER JOIN extra_player_data extra USING (userid)
INNER JOIN player_cp_list player_cp USING (userid);

DROP VIEW IF EXISTS guild_names_history;
CREATE OR REPLACE VIEW guild_names_history AS
SELECT gdata.guilddataid, gdata.gvgeventid, ARRAY_AGG(DISTINCT(gdata.guildname)) AS guild_names
FROM gc_data gdata
GROUP BY gdata.guilddataid, gdata.gvgeventid;

DROP VIEW IF EXISTS guild_gc_history;
CREATE OR REPLACE VIEW guild_gc_history AS
SELECT gdata.guilddataid, gdata.gvgeventid AS "gc_num", names.guild_names, gdata.membernum AS "member_num", ts.timeslot, gdata.point AS "lifeforce", gdata.ranking AS "ranking", gdata.rankinginbattleterm AS "ts_ranking", gdata.winpoint AS "wins"
FROM gc_data gdata
INNER JOIN gc_events events USING (gvgeventid)
INNER JOIN timeslots ts USING (gvgtimetype)
INNER JOIN guild_names_history names USING (guilddataid, gvgeventid)
WHERE gcday = 6 AND gdata.updated_at > events.prelim_end;

DROP VIEW IF EXISTS new_guild_names_history;
CREATE OR REPLACE VIEW new_guild_names_history AS
SELECT DISTINCT ON (gdata.guildname) gdata.guilddataid, gdata.gvgeventid, gdata.guildname
FROM gc_data gdata;

DROP VIEW IF EXISTS new_guild_gc_history;
CREATE OR REPLACE VIEW new_guild_gc_history AS
SELECT gdata.guilddataid, gdata.gvgeventid AS "gc_num", gdata.guildname, ARRAY_AGG(names.guildname) FILTER (WHERE names.guildname != gdata.guildname), gdata.membernum AS "member_num", ts.timeslot, gdata.point AS "lifeforce", gdata.ranking AS "ranking", gdata.rankinginbattleterm AS "ts_ranking", gdata.winpoint AS "wins"
FROM gc_data gdata
INNER JOIN gc_events events USING (gvgeventid)
INNER JOIN timeslots ts USING (gvgtimetype)
LEFT JOIN new_guild_names_history names USING (guilddataid, gvgeventid)
WHERE gcday = 6 AND gdata.updated_at > events.prelim_end
GROUP BY gdata.guilddataid, gdata.gvgeventid, gdata.guildname, gdata.membernum, ts.timeslot, gdata.point, gdata.ranking, gdata.rankinginbattleterm, gdata.winpoint;

DROP VIEW IF EXISTS gc_event_data;
CREATE OR REPLACE VIEW gc_event_data AS
SELECT events.gvgeventid, events.entry_start, events.entry_end, events.prelim_start, events.prelim_end, MAX(gdata.updated_at) AS "last_updated"
FROM gc_events events
LEFT JOIN gc_data gdata USING (gvgeventid)
GROUP BY events.gvgeventid, events.entry_start, events.entry_end, events.prelim_start, events.prelim_end;