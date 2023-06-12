SELECT 
	players.username AS "Name", 
	players.level AS "Level", 
	players.guildname AS "Guild", 
	to_char(players.maxhp, 'FM9,999,999') AS "Total HP", 
	to_char(players.totalpower, 'FM9,999,999') AS "Total CP", 
	to_char(players.attacktotalpower, 'FM9,999,999') AS "P.Atk", 
	to_char(players.defencetotalpower, 'FM9,999,999') AS "P.Def", 
	to_char(players.magicattacktotalpower, 'FM9,999,999') AS "M.Atk", 
	to_char(players.magicdefencetotalpower, 'FM9,999,999') AS "M.Def" 
FROM base_player_data players
INNER JOIN extra_player_data extra USING (userid)
WHERE 
	extra.lastaccesstime <= CURRENT_DATE - INTERVAL '3 months'
ORDER BY totalpower DESC;

SELECT 
	--base.userid,
	base.username AS "Name", 
	players.level AS "Level", 
	gld.guildname AS "Guild", 
	CASE 
		WHEN extra_cp.currenttotalpower > players.totalpower THEN extra_cp.currenttotalpower
		ELSE base.totalpower
	END highest_cp AS to_char(highest_cp, 'FM9,999,999') AS "Highest CP",
	to_char(players.maxhp, 'FM9,999,999') AS "Total HP",
	to_char(players.totalpower, 'FM9,999,999') AS "Main Set CP", 
	to_char(extra_cp.currenttotalpower, 'FM9,999,999') AS "Last Set CP", 
	to_char(extra.gvgtotalpower, 'FM9,999,999') AS "Total GvG CP", 
	to_char(players.attacktotalpower, 'FM9,999,999') AS "P.Atk", 
	to_char(players.defencetotalpower, 'FM9,999,999') AS "P.Def", 
	to_char(players.magicattacktotalpower, 'FM9,999,999') AS "M.Atk", 
	to_char(players.magicdefencetotalpower, 'FM9,999,999') AS "M.Def",
	date_trunc('second', players.updated_at) AS "Last Updated"
FROM base_player_data base
INNER JOIN players_max_cp players USING (userid)
INNER JOIN extra_players_max_cp extra_cp USING (userid)
INNER JOIN guilds gld USING (guilddataid)
INNER JOIN extra_player_data extra USING (userid)
ORDER BY highest_cp DESC;


SELECT 
	main.username AS "Name", 
	main.level AS "Level", 
	main.guildname AS "Guild", 
	to_char(highest_cp, 'FM9,999,999') AS "Highest CP", 
	main.max_hp AS "HP",
	main.atk AS "P.Atk",
	main.pdef AS "P.Def",
	main.matk AS "M.Atk",
	main.mdef AS "M.Def",
	main.main_set_cp AS "Highest Main Set CP", 
	main.last_set_cp AS "Highest Last Set CP" 
FROM (
	SELECT 
		--base.userid,
		base.username, 
		players.level, 
		gld.guildname,
		greatest(players.totalpower, extra_cp.currenttotalpower) highest_cp, 
		to_char(players.maxhp, 'FM9,999,999') AS "max_hp",
		to_char(players.totalpower, 'FM9,999,999') AS "main_set_cp", 
		to_char(extra_cp.currenttotalpower, 'FM9,999,999')AS "last_set_cp", 
		CASE 
			WHEN players.totalpower > extra_cp.currenttotalpower THEN to_char(players.attacktotalpower, 'FM9,999,999')
			ELSE NULL
		END atk,
				CASE 
			WHEN players.totalpower > extra_cp.currenttotalpower THEN to_char(players.defenceTotalPower, 'FM9,999,999')
			ELSE NULL
		END pdef,
				CASE 
			WHEN players.totalpower > extra_cp.currenttotalpower THEN to_char(players.magicAttackTotalPower, 'FM9,999,999')
			ELSE NULL
		END matk,
				CASE 
			WHEN players.totalpower > extra_cp.currenttotalpower THEN to_char(players.magicDefenceTotalPower, 'FM9,999,999')
			ELSE NULL
		END mdef,
		--to_char(extra.gvgtotalpower, 'FM9,999,999') AS "Total GvG CP", 
		--to_char(players.attacktotalpower, 'FM9,999,999') AS "P.Atk", 
		--to_char(players.defencetotalpower, 'FM9,999,999') AS "P.Def", 
		--to_char(players.magicattacktotalpower, 'FM9,999,999') AS "M.Atk", 
		--to_char(players.magicdefencetotalpower, 'FM9,999,999') AS "M.Def",
		date_trunc('second', players.updated_at) AS "Last Updated"
	FROM base_player_data base
	INNER JOIN players_max_cp players USING (userid)
	INNER JOIN extra_players_max_cp extra_cp USING (userid)
	INNER JOIN guilds gld USING (guilddataid)
	INNER JOIN extra_player_data extra USING (userid)
	--ORDER BY highest_cp DESC
) main
ORDER BY main.highest_cp DESC
LIMIT 100
