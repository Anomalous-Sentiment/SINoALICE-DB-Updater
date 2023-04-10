SELECT 
	players.username AS "Name", 
	players.level AS "Level", 
	players.guildname AS "Guild", 
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