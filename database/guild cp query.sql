SELECT 
	gld.guildname AS "Guild",
	gld.mastername AS "Guild Master",
	rks.rank_letter AS "Rank",
	gld.ranking AS "Overall Rank",
	gld.gvgwin AS "Wins",
	gld.gvglose AS "Losses",
	gld.gvgdraw AS "Draws",
	COUNT(players.userid) AS "Members",
	to_char(SUM(players.totalpower), 'FM99,999,999') AS "Total Estimated CP",
	to_char(AVG(players.totalpower), 'FM9,999,999') AS "Average Member CP",
	to_char(PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY players.totalpower), 'FM9,999,999') AS "Median Member CP"
FROM base_player_data players
INNER JOIN guilds gld USING (guilddataid)
INNER JOIN guild_ranks rks USING (guildrank)
WHERE gld.ranking > 0
GROUP BY gld.guilddataid, rks.rank_letter, gld.mastername
ORDER BY gld.ranking ASC;