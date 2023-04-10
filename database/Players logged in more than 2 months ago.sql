SELECT 
	gld.guildname AS "Guild",
	rks.rank_letter AS "Rank",
	gld.ranking AS "Overall Rank",
	gld.gvgwin AS "Wins",
	gld.gvglose AS "Losses",
	gld.gvgdraw AS "Draws",
	COUNT(players.userid) AS "Members",
	SUM(players.totalpower) AS "Total Estimated CP",
	AVG(players.totalpower) AS "Average Member CP",
	PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY players.totalpower) AS "Median Member CP"
FROM base_player_data players
INNER JOIN guilds gld USING (guilddataid)
INNER JOIN guild_ranks rks USING (guildrank)
WHERE gld.ranking > 0
GROUP BY gld.guilddataid, rks.rank_letter
ORDER BY gld.ranking ASC;