
DROP TABLE IF EXISTS gc_days;
CREATE TABLE gc_days
(
    gcDay SMALLINT,
    PRIMARY KEY (gcDay)
);

DROP TABLE IF EXISTS guild_ranks;
CREATE TABLE guild_ranks
(
    guildRank SMALLINT,
    rank_letter VARCHAR(5),
    PRIMARY KEY (guildrank)
);

DROP TABLE IF EXISTS gc_events;
CREATE TABLE gc_events
(
    gvgEventId SMALLINT,
    entry_start TIMESTAMPTZ,
    entry_end TIMESTAMPTZ,
    prelim_start TIMESTAMPTZ,
    prelim_end TIMESTAMPTZ,
    PRIMARY KEY (gvgEventId)
);

DROP TABLE IF EXISTS gc_groups;
CREATE TABLE gc_groups
(
    finals_group CHAR(1),
    PRIMARY KEY (finals_group)
);

DROP TABLE IF EXISTS gc_finals;
CREATE TABLE gc_finals
(
    gvgEventId SMALLINT,
    finals_group CHAR(1),
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    CONSTRAINT fk_group
        FOREIGN KEY (finals_group) REFERENCES gc_groups (finals_group),
    CONSTRAINT fk_gc
        FOREIGN KEY (gvgEventId) REFERENCES gc_events (gvgEventId),
    PRIMARY KEY (gvgEventId, finals_group)
);

DROP TABLE IF EXISTS timeslots;
CREATE TABLE timeslots
(
    gvgtimetype SMALLINT,
    timeslot SMALLINT,
    time_in_utc TIME WITH TIME ZONE,
    gc_available BOOLEAN,
    PRIMARY KEY (gvgtimetype)
);

DROP TABLE IF EXISTS base_player_data;
CREATE TABLE base_player_data
(
    userId BIGINT,
    userName VARCHAR(16),
    level SMALLINT,
    currentUserTitleMstId INTEGER,
    currentJobMstId INTEGER,
    currentCharacterMstId INTEGER,
    playerId INTEGER,
    comment VARCHAR(150),
    relationship VARCHAR(10),
    targetRelationship VARCHAR(10),
    isGuildAvailable BOOLEAN,
    isJoinGuild BOOLEAN,
    isGuildMaster BOOLEAN,
    isGuildSubMaster BOOLEAN,
    guildDataId INTEGER,
    guildName VARCHAR(16),
    gvgWin INTEGER,
    gvgLose INTEGER,
    totalPower INTEGER,
    attackTotalPower INTEGER,
    defenceTotalPower INTEGER,
    magicAttackTotalPower INTEGER,
    magicDefenceTotalPower INTEGER,
    questSpReductionLevel SMALLINT,
    gvgSpReductionLevel SMALLINT,
    appealWeaponCardMstId INTEGER,
    appealNightmareCardMstId INTEGER,
    numericCountryCode INTEGER,
    maxHp INTEGER,
    baseCharacterMstId INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    PRIMARY KEY(userId)
);

DROP TABLE IF EXISTS extra_player_data;
CREATE TABLE extra_player_data
(
    userId BIGINT,
    currentJobRoleType SMALLINT,
    currentJobRolePosition SMALLINT,
    currentCostumeMstId INTEGER,
    currentTotalPower INTEGER,
    gvgCharacterMstId INTEGER,
    gvgJobMstId INTEGER,
    gvgJobRoleType SMALLINT,
    gvgJobRolePosition SMALLINT,
    gvgTotalPower INTEGER,
    gameStatus SMALLINT,
    leaderCardMstId INTEGER,
    deckCost INTEGER,
    maxCard INTEGER,
    maxProtector INTEGER,
    maxNightMare INTEGER,
    maxOtherCard INTEGER,
    maxStorageCard INTEGER,
    maxStorageProtector INTEGER,
    maxStorageNightMare INTEGER,
    maxStorageOtherCard INTEGER,
    maxItem INTEGER,
    maxFriend SMALLINT,
    favoriteAkbMember1 INTEGER,
    favoriteAkbMember2 INTEGER,
    favoriteAkbMember3 INTEGER,
    isGameMaster INTEGER,
    exp BIGINT,
    stamina INTEGER,
    staminaMax INTEGER,
    staminaUpdatedTime TIMESTAMPTZ,
    cleaningUpdatedTime TIMESTAMPTZ,
    cleaningStatus SMALLINT,
    gvgDraw INTEGER,
    gvgWinning INTEGER,
    gvgMaxWinning INTEGER,
    money INTEGER,
    characterPoint INTEGER,
    lastAccessTime TIMESTAMPTZ,
    tutorialFinishTime TIMESTAMPTZ,
    recentLoginTime VARCHAR(7),
    maxMainLimitBreakSkill SMALLINT,
    maxSubLimitBreakSkill SMALLINT,
    maxSupportJob SMALLINT,
    isFrontRowChange BOOLEAN,
    createdTime TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    CONSTRAINT fk_user_id
        FOREIGN KEY (userId) REFERENCES base_player_data (userId),
    PRIMARY KEY(userId)
);

DROP TABLE IF EXISTS guilds;
CREATE TABLE guilds
(
    autoExpulsionType SMALLINT,
    beforeGvgTimeType SMALLINT,
    createdTime TIMESTAMP,
    currentGuildTitleMstId INTEGER,
    favoriteAkbMember INTEGER,
    guildCountryCode SMALLINT,
    guildDataId INTEGER NOT NULL,
    guildDescription VARCHAR(50),
    guildIdentifierId VARCHAR(10),
    guildLanguageUserCode SMALLINT,
    guildLevel SMALLINT,
    guildMasterUserId BIGINT,
    guildName VARCHAR(16),
    guildRank SMALLINT,
    gvgDraw INTEGER,
    gvgLose INTEGER,
    gvgPushCallComment VARCHAR(50),
    gvgTimeType SMALLINT,
    gvgWin INTEGER,
    isAutoAccept BOOLEAN,
    isGvgPushCall BOOLEAN,
    isRecording BOOLEAN,
    isRecruit BOOLEAN,
    joinMember SMALLINT,
    lastRank SMALLINT,
    masterCharacterMstId INTEGER,
    masterLeaderCardMstId INTEGER,
    masterName VARCHAR(10),
    maxMember SMALLINT,
    ranking INTEGER,
    siegeHp INTEGER,
    siegeHpBonus INTEGER,
    subscriptionActionType SMALLINT,
    subscriptionComment VARCHAR(50),
    subscriptionGvgJoinType SMALLINT,
    subscriptionPowerType SMALLINT,
    subscriptionStyleType SMALLINT,
    tacticsDescription VARCHAR(2000),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    CONSTRAINT fk_gm_id
        FOREIGN KEY (guildMasterUserId) REFERENCES base_player_data (userId),
    CONSTRAINT fk_guild_rank
        FOREIGN KEY (guildRank) REFERENCES guild_ranks (guildRank),
    CONSTRAINT fk_timeslot
        FOREIGN KEY (gvgTimeType) REFERENCES timeslots (gvgTimeType),
    PRIMARY KEY (guildDataId)
);


DROP TABLE IF EXISTS gc_data;
CREATE TABLE gc_data
(
    gcDay SMALLINT NOT NULL,
    achievementCount SMALLINT,
    guildCountryCode SMALLINT,
    guildDataId INTEGER NOT NULL,
    guildLevel SMALLINT,
    guildMasterGvgCharacterMstId INTEGER,
    guildMasterName VARCHAR(10),
    guildMasterUserId BIGINT,
    guildName VARCHAR(16),
    gvgEventId SMALLINT NOT NULL,
    gvgEventRankingDataId INTEGER,
    gvgTimeType SMALLINT,
    isDeleted BOOLEAN,
    isEntryUltimateBattle SMALLINT,
    memberNum SMALLINT,
    point BIGINT,
    rank SMALLINT,
    ranking SMALLINT,
    rankingInBattleTerm SMALLINT,
    sourceCount SMALLINT,
    winPoint SMALLINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    CONSTRAINT fk_gc_day
        FOREIGN KEY (gcDay) REFERENCES gc_days (gcDay),
    CONSTRAINT fk_gvgeventid
        FOREIGN KEY (gvgEventId) REFERENCES gc_events (gvgEventId),
    CONSTRAINT fk_gvgtimetype
        FOREIGN KEY (gvgTimeType) REFERENCES timeslots (gvgTimeType),
    CONSTRAINT fk_gm_id
        FOREIGN KEY (guildMasterUserId) REFERENCES base_player_data (userId),
    PRIMARY KEY (gvgEventId, gcDay, guildDataId)
);


DROP TABLE IF EXISTS temp;
CREATE TABLE temp
(
    gvgEventId SMALLINT,
    guildDataId INTEGER NOT NULL,
    ranking INTEGER,
    gvgtimetype SMALLINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    CONSTRAINT fk_guild
        FOREIGN KEY (guildDataId) REFERENCES guilds (guildDataId),
    PRIMARY KEY (guildDataId)
);

DROP TABLE IF EXISTS gc_predictions;
CREATE TABLE gc_predictions
(
    gcDay SMALLINT,
    gvgEventId SMALLINT,
    guildDataId INTEGER NOT NULL,
    opponentGuildDataId INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    CONSTRAINT fk_guild
        FOREIGN KEY (guildDataId) REFERENCES guilds (guildDataId),
    CONSTRAINT fk_opp_guild
        FOREIGN KEY (opponentGuildDataId) REFERENCES guilds (guildDataId),
    CONSTRAINT fk_gc_event
        FOREIGN KEY (gvgEventId) REFERENCES gc_events (gvgEventId),
    CONSTRAINT fk_gc_day
        FOREIGN KEY (gcDay) REFERENCES gc_days (gcDay),
    PRIMARY KEY (guildDataId, gvgEventId, gcDay)
);

DROP TABLE IF EXISTS player_activity;
CREATE TABLE player_activity
(
    snapshot_date DATE,
    logged_within_1_day INTEGER,
    logged_within_3_days INTEGER,
    logged_within_5_days INTEGER,
    logged_within_7_days INTEGER,
    logged_within_14_days INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    PRIMARY KEY (snapshot_date)
);