
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
                    SELECT CURRENT_DATE, d.start_time::DATE, COUNT(ex.userId)
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
                    SELECT d.start_time ::DATE
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
            ) AS ct("curr_date" DATE, "since_1_day" INTEGER, "since_3_days" INTEGER, "since_5_days" INTEGER, "since_7_days" INTEGER, "since_14_days" INTEGER)
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


CREATE OR REPLACE FUNCTION update_highest_cp() RETURNS TRIGGER AS $update_highest_cp$
    BEGIN
        -- Insert into max cp table. Overwrite/update when totalPower is greater than old value
        INSERT INTO players_max_cp
            SELECT userId, level, currentJobMstId, currentCharacterMstId, totalPower, attackTotalPower, defenceTotalPower, magicAttackTotalPower, magicDefenceTotalPower, maxHp, baseCharacterMstId
            FROM new_table
        ON CONFLICT (userid) DO UPDATE SET
            level = EXCLUDED.level,
            currentJobMstId = EXCLUDED.currentJobMstId,
            currentCharacterMstId = EXCLUDED.currentCharacterMstId,
            totalPower = EXCLUDED.totalPower,
            attackTotalPower = EXCLUDED.attackTotalPower,
            defenceTotalPower = EXCLUDED.defenceTotalPower,
            magicAttackTotalPower = EXCLUDED.magicAttackTotalPower,
            magicDefenceTotalPower = EXCLUDED.magicDefenceTotalPower,
            maxHp = EXCLUDED.maxHp,
            baseCharacterMstId = EXCLUDED.baseCharacterMstId
        WHERE players_max_cp.totalPower < EXCLUDED.totalPower;
        RETURN NULL;
    END;
$update_highest_cp$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS max_cp_upd ON base_player_data;
CREATE TRIGGER max_cp_upd
    AFTER UPDATE ON base_player_data
    REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
    FOR EACH STATEMENT EXECUTE FUNCTION update_highest_cp();

DROP TRIGGER IF EXISTS max_cp_ins ON base_player_data;
CREATE TRIGGER max_cp_ins
    AFTER INSERT ON base_player_data
    REFERENCING NEW TABLE AS new_table
    FOR EACH STATEMENT EXECUTE FUNCTION update_highest_cp();