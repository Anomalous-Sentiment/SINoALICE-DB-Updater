INSERT INTO gc_days (gcday)
VALUES
    (1),
    (2),
    (3),
    (4),
    (5),
    (6)
ON CONFLICT (gcday) DO NOTHING;

INSERT INTO timeslots (gvgtimetype, timeslot, time_in_utc, gc_available)
VALUES
    (1, 1, '15:00:00+00:00', FALSE),
    (2, 2, '17:00:00+00:00', FALSE),
    (4, 3, '19:00:00+00:00', TRUE),
    (8, 4, '20:00:00+00:00', TRUE),
    (16, 5, '21:00:00+00:00', TRUE),
    (32, 6, '23:00:00+00:00', TRUE),
    (64, 7, '01:00:00+00:00'. TRUE),
    (128, 8, '02:00:00+00:00', TRUE),
    (256, 9, '03:00:00+00:00', TRUE),
    (512, 10, '04:00:00+00:00', TRUE),
    (1024, 11, '12:00:00+00:00', FALSE),
    (2048, 12, '13:00:00+00:00', TRUE),
    (4096, 13, '14:00:00+00:00', TRUE)
ON CONFLICT (gvgtimetype) DO 
    UPDATE SET 
        timeslot = EXCLUDED.timeslot,
        time_in_utc = EXCLUDED.time_in_utc,
        gc_available = EXCLUDED.gc_available;

INSERT INTO guild_ranks (guildrank, rank_letter)
VALUES
    (0, 'D'),
    (1, 'C'),
    (2, 'B'),
    (3, 'A'),
    (4, 'S')
ON CONFLICT (guildrank) DO 
    UPDATE SET 
        rank_letter = EXCLUDED.rank_letter;

INSERT INTO gc_groups (finals_group)
VALUES
    ('A'),
    ('B')
ON CONFLICT (finals_group) DO NOTHING;