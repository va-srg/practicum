\c dataset

CREATE SCHEMA IF NOT EXISTS study;


CREATE TABLE IF NOT EXISTS study.melodyset (
    row_id      INTEGER PRIMARY KEY,
    user_id     INTEGER NOT NULL,
    song       TEXT,
    artist      TEXT,
    genre       TEXT,
    region      TEXT,
    day         TEXT,
    duration    INTEGER,
    share       NUMERIC(10, 2)
);

INSERT INTO study.melodyset ("row_id", "user_id", "song", "artist", "genre", "region", "day", "duration", "share") VALUES
(300001, 634762, 'song number twenty-seven thousand eight hundred and ten', 'artist number two thousand nine hundred and sixty-five', 'Dance‑pop', 'North-Western', 'Friday', 227, 0.37),
...
(350000, 898950, 'song number thirteen thousand seven hundred and thirty-one', 'artist number twenty-two thousand five hundred and sixty-one', 'Dance‑pop', 'North-Western', 'Friday', 91, 0.86);

-- Загрузка завершена