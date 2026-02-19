import pandas as pd
import numpy as np
from num2words import num2words
#создаем необходимые столбцы и будем собирать данные в датафрейм
melodyset = pd.DataFrame({
    'row_id': pd.Series(dtype='Int64'),
    'user_id': pd.Series(dtype='Int64'),
    'song': pd.Series(dtype='string'),
    'artist': pd.Series(dtype='string'),
    'genre': pd.Series(dtype='string'),
    'region': pd.Series(dtype='string'),
    'day': pd.Series(dtype='string'),
    'duration': pd.Series(dtype='Int64'),
    'share': pd.Series(dtype='Float64')
})
# 1. row_id - присвоим по порядку от 300 001 номера
melodyset['row_id'] = range(300001, 350001)

# 2. user_id - распределим рандомно 40 000 из них дополнительно добавим еще 10 000 из созданных, создав искуственно дубли
unique_ids = np.random.choice(
    np.arange(500000, 999999),
    size=40000,
    replace=False
)

np.random.seed(18022026)
additional_ids = np.random.choice(unique_ids, size=10000, replace=True)

all_ids = np.concatenate([unique_ids, additional_ids])
np.random.shuffle(all_ids)

melodyset['user_id'] = all_ids

# 3. song - здесь задумка в том чтобы присвоить префикс каждой песне сопроводив порядковым номером с помощью букв из них уникальны 45 000 порядковых номеров, оставшиеся 5 000 дополним дублями из имеющихся
unique_tracks = [
    'song number ' + num2words(i, lang='en').replace(',', '').strip()
    for i in range(1, 45001)
]

duplicate_tracks = np.random.choice(unique_tracks, size=5000, replace=True)
duplicate_tracks = duplicate_tracks.tolist()

all_tracks = unique_tracks + duplicate_tracks
np.random.shuffle(all_tracks)

melodyset['song'] = all_tracks

# 4. artist - аналогично предыдущему, с изменением лишь префикса и соотношения уников с дублями
unique_artists = [
    'artist number ' + num2words(i, lang='en').replace(',', '').strip()
    for i in range(1, 30001)
]

duplicate_artists = np.random.choice(
    unique_artists,
    size=20000,
    replace=True
).tolist()

all_artists = unique_artists + duplicate_artists
np.random.shuffle(all_artists)

melodyset['artist'] = all_artists

# 5. genre - воспользуемся поисковиком и отберем 100 популярных уникальных жанров, добавив в одно из значений пасхалку - из них для наиболее популярных жанров зададим больший вес, жанры же рандомно распределим для каждой строки
genres = [
    'Pop', 'Hip‑hop', 'Rock', 'Electronic', 'Country',
    'R&B', 'Jazz', 'Blues', 'Reggae', 'Dance', 'Dance‑pop',
    'House', 'Techno', 'Trance', 'Dubstep', 'Trap',
    'EDM', 'Indie', 'Alternative', 'Metal', 'Punk',
    'Funk', 'Soul', 'Folk', 'Classical', 'Opera', 'va-srg',
    'Country pop', 'Country rock', 'Disco', 'Synthpop',
    'Synthwave', 'Ambient', 'Lo‑fi', 'K‑pop', 'J‑pop',
    'Latin pop', 'Reggaeton', 'Salsa', 'Bossa nova',
    'Calypso', 'Mariachi', 'Flamenco', 'Fado', 'World music',
    'Gospel', 'Motown', 'Neo‑soul', 'Grime', 'Drill',
    'Hardcore', 'Hardstyle', 'Drum and bass', 'Garage',
    'UK garage', 'Breakbeat', 'Electro', 'Electro house',
    'Deep house', 'Minimal techno', 'Vaporwave', 'Chiptune',
    'Glitch', 'Experimental', 'Post‑rock', 'Shoegaze',
    'Indie pop', 'Indie rock', 'New wave', 'Post‑punk',
    'Psychedelic rock', 'Surf rock', 'Garage rock', 'Emo',
    'Metalcore', 'Death metal', 'Black metal', 'Doom metal',
    'Thrash metal', 'Progressive rock', 'Art rock', 'Funk rock',
    'Blues rock', 'Soft rock', 'Southern rock', 'Space rock',
    'Power pop', 'Bubblegum pop', 'Eurodance', 'Europop',
    'Britpop', 'Grunge', 'Hard rock', 'Heavy metal',
    'Outlaw country', 'Bluegrass', 'Celtic', 'Folktronica',
    'Alternative rock', 'Pop rock'
]

# задаем вероятности базовые 0,3% для всех жанров
probabilities = np.full(len(genres), 0.003)

top_indices = [
    genres.index('Pop'),
    genres.index('Hip‑hop'),
    genres.index('Rock'),
    genres.index('Electronic'),
    genres.index('Country'),
    genres.index('Jazz'),
    genres.index('Blues'),
    genres.index('Reggae'),
    genres.index('Dance‑pop'),
    genres.index('Trap')
]
probabilities[top_indices] = 0.05  #5% для топа

# нормализуем вероятности (сумма 1)
probabilities /= probabilities.sum()

melodyset['genre'] = np.random.choice(genres, size=50000, p=probabilities, replace=True)

# 6. region - здесь задаем вручную три региона и как принято считать зададим веса примерно в соответствии с населением 45%, 25%, 30%
regions = ['Central', 'North-Western', 'Southern']
weights = [0.45, 0.25, 0.30]  #45%, 25%, 30%
melodyset['region'] = np.random.choice(regions, size=50000, p=weights, replace=True)

# 7. day - заполняем день недели, определив значения вручную
days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
melodyset['day'] = np.random.choice(days_of_week, size=50000)

# 8. duration - целые числа от 55 до 755 включительно, продолжительность прослушивания указанная в секундах, примерно берем значения по длительности одной композиции
melodyset['duration'] = np.random.randint(55, 356, size=50000)

# 9. share - а это как раз доля прослушиваний, тоже задаем рандомом - в конкретном случае нас не интересует достоверность датасета, а именно наличие рандомных данных для демонстраци работы sql-запросов
melodyset['share'] = np.round(np.random.random(size=50000), 2)

# 10. И наконец создаем сам дамп для дальнейшей загрузки в postgresql
OUTPUT_SQL = "melodyset_dump.sql"
DB_NAME = "dataset"
SCHEMA_NAME = "study"
TABLE_NAME = "melodyset"

# cписок SQL‑команд
sql_lines = []

# 1 - переключение на БД (для psql)
sql_lines.append(f"\\c {DB_NAME}\n")
# 2 - создание схемы
sql_lines.append(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};\n")
# 3 - создание таблицы с обновлёнными типами данных
sql_lines.append(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
    row_id      INTEGER PRIMARY KEY,
    user_id     INTEGER NOT NULL,
    song       TEXT,
    artist      TEXT,
    genre       TEXT,
    region      TEXT,
    day         TEXT,
    duration    INTEGER,
    share       NUMERIC(10, 2)
);\n""")
# 4 - заголовок для вставки данных
columns = ', '.join([f'"{col}"' for col in melodyset.columns])
sql_lines.append(f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} ({columns}) VALUES")
# 5 - собираем строки со значениями
values = []
for _, row in melodyset.iterrows():
    row_values = []
    for col_name, val in row.items():
        if pd.isna(val) or val is None:
            row_values.append('NULL')
        elif isinstance(val, str):
            # экранируем одинарные кавычки: ' → ''
            escaped = val.replace("'", "''")
            row_values.append(f"'{escaped}'")
        elif isinstance(val, (int, float)):
            row_values.append(str(val))
        else:
            # для прочих типов в строку и в кавычки
            row_values.append(f"'{str(val)}'")
    values.append(f'({", ".join(row_values)})')
# 6 - добавляем все значения, разделяя запятыми
sql_lines.append(',\n'.join(values) + ';')
# 7 - финальный комментарий
sql_lines.append("\n-- Загрузка завершена")
# 8 - сохраняем в файл
with open(OUTPUT_SQL, 'w', encoding='utf-8') as f:
    f.write('\n'.join(sql_lines))