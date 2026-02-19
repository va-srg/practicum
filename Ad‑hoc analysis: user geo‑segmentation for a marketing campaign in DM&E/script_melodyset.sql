/*
row_id - уникальный идентификатор записи
user_id - идентификатор пользователя
song - название композиции
artist - исполнитель композиции
genre - музыкальный жанр
region - регион прослушивания
day - день недели прослушивания
duration - длительность прослушивания в секундах
share - доля прослушивания трека
*/

-- перед началом выполнения заданий, добавлю предварительный анализ датасета по каждому полю
SELECT
    -- row_id
	COUNT(row_id) AS count_row_id
,	COUNT(DISTINCT row_id) AS unique_row_id
,	(COUNT(*) - COUNT(row_id)) AS null_row_id
,	(COUNT(row_id) - COUNT(DISTINCT row_id)) AS duplicate_count_row_id
,	MIN(row_id) AS min_row_id
,	MAX(row_id) AS max_row_id
,
    -- user_id
	COUNT(user_id) AS count_user_id
,	COUNT(DISTINCT user_id) AS unique_user_id
,	(COUNT(*) - COUNT(user_id)) AS null_user_id
,	(COUNT(user_id) - COUNT(DISTINCT user_id)) AS duplicate_count_user_id
,	MIN(user_id) AS min_user_id
,	MAX(user_id) AS max_user_id
,
    -- song
	COUNT(song) AS count_song
,	COUNT(DISTINCT song) AS unique_song
,	(COUNT(*) - COUNT(song)) AS null_song
,	(COUNT(song) - COUNT(DISTINCT song)) AS duplicate_count_song
,	MIN(song) AS min_song
,	MAX(song) AS max_song
,
    -- artist
	COUNT(artist) AS count_artist
,	COUNT(DISTINCT artist) AS unique_artist
,	(COUNT(*) - COUNT(artist)) AS null_artist
,	(COUNT(artist) - COUNT(DISTINCT artist)) AS duplicate_count_artist
,	MIN(artist) AS min_artist
,	MAX(artist) AS max_artist
,
    -- genre
	COUNT(genre) AS count_genre
,	COUNT(DISTINCT genre) AS unique_genre
,	(COUNT(*) - COUNT(genre)) AS null_genre
,	(COUNT(genre) - COUNT(DISTINCT genre)) AS duplicate_count_genre
,	MIN(genre) AS min_genre
,	MAX(genre) AS max_genre
,
    -- region
	COUNT(region) AS count_region
,	COUNT(DISTINCT region) AS unique_region
,	(COUNT(*) - COUNT(region)) AS null_region
,	(COUNT(region) - COUNT(DISTINCT region)) AS duplicate_count_region
,	MIN(region) AS min_region
,	MAX(region) AS max_region
,
    -- day_of_week
	COUNT(day) AS count_day_of_week
,	COUNT(DISTINCT day) AS unique_day_of_week
,	(COUNT(*) - COUNT(day)) AS null_day_of_week
,	(COUNT(day) - COUNT(DISTINCT day)) AS duplicate_count_day_of_week
,	MIN(day) AS min_day_of_week
,	MAX(day) AS max_day_of_week
,
    -- duration
	COUNT(duration) AS count_duration
,	COUNT(DISTINCT duration) AS unique_duration
,	(COUNT(*) - COUNT(duration)) AS null_duration
,	(COUNT(duration) - COUNT(DISTINCT duration)) AS duplicate_count_duration
,	MIN(duration) AS min_duration
,	MAX(duration) AS max_duration
,
    -- share
	COUNT(share) AS count_share
,	COUNT(DISTINCT share) AS unique_share
,	(COUNT(*) - COUNT(share)) AS null_share
,	(COUNT(share) - COUNT(DISTINCT share)) AS duplicate_count_share
,	MIN(share) AS min_share
,	MAX(share) AS max_share

FROM 
	study.melodyset
;

-- 1. Вывести первые 10 строк, чтобы изучить ее, при этом не перегружать БД
SELECT 
	*
FROM 
	study.melodyset
LIMIT 
	10;

--2. Вывести первые 15 строк с информацией об исполнителе композиции, названии и музыкальном жанре
SELECT 
	artist
,	song
,	genre
FROM 
	study.melodyset
LIMIT 
	15 
;

--3. Выгрузить все уникальные жанры музыки, отсортировав их
SELECT DISTINCT
	genre AS unique_genres
FROM 
	study.melodyset
ORDER BY 
	unique_genres ASC
;

--4. Определить, какие жанры наиболее популярны среди пользователей: выведя топ пяти жанров по количеству прослушиваний
SELECT 
	genre
,	COUNT(genre) AS count_genres
FROM 
	study.melodyset
GROUP BY 
	genre
ORDER BY 
	count_genres DESC
LIMIT 
	5
;

--5. Коллеги хотели бы визуализировать длительность прослушивания треков разных исполнителей: необходимо вывести название трека, исполнителя, длительность прослушивания трека в минутах; возможно в выборке у исполнителя окажутся одинаковые треки разной длительности - выведем каждую
SELECT 
	song
,	artist
,	CAST(duration AS REAL) / 60 AS song_duration
FROM 
	study.melodyset
ORDER BY
	song_duration DESC
;

--6. Для маркетинговой кампании нужно отобрать жанры, у которых суммарная длительность прослушиваний треков превышает 1870 минут
SELECT 
	genre
,	ROUND(SUM(CAST(duration AS NUMERIC)/60),2) AS song_duration
FROM 
	study.melodyset
GROUP BY 
	genre
HAVING 
	SUM(CAST(duration AS numeric)/60) > 1870
ORDER BY 
	song_duration DESC
;

--7. Собрать статистику прослушиваний музыки по жанрам: для каждого жанра - общее количество и средняя длительность прослушиваний композиций; суммарная, минимальная, максимальная длительность прослушивания композиций; прослушивания треков.
SELECT 
	genre
,	COUNT(row_id) AS count_row_ids
,	SUM(duration) AS sum_song_duration
,	MIN(duration) AS min_song_duration
,	MAX(duration) AS max_song_duration
,	ROUND(AVG(CAST(duration AS NUMERIC)),2) AS average_song_duration
FROM 
	study.melodyset
GROUP BY 
	genre
ORDER BY 
	average_song_duration DESC
;

--8. Выгрузить все прослушивания треков жанра “Reggae” в северо-западном регионе
SELECT 
	*
FROM 
	study.melodyset
WHERE 
	genre = 'Reggae' AND
	region = 'North-Western'
;

--9. Сравнить музыкальные вкусы регионов в одной выборке: выведем информацию о жанре, регионе прослушивания, количестве композиций жанра и суммарной длительности их прослушивания
SELECT 
	genre
,	region
,	COUNT(row_id) AS count_row_ids
,	SUM(duration) AS sum_song_duration
FROM 
	study.melodyset
WHERE 
	region IN ('Central','Southern', 'North-Western')
GROUP BY 
	genre
,	region
ORDER BY 
	region
;

--10. Изучить, сколько раз треки прослушали менее чем наполовину - отразим общее количество таких прослушиваний и их среднюю длительность в минутах
SELECT
	COUNT(*) AS count_rows
,	CEILING(AVG(duration)/60) AS average_song_duration
FROM
	study.melodyset
WHERE
	share < 0.5
;

--11. Для анализа прослушивания треков в понедельник и среду необходима выборка, которая отразит день недели прослушивания, композицию, ее исполнителя, длительность и долю прослушивания для всех воспроизведений в понедельник и среду
SELECT
	day
,	song
,	artist
,	duration
,	share
FROM
	study.melodyset
WHERE
	day IN ('Monday','Wednesday')
;

--12. Изучить, как слушают треки определённых жанров по регионам: выведем информацию о регионе, жанре, названии композиции, об исполнителе, длительности и доли прослушивания. Ограничиться первыми двадцатью строками
SELECT
	region
,	genre
,	song
,	artist
,	duration
,	share
FROM
	study.melodyset
WHERE
	region IN ('Central','Southern', 'North-Western')
ORDER BY
	region ASC
,	genre ASC
,	share DESC
LIMIT
	20
;

--13. Отобразить уникальные пары исполнителя и жанра, где исполнитель имеет более одной композиции в этом жанре, из выборки исключить  все строки где (если) исполнитель неизвестен
SELECT DISTINCT
	artist
,	genre
FROM
	study.melodyset
WHERE
	artist <> 'Unknown'
GROUP BY 
	artist
,	genre
HAVING
	COUNT(song) > 1
ORDER BY
	genre
,	artist
;

--14. Вычислить длину композиции, используя длительность в секундах и долю прослушивания. Исходную длину вычислять только для прослушиваний со значением доли больше 0
SELECT 
	song
,   duration
,   share
,   ROUND(CAST((duration / share) AS NUMERIC), 0) AS length
FROM 
	study.melodyset
WHERE 
	share > 0
;

--15. Провести классификацию композиций по жанрам со значениями: Pop - категория 'Popular Music'; Rock - категория 'Rock Music'; Jazz - категория 'Jazz Music'. Если жанр не указан или не входит в эти категории, то 'Other'
SELECT 
	song
,   genre
,   CASE
    	WHEN genre = 'Pop' THEN 'Popular Music'
    	WHEN genre = 'Rock' THEN 'Rock Music'
    	WHEN genre = 'Jazz' THEN 'Jazz Music'
        	ELSE 'Other'
    END AS genre_category
FROM 
	study.melodyset
;

--16. Разделить прослушивания на категории по их длительности и посчитать количество в каждой категории: меньше 180 секунд - 'Short; от 180 до 300 секунд - 'Medium'; более 300 секунд - 'Long'
SELECT
	CASE
		WHEN duration < 180 THEN 'Short'
		WHEN duration <= 300 THEN 'Medium'
			ELSE 'Long'
	END AS length_category
,	COUNT(*) AS listens_count
FROM 
	study.melodyset
GROUP BY
	length_category
;