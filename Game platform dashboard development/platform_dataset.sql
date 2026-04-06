/*
Объединенный датасет для сборки дашборда
    - в cte выбираем данные из таблицы items и применяем динамические условия фильтрации с помощью jinja в секции where (2023-01-01 - 2025-01-01): если в _dttm не выбрана дата, то применяется условие по умолчанию;
    - в основном запросе присоединяем все необходимые нам данные к подготовленным в cte
*/
WITH items_cte AS (
    SELECT 
        game_id
	,   official_launch_at
	,   item_name
	,   available_plf
	,   item_dev
	,   item_publisher
	,   item_genre
	,   item_language
    FROM 
    	items
    WHERE
    	{% IF from_dttm %} 									--если фильтр не пустой
    	official_launch_at >= '{{ from_dttm }}' 			--подставим значение
    	{% ELSE %} 											--если пустой
    	official_launch_at >= date('2023-01-01') 			--используем дату
    	{% endif %}
    AND
   		{% IF to_dttm %}
    	official_launch_at < '{{ to_dttm }}' 
    	{% ELSE %}
    	official_launch_at < date('2025-01-01') 
    	{% endif %}
    AND 
    	{% IF available_plf %} 
    	available_plf IN {{ filter_values('available_plf') | where_in }}
    	{% ELSE %}
    	available_plf IN ('PS Vita', 'PS3', 'PS4', 'PS5')
    	{% endif %}
)
SELECT 
    cte.game_id
,   cte.official_launch_at 									-- дата выпуска
,   cte.item_name
,   cte.available_plf
,   cte.item_dev
,   cte.item_publisher
,   cte.item_genre
,   cte.item_language
,   cu.purchase_at 											-- дата приобретения
,   COALESCE(cu.currency_unit_rub, 0) AS currency_unit_rub
,   bi.player_id
,   CONCAT(bi.game_id, bi.player_id) AS install_id
,   b.badge_id
,   b.badge_name AS badge_title
,   b.badge_feature
,   b.badge_tier
,   g.alias
,   g.location
FROM 
	items_cte AS cte
LEFT JOIN 
	currency_units AS cu ON cte.game_id = cu.game_id
INNER JOIN 
	base_ids AS bi ON cte.game_id = bi.game_id
LEFT JOIN 
	gamers AS g ON g.player_id = bi.player_id
LEFT JOIN 
	badges AS b ON b.game_id = cte.game_id
;