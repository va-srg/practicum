"""Структура работы
1. Загрузка и знакомство с данными
2. Предобработка данных
    2.1 Работаем с пропусками
    2.2 Нормализация данных
    2.3 Преобразуем тип данных
3. Исследовательский анализ данных
    3.1 Исследуем количество объектов общественного питания по каждой категории заведения
    3.2 Исследуем распределение количества заведений по административным районам города
    3.3 Соотношение сетевых и несетевых заведений
    3.4 Исследуем количество посадочных мест в заведениях
    3.5 Исследуем рейтинг заведений
    3.6 Корреляция рейтингов заведений
    3.7 Найдем топ-15 популярных сетевых заведений города
    3.8 Изучим влияние удаленности от центра города на цены в заведениях
4. Итоговый вывод и рекомендации
    4.1 Общий обзор проделанной работы
    4.2 Ответы на исследовательские вопросы или главные выводы
    4.3 Рекомендации на основе анализа данных
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from phik import phik_matrix # для расчета коэффициента корреляции

# 1. Загрузка и знакомство с данными
url_dataset1 = 'catering_establishments_info.csv'
url_dataset2 = 'catering_establishments_price.csv'

try:
    info_df = pd.read_csv(url_dataset1)
    print("данные info_df загружены успешно")
    print("-" * 3)
    print("информация о датафрейме info_df")
    print("\n")
    info_df.info()

    print("-" * 3)
    print("вывод первых строк info_df")
    print("\n")
    print(info_df.head())

    print("-" * 3)
    print("описательная статистика info_df")
    print("\n")
    print(info_df.describe())

    price_df = pd.read_csv(url_dataset2)
    print("\nданные price_df загружены успешно")

    print("-" * 3)
    print("информация о датафрейме price_df")
    print("\n")
    price_df.info()

    print("-" * 3)
    print("вывод первых строк price_df")
    print("\n")
    print(price_df.head())

    print("-" * 3)
    print("описательная статистика price_df")
    print("\n")
    print(price_df.describe())

except FileNotFoundError as e:
    print(f"ошибка файл не найден {e}")
    exit(1)    

except Exception as e:
    print(f"ошибка при загрузке данных {e}")
    exit(1)

# 2. Предобработка данных
# объединим данные двух датасетов в один, с которым и будем дальше работать
rest_df = info_df.merge(price_df, how='left', on='id')
print("-" * 3)
print("информация об объединенном датафрейме rest_df")
print("\n")
rest_df.info()

# 2.1 Работаем с пропусками
# для самопроверки и отслеживания изменений создадим переменные, считающее количество строк и пропусков - до начала предобработки
initial_nan = rest_df.isnull().sum().sum()
initial_rows = rest_df.shape[0]
initial_cols = len(rest_df.columns)
initial_columns = set(rest_df.columns)
print("-" * 3)
missing_absolute = rest_df.isna().sum()
missing_relative = (rest_df.isna().mean() * 100)
missing_absolute = missing_absolute[missing_absolute > 0].sort_values(ascending=False)
missing_relative = missing_relative[missing_relative > 0].sort_values(ascending=False).round(2)
if not missing_absolute.empty:
    print(f"пропущенных строк в столбцах датафрейма в абсолютных значениях\n{missing_absolute}")
    print("\n")
    print(f"пропущенных строк в столбцах датафрейма в относительных значениях\n{missing_relative}")
else:
    print("пропусков в данных нет")

# hours
rest_df['hours'] = rest_df['hours'].replace({
    np.nan: 'нет расписания',
    '' : 'нет расписания',
    ' ' : 'нет расписания',
    None : 'нет расписания'
})

# avg_bill
avg_bills = []

for value in rest_df['avg_bill']:
    try:
        # очистим строки, оставляя только цифры и тире
        cleaned = ''.join([char for char in str(value) if char.isdigit() or char == '–'])
        # разделяем через тире и приводим тип данных к float
        numbers = [float(num) for num in cleaned.split('–')]
        # вычисляем среднее значение, если список не пуст (если после выполнения условия список пустой, то среднее принимаем за 0,0)
        if numbers:
            avg = sum(numbers) / len(numbers)
        else:
            avg = 0.0
        
        avg_bills.append(avg)
        # при любой возникающей ошибке добавляем 0.0
    except:
        avg_bills.append(0.0)

rest_df['avg_bill'] = avg_bills
# чтобы не искажать результаты, заменим заполненные нули на пропуски
rest_df['avg_bill'] = rest_df['avg_bill'].replace(0.0, np.nan)

# middle_coffee_cup, middle_avg_bill
rest_df = rest_df.drop(columns=['middle_coffee_cup', 'middle_avg_bill'])

# для самопроверки и отслеживания изменений создадим переменные, считающее количество строк и пропусков - до начала предобработки
print("-" * 3)
missing_absolute_after_processing = rest_df.isna().sum()
missing_relative_after_processing = (rest_df.isna().mean() * 100)
missing_absolute_after_processing = missing_absolute_after_processing[missing_absolute_after_processing > 0].sort_values(ascending=False)
missing_relative_after_processing = missing_relative_after_processing[missing_relative_after_processing > 0].sort_values(ascending=False).round(2)
if not missing_absolute_after_processing.empty:
    print(f"пропущенных строк в столбцах датафрейма в абсолютных значениях после преобразований\n{missing_absolute_after_processing}")
    print("\n")
    print(f"пропущенных строк в столбцах датафрейма в относительных значениях после преобразований\n{missing_relative_after_processing}")
else:
    print("пропусков после преобразований в данных нет")

# price - ознакомимся с разбросом цен в разрезе районов и категорий без внесения изменений в основной датафрейм (в качестве дополнительной информации)
print("-" * 3)
price_range = rest_df[rest_df['avg_bill'] != 'NaN']
price_range_df = price_range.groupby(['district', 'category', 'price'])['avg_bill'].agg(['min', 'max']).reset_index()
price_range_df['range'] = price_range_df['max'] - price_range_df['min']
price_range_df = price_range_df.set_index(['district', 'category', 'price']).sort_index().round(2)
print('диапазон цен по районам, категориям и ценовым сегментам')
print(price_range_df['min'])

# 2.2 Нормализация данных
# проверяем количество явных дубликатов
def check_duplicates_by_columns(df, columns_to_check):
    duplicates_count = {col: 0 for col in columns_to_check}
    
    for col in columns_to_check:
        duplicates = df[col].duplicated()
        duplicates_count[col] = duplicates.sum()
        
    for col, count in duplicates_count.items():
        print(f"дубликатов в столбце {col} - {count}")

print("-" * 3)
columns_to_check = ['id', 'address', 'name']
check_duplicates_by_columns(rest_df, columns_to_check)

# проверим заинтересовавшие нас дубли по комбинации адрес-название до преобразования
def check_name_address_duplicates(df):
    df_lower = df.copy()
    df_lower['address'] = df_lower['address'].str.lower()
    df_lower['name'] = df_lower['name'].str.lower()
    
    grouped = df_lower.groupby(['address', 'name']).size().reset_index(name='count')
    duplicates = grouped[grouped['count'] > 1]
    total_duplicates = duplicates['count'].sum() # общее количество дубликатов по заданной связке для заведений
    
    if not duplicates.empty:
        print(f"общее количество дублирующихся записей {total_duplicates}\n")
        print("найденные дубликаты в связке address&name")
        
        for index, row in duplicates.iterrows():
            print(f"\nадрес: {row['address']}")
            print(f"название: {row['name']}")
            print(f"количество повторений: {row['count']}")
            
            print("все повторяющиеся записи")
            print(df[(df['address'].str.lower() == row['address']) & (df['name'].str.lower() == row['name'])])
            print("-" * 3)
    else:
        print("дубликатов в связке address&name не найдено")

print("-" * 3)
check_name_address_duplicates(rest_df)

# заведение сетевое?
count_name = ['more poke', 'раковарня клешни и хвосты', 'хлеб да выпечка']
for name in count_name:
    print("-" * 3)
    print(f"количество '{name}': {(rest_df['name'].str.lower() == name.lower()).sum()}")
rest_df = rest_df.drop(index=[189, 1430, 2211, 3109])

# проверим дубли по комбинации адрес-название после обработки
print("-" * 3)
check_name_address_duplicates(rest_df)

# 2.3 Преобразуем тип данных
string_cols = ['id', 'name', 'address']
rest_df[string_cols] = rest_df[string_cols].astype('string') # string
rest_df['rating'] = pd.to_numeric(rest_df['rating'], downcast='float')  # float32
rest_df['chain'] = pd.to_numeric(rest_df['chain'], downcast='integer')  # int8
categorical_cols = ['district', 'category', 'hours', 'price']
rest_df[categorical_cols] = rest_df[categorical_cols].astype('category') # category
rest_df['seats'] = pd.to_numeric(rest_df['seats'], errors='coerce').astype('float32') # float32 if not NaN
rest_df['avg_bill'] = pd.to_numeric(rest_df['avg_bill'], errors='coerce').astype('float32') # float32 if not NaN

# по требованию заказчика, дополним итоговый датасет столбцом `is_24_7` с обозначением того, что заведение работает ежедневно и круглосуточно, то есть 24/7 (на основе данных имеющихся в 'hours'): true - если заведение работает ежедневно и круглосуточно; логическое значение false - в противоположном случае
rest_df['is_24_7'] = rest_df['hours'].str.lower().str.contains('ежедневно, круглосуточно|ежедневно круглосуточно|ежедневно,круглосуточно', na=False).astype(bool)
print("-" * 3)
print(f'содержание столбца is_24_7\ntrue - {rest_df['is_24_7'].value_counts().get(True, 0)}\nfalse - {rest_df['is_24_7'].value_counts().get(False, 0)}')

# статистика после обработки
final_nan = rest_df.isnull().sum().sum()  # NaN после обработки
final_rows = rest_df.shape[0]  # строк
final_cols = len(rest_df.columns)  # столбцов
final_columns = set(rest_df.columns) # список всех имен столбцов датафрейма
nan_removed = initial_nan - final_nan  # удаленных NaN
rows_removed = initial_rows - final_rows  # удаленных строк
removed_columns = initial_columns - final_columns # удаленных столбцов
added_columns = final_columns - initial_columns # созданных столбцов
data_types = rest_df.dtypes.value_counts()  # распределение типов данных
print("-" * 3)
print('\nстатистика датафрейма после обработки')
print(f'\nколичество удаленных строк {rows_removed}')
print(f'количество удаленных столбцов {len(removed_columns)} - {', '.join(removed_columns)}')
print(f'количество добавленных столбцов {len(added_columns)} - {', '.join(added_columns)}')
print(f'количество обработанных пропусков {nan_removed}')

print('\nитоговая статистика')
print(f'количество оставшихся строк {final_rows}')
print(f'количество оставшихся столбцов  {final_cols}')
print(f'количество оставшихся пропусков  {final_nan}')

print('\nраспределение типов данных (столбцов)')
for dtype, count in data_types.items():
    print(f'{dtype} - {count}')

# 3. Исследовательский анализ данных
# 3.1 Исследуем количество объектов общественного питания по каждой категории заведения
def show_plot_with_delay(delay=10):
    """
    отображает график на указанное количество секунд, затем закрывает его
    параметры: delay (int) - время отображения графика в секундах (по умолчанию 10)
    """
    plt.tight_layout()
    plt.show(block=False)
    plt.pause(delay)
    plt.close()

# столбчаатя диаграмма распределения категорий заведений
sorted_category = rest_df['category'].value_counts().sort_values(ascending=True)
cmap = plt.get_cmap('coolwarm') #  градиент
colors = cmap(np.linspace(0, 1, len(sorted_category)))
ax = sorted_category.plot(
    kind='barh',
    figsize=(11, 5),
    color=colors,
    edgecolor='gray',
    linewidth=0.5
)
ax.grid(axis='x',  color='gray', linewidth=0.1) # горизонтальная сетка
for cont in ax.containers:
    ax.bar_label(cont, padding=3, color='black')
ax.set_title('Распределение категорий заведений в городе')
ax.set_xlabel('Количество в выборке', color='gray')
ax.set_ylabel('Категория (тип) заведения', color='gray')
show_plot_with_delay()

# 3.2 Исследуем распределение количества заведений по административным районам города, а также отдельно по каждой категории в Центральном административном округе
# столбчатая диаграмма распределения заведений по административным районам
sorted_districts = rest_df['district'].value_counts().sort_values(ascending=True)
cmap = plt.get_cmap('coolwarm')
colors = cmap(np.linspace(0, 1, len(sorted_districts)))
ax = sorted_districts.plot(
    kind='barh',
    figsize=(11, 5),
    color=colors,
    edgecolor='gray',
    linewidth=0.5
)
ax.grid(axis='x', color='gray', linewidth=0.1)
for cont in ax.containers:
    ax.bar_label(cont, padding=3, color='black')
ax.set_title('Распределение заведений по административным районам')
ax.set_xlabel('Количество заведений', color='gray')
ax.set_ylabel('Административный район', color='gray')
show_plot_with_delay()

# столбчатая диаграмма распределения заведений в центральном округе
sorted_categories = (
    rest_df[rest_df['district'] == 'Центральный административный округ']
    ['category']
    .value_counts()
    .sort_values(ascending=True)
)
cmap = plt.get_cmap('coolwarm')
colors = cmap(np.linspace(0, 1, len(sorted_categories)))
ax = sorted_categories.plot(
    kind='barh',
    figsize=(11, 5),
    color=colors,
    edgecolor='gray',
    linewidth=0.5
)
ax.grid(axis='x', color='gray', linewidth=0.1)
for cont in ax.containers:
    ax.bar_label(cont, padding=3, color='black')
ax.set_title('Распределение заведений по категориям в ЦАО')
ax.set_xlabel('Количество заведений', color='gray')
ax.set_ylabel('Категория заведения', color='gray')
show_plot_with_delay()

# тепловая карта распределения категорий заведений по административным районам города без учета центрального
heatmap_without_central_district = rest_df.copy()
heatmap_without_central_district = heatmap_without_central_district[heatmap_without_central_district['district'] != 'Центральный административный округ'] # исключим строки с ЦАО
heatmap_without_central_district['district'] = heatmap_without_central_district['district'].cat.remove_unused_categories() # удаляем категории, которые больше не представлены в данных
district_category_counts = (
    heatmap_without_central_district
    .groupby(['district', 'category'], observed=False)
    .size()
    .unstack(fill_value=0) # группировка строк район-категория, их подсчет и преобразование в таблицу
)
plt.figure(figsize=(15, 11)) 
sns.heatmap(
    district_category_counts, 
    annot=True, # отобразим числовые значения в ячейках
    cmap='coolwarm',
    fmt='g', # формат чисел (без фиксированного числа знаков)
    cbar_kws={'label': '\nКоличество заведений'},
    linewidths=0.5,
    edgecolor='gray'
)
plt.title('Распределение категорий заведений по районам (без ЦАО)')
plt.xlabel('\nКатегория заведения', color='gray')
plt.ylabel('\nАдминистративный район', color='gray')
show_plot_with_delay()

# 3.3 Соотношение сетевых и несетевых заведений в целом по всем данным и в разрезе категорий заведения
print("-" * 3)
print('соотношение сетевых и несетевых заведений')
print(pd.DataFrame({
    'total_count': (counts := rest_df['chain'].map({1: 'сетевое', 0: 'несетевое'}).value_counts()), # присвоим расчет и вернем значение
    'total_percentage': (counts / counts.sum() * 100).round(2)
}))
category_analysis = rest_df.groupby('category', observed=True)['chain'].agg(['sum', 'count'])
category_analysis['percentage'] = (category_analysis['sum'] / category_analysis['count'] * 100).round(2)
category_analysis = category_analysis.sort_values('percentage', ascending=False)
print("-" * 3)
print('топ категорий по доле сетевых заведений')
print(category_analysis[['sum', 'count', 'percentage']].head().to_string())

# пайчарт соотношения сетевых и несетевых заведений
chain_counts = rest_df['chain'].value_counts() # распределение данных по значениям столбца 'chain'
cmap = plt.get_cmap('coolwarm_r')
colors = cmap(np.linspace(0, 1, len(chain_counts)))
plt.figure(figsize=(5, 5))
texts, autotexts = plt.pie(
    chain_counts,
    labels=['Несетевые', 'Сетевые'],
    startangle=90, # поворот диаграммы
    wedgeprops={'linewidth': 1, 'edgecolor': 'white'}, # настройка границ секторов
    autopct='%.1f%%', # проценты в каждом секторе
    colors=colors
)[1:3]  # [1] — подписи, [2] — проценты

for text in texts:
    text.set_color('black') # цвет подписей секторов
for autotext in autotexts:
    autotext.set_color('white') # цвет процентов
plt.title('Соотношение сетевых и несетевых заведений')
plt.axis('equal')  # круглая (выравнивание масштаба осей)
show_plot_with_delay()

# столбчатая диаграмма по доле сетевых заведений в каждой из категорий
plt.figure(figsize=(11, 5))
cmap = plt.get_cmap('coolwarm_r')
colors = cmap(np.linspace(0, 1, len(category_analysis)))
plt.bar(
    category_analysis.index, # названия категорий
    category_analysis['percentage'], # высота столбцов (процент по процентной доле сетевых заведений)
    color=colors,
    linewidth=0.5,
    edgecolor='gray'
)
for i, percentage in enumerate(category_analysis['percentage']): # аннотация
    plt.text(
        i, # позиция по оси X (индекс столбца)
        percentage + 2, # позиция по оси Y (на 2 пункта выше столбца)
        f'{percentage:.2f}%\n({category_analysis["count"].iloc[i]} заведений)',
        ha='center' # горизонтальное выравнивание по центру
    )
plt.ylabel('Доля сетевых заведений (%)', color='gray')
plt.title('Доля сетевых заведений по категориям')
plt.xticks(rotation=0)
plt.margins(y=0.15)
show_plot_with_delay()

# 3.4 Исследуем количество посадочных мест в заведениях
print("-" * 3)
print("описательная статистика столбца seats")
rest_df['seats'].describe()

# гистограмма распределения посадочных мест
plt.figure(figsize=(15, 11))
sns.histplot(rest_df['seats'], kde=True, color='gray', edgecolor='gray', linewidth=0.5) # с кривой плотности распределения
plt.title('Распределение количества посадочных мест')
plt.xlabel('Количество мест', color = 'gray')
plt.ylabel('Частота', color = 'gray')
plt.xlim(0,1500)
plt.grid(axis='y', alpha=0.5)
show_plot_with_delay()

# ящик распределения посадочных мест
plt.figure(figsize=(11, 5))
sns.boxplot(y=rest_df['seats'], color='gray', boxprops={
        'edgecolor': 'gray',
        'linewidth': 0.5
    })
plt.title('Распределение количества посадочных мест')
plt.ylabel('Количество мест', color = 'gray')
plt.ylim(0,1500)
show_plot_with_delay()

# гистограмма распределения мест по категориям (до 200)
seats_by_category_200 = rest_df[rest_df['seats'] <= 200]
categories = seats_by_category_200['category'].unique()
plt.figure(figsize=(15, 11))
sns.histplot(
    data=seats_by_category_200,
    x='seats',
    hue='category',
    multiple='stack', # схлопнем столбцы "стопкой"
    kde=True, # добавим сглаженную кривую
    palette='coolwarm_r',
    binwidth=5, # ширина бина
    edgecolor='gray',
    linewidth=0.5
)
plt.legend(categories, title='Категория', loc='upper right')
plt.title('Распределение мест по категориям заведений (до 200 мест)')
plt.xlabel('Количество мест', color='gray')
plt.ylabel('Частота', color='gray')
plt.xlim(0, 200)
plt.grid(axis='y', alpha=0.1)
show_plot_with_delay()
# покажем среднее, медиану и число заведений в категории
print("-" * 3)
print(f'статистика по категориям\n{rest_df.groupby('category', observed=False)['seats'].agg(['mean', 'median', 'count'])}')

# 3.5 Исследуем рейтинг заведений
print("-" * 3)
print("описательная статистика столбца rating")
rest_df['rating'].describe()
category_ratings = rest_df.groupby('category', observed=False)['rating'].agg(avg_rating='mean', median_rating='median', std_rating='std', count='count').sort_values('avg_rating', ascending=False)
print("-" * 3)
print('статистика по категориям')
print(category_ratings.head().to_string())

# ящик - сравнение рейтингов в зависимости от категории заведения
plt.figure(figsize=(15, 11))
sns.boxplot(
    x='rating',
    y='category',
    hue='category', # цвет ящиков по категориям
    data=rest_df,
    order=category_ratings.index, # на оси Y используем индекс category_ratings
    palette='coolwarm_r',
    legend=False
)
plt.title('Сравнение рейтингов по категориям заведений', fontsize=14)
plt.xlabel('Рейтинг заведения', fontsize=10, color='gray')
plt.ylabel('Категория', fontsize=10, color='gray')
plt.xticks(ticks=range(1, 6)) # зададим деления на оси X
plt.grid()
show_plot_with_delay()

# 3.6 Изучим корреляцию рейтингов заведений (проверим самую сильную связь)
# корреляция рейтинга
numeric_columns = ['rating', 'chain', 'seats', 'avg_bill', 'is_24_7']
numeric_data = rest_df[numeric_columns]
corr_matrix = numeric_data.corr()
plt.figure(figsize=(15, 11))
sns.heatmap(
    corr_matrix,
    annot=True, # значения в ячейках
    cmap='coolwarm',
    fmt='.2f',
    cbar_kws={'label': 'Коэффициент корреляции'},
    linewidths=0.5,
    edgecolor='gray',
    vmin=-1, # минимальное значение шкалы
    vmax=1 # максимальное значение шкалы
)
plt.title('Матрица корреляции числовых параметров')
show_plot_with_delay()

print("-" * 3)
print(f'переменная наиболее сильно коррелирующая с rating\n{corr_matrix['rating'].abs().sort_values(ascending=False).iloc[1:]}')
print("-" * 3)
print(f'параметр с максимальной корреляцией - {max_corr.index[0]}\nего значение - {max_corr.iloc[0]}')
print(f'параметр с максимальной корреляцией - {max_corr.index[0]}\nего значение - {max_corr.iloc[0]}')
print("-" * 3)
print(f'дополнительная статистика по параметру с максимальной корреляцией\n{numeric_data[['rating', max_corr.index[0]]].describe()}')

# корреляция рейтинга с дополнительными анализируемыми столбцами
all_columns = ['rating', 'chain', 'seats', 'avg_bill', 'is_24_7', 'category', 'district', 'price']
valid_data = rest_df[all_columns].dropna()
for col in all_columns:
    if col not in ['chain', 'seats', 'avg_bill', 'is_24_7']:
        valid_data[col] = valid_data[col].astype('category').cat.codes # преобразуем все категориальные столбцы в числовые коды
corr_matrix = valid_data.corr()
plt.figure(figsize=(15, 11))
sns.heatmap(
    corr_matrix,
    annot=True,
    cmap='coolwarm',
    fmt=".2f", # формат чисел
    cbar_kws={'label': 'Коэффициент корреляции'}, #подпись шкалы
    linewidths=0.5,
    edgecolor='gray',
    vmin=-1,
    vmax=1
)
plt.title('Матрица корреляции рейтинга с параметрами заведения')
show_plot_with_delay()
print("-" * 3)
print(f'переменная наиболее сильно коррелирующая с rating\n{corr_matrix['rating'].abs().sort_values(ascending=False).iloc[1:]}')
print("-" * 3)
print(f'параметр с максимальной корреляцией - {max_corr.index[0]}\nего значение - {max_corr.iloc[0]}')
print(f'параметр с максимальной корреляцией - {max_corr.index[0]}\nего значение - {max_corr.iloc[0]}')
print("-" * 3)
print(f'дополнительная статистика по параметру с максимальной корреляцией\n{valid_data[['rating', max_corr.index[0]]].describe()}')

# посмотрим распределение значений price
if max_corr.index[0] in ['category', 'district', 'price']: # если параметр был категориальным, восстанавливаем исходные значения
    original_values = rest_df.loc[valid_data.index, max_corr.index[0]]  # # берем исходные данные из rest_df без NaN
else:
    original_values = valid_data[max_corr.index[0]]
value_counts = pd.DataFrame({'частота': original_values.value_counts()})  # пересчитываем частоты и доли
value_counts['доля, %'] = (value_counts['частота'] / value_counts['частота'].sum() * 100).round(1)
print("-" * 3)
print(f'распределение значений `{max_corr.index[0]}`')
print(value_counts)

# 3.7 Найдем топ-15 популярных сетевых заведений города
top_15 = (
    rest_df[rest_df['chain'] == 1]  # оставляем только сетевые заведения
    .groupby('name')
    .agg(
        count=('name', 'size'),
        avg_rating=('rating', 'mean'),
        categories=('category', 'unique')
    )
    .reset_index()
    .sort_values('count', ascending=False)
    .head(15)
)
print("-" * 3)
print("топ-15 популярных заведений города по убыванию")
for _, row in top_15.iterrows():
    categories = ', '.join(row['categories'])
    print(f"\nсеть: {row['name']} ({categories})")
    print(f"средний рейтинг: {row['avg_rating']:.2f}")
    print(f"количество заведений: {row['count']}")

# построим горизонтальную столбчатую диаграмму для визуализации топа
top_15['full_name'] = top_15.apply(lambda row: f"{row['name']} ({', '.join(row['categories'])})", axis=1) # для подписей применяем lambda к каждой строке объединяя название сети и категории
plt.figure(figsize=(15, 11))
ax = sns.barplot(
    x='count', # количество заведений
    y='full_name', # составное название сети с категориями
    data=top_15,
    hue='full_name',  # цветовая дифференциация столбцов
    palette='coolwarm_r',
    legend=False
)
ax.grid(axis='x', color='gray', linewidth=0.1) # сетка оси
plt.title('Количество точек у топ-15 сетей')
plt.xlabel('Количество заведений', color='gray')
plt.ylabel('Название сети (категории)', color='gray')
show_plot_with_delay()

# 3.8 Изучим влияние удаленности от центра города на цены в заведениях
print("-" * 3)
print(rest_df['avg_bill'].describe())
# дополнительно сопроводим визуализируем ящиком
plt.figure(figsize=(15, 11))
ax = sns.boxplot(
    x='avg_bill',
    y='district',
    data=rest_df,
    hue='district', # явно указываем, что цвет соответствует district
    palette='coolwarm_r',
    legend=False
)
plt.axvline(x=5000, color='red', linestyle='--') # добавляем линию-аннотацию на уровне 5000 руб.
plt.text(5000, 0.1, ' ограничение до 5000 руб.', # подпись линии-аннотации
         horizontalalignment='left', # выравнивание текста к линии слева
         verticalalignment='center', # центрировать по вертикали относительно точки
         color='red')
plt.xlim(0, 8000) # предел оси X
plt.title('Сравнение среднего чека по районам')
plt.xlabel('Средний чек (руб.)', color='gray')
plt.ylabel('Округ', color='gray')
show_plot_with_delay()
# выведем дополнительную статистику по 'ЦАО' и 'другим административным районам'
cao_rests = rest_df[rest_df['district'] == 'Центральный административный округ']
other_rests = rest_df[rest_df['district'] != 'Центральный административный округ']
print("-" * 3)
print(
    f"\nсредний чек в ЦАО - {cao_rests['avg_bill'].mean():.2f}"
    f"\nмедианный чек в ЦАО - {cao_rests['avg_bill'].median():.2f}"
    f"\nразброс цен в ЦАО - {cao_rests['avg_bill'].std():.2f}"
    f"\nсредний чек в других районах - {other_rests['avg_bill'].mean():.2f}"
    f"\nмедианный чек в других районах - {other_rests['avg_bill'].median():.2f}"
    f"\nразброс цен в других районах - {other_rests['avg_bill'].std():.2f}"
)

# 4. Итоговый вывод, рекомендации и подробное описание в тетрадке jupyter


