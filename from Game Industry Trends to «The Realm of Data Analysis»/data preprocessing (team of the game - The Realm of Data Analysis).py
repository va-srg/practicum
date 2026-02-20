'''
Структура работы
    1. Загрузка и знакомство с данными
    2. Проверка ошибок в данных и их предобработка:
        2.1 Названия или метки столбцов датафрейма
        2.2 Наличие пропусков в данных
        2.3 Типы данных
        2.4 Явные и неявные дубликаты в данных
    3. Фильтрация данных
    4. Категоризация данных:
        4.1 Оценки пользователей
        4.2 Оценки критиков
        4.3 Топ-7 платформ
'''

# 1. Загрузка и знакомство с данными

import pandas as pd
import re

url = 'new_games.csv'

try:
    df = pd.read_csv(url)
    print("данные new_games.csv загружены успешно")

# проверка структуры данных
    print("-" * 3)
    print("информация о датафрейме (df.info())")
    print("-" * 3)
    df.info()

    print("-" * 3)
    print("вывод первых строк (df.head())")
    print("-" * 3)
    print(df.head())

    print("-" * 3)
    print("описательная статистика (df.describe())")
    print("-" * 3)
    print(df.describe())

except FileNotFoundError:
    print(f"ошибка - файл '{url}' не найден")
    exit(1)
except Exception as e:
    print(f"ошибка при загрузке данных {e}")
    exit(1)

# 2. Проверка ошибок в данных и их предобработка
# для самопроверки и отслеживания изменений создадим переменные, считающее количество строк и пропусков - до начала предобработки
initial_rows = df.shape[0]
initial_na_count = df.isna().sum()

# 2.1 Названия или метки столбцов датафрейма

def to_snake_case(text):
    text = re.sub(r'[\s-]+', '_', text)
    text = re.sub(r'(?<=[a-z])(?=[A-Z])', '_', text)
    return text.lower()

print("-" * 3)
print(f"столбцы до приведения к snake_case\n{list(df.columns)}")
df.columns = [to_snake_case(col) for col in df.columns]

print("-" * 3)
print(f"столбцы после приведения к snake_case\n{list(df.columns)}")

# 2.2 Наличие пропусков в данных
print("-" * 3)
missing_absolute = df.isna().sum()
missing_relative = (df.isna().mean() * 100)
missing_absolute = missing_absolute[missing_absolute > 0]
missing_relative = missing_relative[missing_relative > 0].sort_values(ascending=False).round(2)
if not missing_absolute.empty:
    print(f"пропущенных строк в столбцах датафрейма в абсолютных значениях\n{missing_absolute}")
    print("\n")
    print(f"пропущенных строк в столбцах датафрейма в относительных значениях\n{missing_relative}")
else:
    print("пропусков в данных нет")

# с показателями пропусков столбцов year_of_release, name, genre все более прозаично - избавимся от них, т.к. составляют около 1% от всех данных и не повлияют на результат исследования
df.dropna(subset=['year_of_release', 'name', 'genre'], inplace=True)
print("-" * 3)
missing_absolute_afterdropna = df.isna().sum()
missing_relative_afterdropna = (df.isna().mean() * 100)
missing_absolute_afterdropna = missing_absolute_afterdropna[missing_absolute_afterdropna > 0]
missing_relative_afterdropna = missing_relative_afterdropna[missing_relative_afterdropna > 0].sort_values(ascending=False).round(2)
if not missing_absolute_afterdropna.empty:
    print(f"Пропущенных строк в столбцах датафрейма в абсолютных значениях\n{missing_absolute_afterdropna}")
    print("\n")
    print(f"Пропущенных строк в столбцах датафрейма в относительных значениях\n{missing_relative_afterdropna}")
else:
    print("Пропусков в данных нет")

# заполним пропуски для rating
def fill_missing_ratings(df):
    def fill_rating(row):
        if pd.isna(row['rating']):
            # ищем другие значения в той же группе
            group = df[
                (df['genre'] == row['genre']) &
                (df['platform'] == row['platform']) &
                (df['year_of_release'] == row['year_of_release'])
            ]
            # если есть другие значения в группе
            if not group['rating'].isna().all():
                return group['rating'].dropna().iloc[0]
            else:
                return 'RP'
        return row['rating']
    # применяем функцию к каждой строке
    df['rating'] = df.apply(fill_rating, axis=1)
    
    return df

df = fill_missing_ratings(df)

# заполним пропуски для user_score, но учтем что в уникальных значениях столбца, содержатся строки 'tbd' (предположим - to be determined), их тоже необходимо заполнить
def fill_missing_scores(df):
    # преобразуем значения в числовой формат и заменяем 'tbd' на NaN
    df['user_score'] = pd.to_numeric(df['user_score'].replace('tbd', None), errors='coerce')
    # сохраняем групповые средние
    group_means = df.groupby(['genre', 'platform', 'year_of_release'])['user_score'].transform('mean')
    # сохраняем словарь групповых средних
    group_means_dict = df.groupby(['genre', 'platform', 'year_of_release'])['user_score'].mean().to_dict()
    # заполняем пропуски групповыми средними
    df['user_score'] = df['user_score'].fillna(group_means)
    # сохраняем глобальное среднее
    global_mean = df['user_score'].mean()
    # заполняем оставшиеся пропуски глобальным средним
    df['user_score'] = df['user_score'].fillna(global_mean)
    return df

df = fill_missing_scores(df)

# и наконец финальная проверка на пропуски - убедимся, что все ок
print("-" * 3)
missing_absolute_afterfilling = df.isna().sum()
missing_relative_afterfilling = (df.isna().mean() * 100)
missing_absolute_afterfilling = missing_absolute_afterfilling[missing_absolute_afterfilling > 0]
missing_relative_afterfilling = missing_relative_afterfilling[missing_relative_afterfilling > 0].sort_values(ascending=False).round(2)
if not missing_absolute_afterfilling.empty:
    print(f"Пропущенных строк в столбцах датафрейма в абсолютных значениях\n{missing_absolute_afterfilling}")
    print("\n")
    print(f"Пропущенных строк в столбцах датафрейма в относительных значениях\n{missing_relative_afterfilling}")
else:
    print("Пропусков в данных нет")

# 2.3 Типы данных
print("-" * 3)
print(f"информация о датафрейме до оптимизации типов данных\n{df.info()}")
'''
platform - object: преобразуем в категорию, так как это ограниченный набор значений;
year_of_release - int64: оптимизируем до int16, так как диапазон лет ограничен;
genre - object: аналогично платформе преобразуем в категорию, ведь это ограниченный набор значений;
na_sales - float64: попробуем понизить до float32;
eu_sales - object: очевидно должен быть числовым, попробуем преобразовать в float32;
jp_sales - object: также попробуем преобразовать в float32;
other_sales - float64: попробуем понизить до float32;
critic_score - float64: преобразуем в int8, так как оценки критиков целые числа и варьируются от 0 до 100;
user_score - object: преобразуем в float16;
rating - object: преобразуем в категорию, так как рейтинг в данном случае строго ограничен
'''
# оптимизируем типы данных столбцов датафрейма с понижением разрядности (необходимо понимать, что представленные типы данных для числовых значений стоит рассматривать как ожидаемый результат после автоподбора)
# преобразуем названия в строковый тип
df['name'] = df['name'].astype('string')
# преобразуем категориальные переменных
df['platform'] = df['platform'].astype('category')
df['genre'] = df['genre'].astype('category')
df['rating'] = df['rating'].astype('category')
# преобразуем напрямую, так как точно знаем что там только год
df['year_of_release'] = df['year_of_release'].astype('int16')
# преобразуем напрямую оценки критиков, так как диапазон известен
df['critic_score'] = df['critic_score'].astype('int8')
# преобразуем числовые типы с применением автоматического выбора пониженного разряда
df['other_sales'] = pd.to_numeric(df['other_sales'], downcast='float')
df['na_sales'] = pd.to_numeric(df['na_sales'], downcast='float')
df['eu_sales'] = pd.to_numeric(df['eu_sales'], errors='coerce', downcast='float')
df['jp_sales'] = pd.to_numeric(df['jp_sales'], errors='coerce', downcast='float')
df['user_score'] = pd.to_numeric(df['user_score'], downcast='float')
# в ходе преобразования числовых типов столбцов eu_sales и jp_sales происходит ошибка, игнорируя ошибку заполняем их медианными значениями и выводим инфо
df['eu_sales'] = df['eu_sales'].fillna(df['eu_sales'].median())
df['jp_sales'] = df['jp_sales'].fillna(df['jp_sales'].median())

print("-" * 3)
print(f"информация о датафрейме после оптимизации типов данных\n{df.info()}")

# 2.4 Явные и неявные дубликаты в данных
# нормализуем текстовые данные, приведя их к единому регистру
df['name'] = df['name'].str.lower()
df['genre'] = df['genre'].str.lower()
df['platform'] = df['platform'].str.upper()
df['rating'] = df['rating'].str.upper()

print("Проверка регистра данных - вывод трех значений")
print(f"Название\n{df['name'].head(3).tolist()}")
print(f"Жанр\n{df['genre'].head(3).tolist()}")
print(f"Платформа\n{df['platform'].head(3).tolist()}")
print(f"Рейтинг\n{df['rating'].head(3).tolist()}")
print("-" * 3)

# теперь выведем статистику произведенных изменений (на основе переменных, заданных до начала работы с данными)
final_rows = df.shape[0]
rows_removed = initial_rows - final_rows
percent_removed = (rows_removed / initial_rows) * 100
print("\n")
print(f"В процессе обработки данных было удалено {rows_removed} строк, что составляет примерно {percent_removed:.2f}% от общего объема данных")
print(f"- все столбцы после обработки имеют одинаковое количество значений ({final_rows})")
print(f"- общее количество строк уменьшилось на {rows_removed}")

# 3. Фильтрация данных
# для изучения истории продаж игр в начале XXI века, отфильтруем данные по кретерию период с 2000 по 2013 год включительно - новый срез данных сохраним в датафрейме df_actual
df_actual = df[(df['year_of_release'] >= 2000) & (df['year_of_release'] <= 2013)]
df_actual_sort = df_actual['year_of_release'].value_counts().sort_index()
print("-" * 3)
print(f"Распределение игр по годам\n{df_actual_sort}")

# 4. Категоризация данных
print("-" * 3)
print(f"инфо датафрейма с данными за период 2000-2013\n{df_actual.info()}")

# 4.1 Оценки пользователей
df_actual_rating = df_actual.copy()
df_actual_rating['user_rating_category'] = pd.cut(
    df_actual_rating['user_score'],
    bins=[0, 3, 8, 10],
    labels=['низкая', 'средняя', 'высокая'],
    right=False
)
df_actual_user_rating = df_actual_rating['user_rating_category'].value_counts()
print("-" * 3)
print(f"распределение игр по категориям оценок\n{df_actual_user_rating}")

# 4.2 Оценки критиков
df_actual_reviews = df_actual.copy()
df_actual_reviews['critic_rating_category'] = pd.cut(
    df_actual_reviews['critic_score'],
    bins=[0, 30, 80, 100],
    labels=['низкая', 'средняя', 'высокая'],
    right=False
)
df_actual_critic_reviews = df_actual_reviews['critic_rating_category'].value_counts()
print("-" * 3)
print(f"распределение игр по оценкам критиков\n{df_actual_critic_reviews}")

# 4.3 Топ-7 платформ
platform_counts = df_actual['platform'].value_counts()
top_platforms = platform_counts.head(7)
print("-" * 3)
print(f"Топ-7 платформ по количеству игр\n{top_platforms}")