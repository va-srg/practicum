"""Структура работы
1. Предобработка данных: загрузка и преобразование
    1.1 Вывод общей информации, первичное знакомство с исходными данными
    1.2 Смена типов и анализ пропусков
2. Выборочное исследование по запросу компании-заказчика
    2.1 Раунды финансирования по годам
    2.2 Люди и их образование
    2.3 Поиск столбцов для объединения датасетов
    2.4 Проблемный датасет и причина возникновения пропусков
3. Исследовательский анализ данных объединенного датасета
    3.1 Объединение данных
    3.2 Анализ выбросов
    3.3 Исследуем компании, проданные за символическую стоимость
    3.4 Цены стартапов по категориям
    3.5 Количество раундов стартапа перед покупкой
4. Итоговый вывод и рекомендации
    
"""

# 1. Предобработка данных: загрузка и преобразование
import re
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib_venn import venn2
import missingno as msno

    # 1.1 Вывод общей информации, первичное знакомство с исходными данными
url = '/datasets/'
startup_acq_df = 'acquisition.csv'
startup_fin_df = 'company_and_rounds.csv'
employee_ed_df = 'people.csv'
employee_profiles_df = 'education.csv'
employee_degrees_df = 'degrees.csv'

# напишем функцию для загрузки данных каждого датасета и их анализа
def load_dataset(file_path, df_name="dataset"):
    """
    загружает и выводит информацию, первые строки и описательную статистику
    параметры на вход
    file_path - путь к csv‑файлу для загрузки и анализа
    df_name- имя датафрейма для отображения в сообщениях (по умолчанию "dataframe")
    """
    full_path = url + file_path
    try:
        df = pd.read_csv(full_path)
        print(f"данные {df_name} загружены успешно - {df.shape[0]} строк, {df.shape[1]} столбцов")
        print("-" * 3)
        
        print(f"информация о датафрейме {df_name}")
        df.info()

        print("-" * 3)
        print(f"вывод первых строк датафрейма {df_name}")
        print(df.head())

        print("-" * 3)
        print(f"описательная статистика {df_name}")
        print(df.describe())
        print("-" * 3)
        return df
    except FileNotFoundError as e:
        print(f"ошибка файл {df_name} не найден {e}")
        return None
    except pd.errors.EmptyDataError:
        print(f"ошибка файл {df_name} пуст")
        return None
    except Exception as e:
        print(f"произошла ошибка при загрузке {df_name}: {e}")
        return None

#  информация о сделках по поглощению компаний
startup_acq_df = load_dataset('acquisition.csv', 'startup_acq')
# информация о компаниях и их инвестиционном финансировании
startup_fin_df = load_dataset('company_and_rounds.csv', 'startup_fin')
    # особое внимание обратим на столбцы company  ID и company  id - они имеют одинаковые названия
first_id = set(startup_fin_df['company  ID'].dropna().unique())
second_id = set(startup_fin_df['company  id'].dropna().unique())
    # пересечения на основании схемы
intersection = first_id.intersection(second_id)
only_first = first_id.difference(second_id)
only_second = second_id.difference(first_id)
print("-" * 3)
print(f"уникальных в company_id - {len(only_first)}")
print(f"уникальных в company_id_round - {len(only_second)}")
print(f"пересечений в результатах - {len(intersection)}")
print(f"процент совпадений - {len(intersection)/(len(first_id) + len(second_id) - len(intersection))*100:.2f}%")
print("-" * 3)

# напишем функцию для вызова при обращении к выводу графиков
def show_plot_with_delay(delay=10, save_path='plot.png', dpi=300):
    """
    отображает график на указанное количество секунд, сохраняет, затем закрывает
    параметры:
        delay (int) - время отображения графика в секундах (по умолчанию 10);
        save_path (str) - путь для сохранения файла (по умолчанию 'plot.png');
        dpi (int) - разрешение изображения в точках на дюйм (по умолчанию 300)
    """
    plt.tight_layout()
    plt.show(block=False)
    plt.pause(delay)
    plt.savefig(save_path, dpi=dpi, bbox_inches='tight')
    plt.close()
    print(f"диаграмма {save_path} сохранена в директории")

    # сопроводим пересечения диаграммой венна
venn2([first_id, second_id], set_labels=('company id', 'company id round'))
plt.title("Пересечение 'company id' и 'company id round'")
show_plot_with_delay(10, '1-1_venn.png')

# пересечение наборов показывает наличие 31707 общих id (16%) - преобразуем nan, заполнив значения 'company  ID' строками из 'company  id'
initial_mismatches = (startup_fin_df['company  ID'] != startup_fin_df['company  id']).sum()
initial_na = startup_fin_df['company  ID'].isna().sum()
initial_unique_ID = startup_fin_df['company  ID'].nunique()
initial_unique_id = startup_fin_df['company  id'].nunique()
nan_rows = startup_fin_df['company  ID'].isna()
startup_fin_df.loc[nan_rows, 'company  ID'] = startup_fin_df.loc[nan_rows, 'company  id'] # заполняем NaN в 'company  ID' значениями из 'company  id'
remaining_mismatches = (startup_fin_df['company  ID'] != startup_fin_df['company  id']).sum()
remaining_na = startup_fin_df['company  ID'].isna().sum()
unique_company_ID = startup_fin_df['company  ID'].nunique()
unique_company_id = startup_fin_df['company  id'].nunique()
print("-" * 3)
print("до изменений / nосле изменений") # результат
print(f"несовпадений в 'company  ID' - {initial_mismatches} / оставшихся несовпадений - {remaining_mismatches}")
print(f"NaN в 'company  ID' - {initial_na} / оставшихся NaN в 'company  ID' - {remaining_na}")
print(f"уникальных значений в 'company  ID' - {initial_unique_ID} / уникальных значений в 'company  ID' - {unique_company_ID}")
print(f"уникальных значений в 'company  id' - {initial_unique_id} / уникальных значений в 'company  id' - {unique_company_id}")

# видим, что столбец id компаний соответствует раунду финансирования - переименуем в `company_id_round`
startup_fin_df = startup_fin_df.rename(columns={'company  id': 'company_id_round'})

# приведем названия столбцов датафрейма к snake_case
def to_snake_case(text):
    text = re.sub(r'[\s-]+', '_', text)
    text = re.sub(r'(?<=[a-z])(?=[A-Z])', '_', text)
    return text.lower()
startup_fin_df.columns = [to_snake_case(column) for column in startup_fin_df.columns]
print("-" * 3)
print(list(startup_fin_df.columns))

#  информация о людях и их профессиональной принадлежности
employee_ed_df = load_dataset('people.csv', 'employee_ed')

# информация об образовании людей
employee_profiles_df = load_dataset('education.csv', 'employee_profiles')
# при выводе, в одном из столбцов видим грамматическую ошибку - поправим
employee_profiles_df = employee_profiles_df.rename(columns={'instituition': 'institution'})
print("-" * 3)
print(list(employee_profiles_df.columns))

# информация об академических степенях и специальностях
employee_degrees_df = load_dataset('degrees.csv', 'employee_degrees')

    # 1.2 Смена типов и анализ пропусков
dfs = [startup_acq_df, startup_fin_df, employee_ed_df, employee_profiles_df, employee_degrees_df]
name_dfs = ['startup_acq_df', 'startup_fin_df', 'employee_ed_df', 'employee_profiles_df', 'employee_degrees_df']

# функция привоит к datetime столбцы, содержащие в названии '_at'
def columns_to_datetime(df_list):
    """
    из списка ищет столбцы с окончанием '_at' и преобразует их в формат datetime
    параметры
    df_list - список датафреймов
    """
    for df in df_list:
        at_columns = [column for column in df.columns if column.endswith('_at')]

        for column in at_columns:
            try:
                df[column] = pd.to_datetime(df[column])
                print(f"столбец '{column}' успешно преобразован в datetime")
            except Exception as e:
                print(f"ошибка при преобразовании столбца '{column}' {e}")

    return df_list

print("-" * 3)
datetime_columns = columns_to_datetime(dfs)

# функция оптимизации строковых типов данных
def columns_to_string(df_list):
    """
    из списка датафреймов ищет столбцы, названия которых содержат 'name' и преобразует их в строковый тип
    параметры:
    df_list - список датафреймов
    """
    for df in df_list:
        name_columns = [column for column in df.columns if 'name' in column.lower()]
        
        for column in name_columns:
            try:
                df[column] = df[column].astype(str)
                print(f"столбец '{column}' успешно преобразован в string")
            except Exception as e:
                print(f"ошибка при преобразовании столбца '{column}': {e}")

    return df_list

print("-" * 3)
string_columns = columns_to_string(dfs)

# функция оптимизации числовых типов данных
def optimize_memory_usage(df: pd.DataFrame, df_name: str = "dataframe", print_size: bool = True) -> pd.DataFrame:
    """
    оптимизирует использование памяти в датафрейме, подбирая оптимальные типы данных
    параметры
    df - датафрейм;
    df_name - имя датафрейма (по умолчанию dataframe);
    print_size - показывать ли результат (по умолчанию True)
    """
    memory_before = df.memory_usage().sum() / 1024**2

    for column_name in df.columns:
        dtype = df[column_name].dtype

        if dtype == 'Int64':
            continue

        if not (pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(dtype)):
            continue

        if 'round' in column_name.lower() or 'id' in column_name.lower():
            has_nulls = df[column_name].isnull().any()
            if has_nulls:
                df[column_name] = df[column_name].astype('Int64')
                print(f"столбец '{column_name}' преобразован в Int64 (содержит пропуски)")
                continue
            else:
                df[column_name] = df[column_name].astype('int64')
                print(f"столбец '{column_name}' приведён к int64 для дальнейшей оптимизации разрядности")
                dtype = df[column_name].dtype

        if pd.api.types.is_integer_dtype(dtype):
            min_value, max_value = df[column_name].min(), df[column_name].max()
            original_dtype = dtype

            for opt_int in [np.int8, np.int16, np.int32]:
                int_info = np.iinfo(opt_int)
                if (min_value >= int_info.min and
                    max_value <= int_info.max):
                    df[column_name] = df[column_name].astype(opt_int)
                    break
            else:
                print(f"столбец '{column_name}' оставлен как {original_dtype} (значения выходят за диапазон меньших целочисленных типов)")

        elif pd.api.types.is_float_dtype(dtype):
            min_value, max_value = df[column_name].min(), df[column_name].max()
            if np.isfinite(min_value) and np.isfinite(max_value):  # Проверка на inf/-inf
                if (min_value >= np.finfo(np.float16).min and
                    max_value <= np.finfo(np.float16).max):
                    df[column_name] = df[column_name].astype(np.float16)
                elif (min_value >= np.finfo(np.float32).min and
                      max_value <= np.finfo(np.float32).max):
                    df[column_name] = df[column_name].astype(np.float32)

    memory_after = df.memory_usage().sum() / 1024**2

    if print_size:
        memory_reduction_percentage = 100 * (memory_before - memory_after) / memory_before
        print("-" * 3)
        print(f'размер использования памяти {df_name}\nдо {memory_before:5.2f} Мб - после {memory_after:5.2f} Мб '
              f'(разница {memory_reduction_percentage:.1f}%)')

    return df

# функция проверки пропусков
def missing_values_report(df):
    """
    смотрим пропуски - абсолютные и относительные значения
    параметры
    df - датафрейм
    """
    print("-" * 3)
    missing_absolute = df.isna().sum()
    missing_relative = (df.isna().mean() * 100)
    missing_absolute_filtered = missing_absolute[missing_absolute > 0].sort_values(ascending=False)
    missing_relative_filtered = missing_relative[missing_relative > 0].sort_values(ascending=False).round(2)
    if not missing_absolute_filtered.empty:
        print(f"пропущенных строк в столбцах датафрейма в абсолютных значениях после преобразований\n{missing_absolute_filtered}")
        print("\n")
        print(f"пропущенных строк в столбцах датафрейма в относительных значениях после преобразований\n{missing_relative_filtered}")
    else:
        print("пропусков после преобразований в данных нет")

# startup_acq_df
print("-" * 3)
startup_acq_df = optimize_memory_usage(startup_acq_df, "startup_acq_df")
missing_values_report(startup_acq_df)

term_codes = startup_acq_df['term_code'].dropna().unique()
startup_acq_df['term_code'] = startup_acq_df['term_code'].astype('category')
print("-" * 3)
startup_acq_df.info()

# employee_ed_df
print("-" * 3)
employee_ed_df = optimize_memory_usage(employee_ed_df, 'employee_ed_df')
missing_values_report(employee_ed_df)
# first_name и last_name содержат меньше процента пропусков - очистим их
try:
    employee_ed_df = employee_ed_df.dropna(subset=['first_name', 'last_name'])
    print("-" * 3)
    print("пропуски в 'first_name' и 'last_name' успешно удалены")
except Exception as e:
    print("-" * 3)
    print(f"ошибка при удалении пропусков {e}")

print("-" * 3)
employee_ed_df.info()

# employee_profiles_df
print("-" * 3)
employee_profiles_df = optimize_memory_usage(employee_profiles_df, 'employee_profiles_df')
missing_values_report(employee_profiles_df)

employee_profiles_df = employee_profiles_df.dropna(subset=['institution'])

print("-" * 3)
employee_profiles_df.info()

# employee_degrees_df
employee_degrees_df['object_id'] = employee_degrees_df['object_id'].str.replace('p:', '').astype('int64')
employee_degrees_df['degree_type'] = employee_degrees_df['degree_type'].astype('category')
print("-" * 3)
employee_degrees_df = optimize_memory_usage(employee_degrees_df, 'employee_degrees_df')
print("-" * 3)
employee_degrees_df.info()

# startup_fin_df

# учитывая особенности данных, сразу явным образом преобразуем столбцы в целочисленный тип, с учетом особенности сохранения NaN, оставшиеся необходимые обработает функция оптимизации
startup_fin_df[['milestones', 'participants']] = startup_fin_df[['milestones', 'participants']].astype('Int64')
startup_fin_df['status'] = startup_fin_df['status'].astype('category')
startup_fin_df['category_code'] = startup_fin_df['category_code'].astype('category')
startup_fin_df['country_code'] = startup_fin_df['country_code'].astype('category')
startup_fin_df['funding_round_type'] = startup_fin_df['funding_round_type'].astype('category')
print("-" * 3)
startup_fin_df = optimize_memory_usage(startup_fin_df, 'startup_fin_df')
print("-" * 3)
startup_fin_df.info()
# визуализируем пропуски с помощью библиотеки missingno, поймем структуру пропусков в startup_fin_df
# с помощью msno.matrix() увидем кластеры пропусков
# посмотрим на корреляцию между пропусками в разных столбцах, покажет образовались многие наши пропуски
msno.matrix(startup_fin_df)
show_plot_with_delay(delay=10, save_path='msno.matrix.png', dpi=300)
msno.bar(startup_fin_df)
show_plot_with_delay(delay=10, save_path='msno.bar.png', dpi=300)
msno.heatmap(startup_fin_df)
show_plot_with_delay(delay=10, save_path='msno.heatmap.png', dpi=300)
missing = (pd.DataFrame({'Кол-во пропусков': startup_fin_df.isnull().sum(),
                        'Доля пропусков': startup_fin_df.isnull().sum() / len(startup_fin_df)})
          .style.background_gradient(cmap='coolwarm'))
missing.to_csv('missing.csv', encoding='utf-8')

# 2. Выборочное исследование по запросу компании-заказчика
    # 2.1 Раунды финансирования по годам
# составим сводную таблицу по годам, рассмотрим наиболее информативные (более 50 раундов)
funding_rounds_by_year = (
    startup_fin_df
    .assign(год=startup_fin_df['funded_at'].dt.year) # извлекаем год из даты финансирования
    .groupby('год')
    .agg(
        типичные_значения=('raised_amount', 'median'),  # медиана
        количество_раундов=('funding_rounds', 'count')  # количество раундов
    )
    .query('количество_раундов > 50')
    .sort_index() # сортируем результат по годам
    ).reset_index()
funding_rounds_by_year = funding_rounds_by_year.rename(columns={ # уберем символы из названий столбцов
    'типичные_значения': 'типичные значения',
    'количество_раундов': 'количество раундов'
})
print(funding_rounds_by_year)

# дополним полученные данные скользящей средней за 3 года
funding_rounds_by_year['скользящая_средняя'] = funding_rounds_by_year['типичные значения'].rolling(window=3).mean() / 1000000
plt.figure(figsize=(11, 5))
plt.plot( # диаграмма динамики финансирования
    funding_rounds_by_year['год'], # ось X
    funding_rounds_by_year['типичные значения'] / 1000000, # ось Y: разделим на миллион, уйдя от коннотации
    marker='o',
    linestyle='-',
    color='gray',
    label='Исходные данные'
)
plt.plot( # скользящая средняя
    funding_rounds_by_year['год'],
    funding_rounds_by_year['скользящая_средняя'],
    linestyle='--',
    color='red',
    label='Скользящая средняя (3 года)'
)
plt.xlabel('Год', color='gray')
plt.ylabel('Объем финансирования (млн.)', color='gray')
plt.title('Динамика финансирования')
plt.grid(True)
plt.legend()
plt.xlim(1999,2013)
plt.xticks(funding_rounds_by_year['год'])
show_plot_with_delay(delay=10, save_path='funding_rounds_by_year.png', dpi=300)

    # 2.2 Люди и их образование
#  зависит ли полнота сведений о сотрудниках (образование) от размера компаний
employee_ed_df = employee_ed_df.copy()
employee_profiles_df = employee_profiles_df.copy()
people_unique_df = employee_ed_df.drop_duplicates(subset='id') # исключим дубли
employee_merged_df = pd.merge(people_unique_df, employee_profiles_df, left_on='id', right_on='person_id', how='left', suffixes=('_people', '_education')) # объединим датафреймы об образовании
# посмотрим на пропуски в объединенном датафрейме
missing_study = (pd.DataFrame({'Кол-во пропусков': employee_merged_df.isnull().sum(),
                        'Доля пропусков': employee_merged_df.isnull().sum() / len(employee_merged_df)})
          .style.background_gradient(cmap='coolwarm'))
missing_study.to_csv('missing_study.csv', encoding='utf-8')
# визуализация распределения
company_counts = employee_merged_df.groupby('company_id')['id_people'].nunique().reset_index() # анализ распределения компаний по количеству сотрудников
plt.figure(figsize=(11, 5))
sns.countplot(x = company_counts['id_people'],
              color = 'lightgray',
              edgecolor='gray',
              linewidth=0.5)
plt.title('Распределение компаний по количеству сотрудников')
plt.xlabel('Количество сотрудников', color = 'gray')
plt.ylabel('Количество компаний', color = 'gray')
show_plot_with_delay(delay=10, save_path='employee_merged_df.png', dpi=300)

# выведем статистику по категориям компаний, зададим интервалы в корзинах
bins = [0, 1, 2, 3, 5, 10, 25, float('inf')]
labels = ['1', '2', '3', '4-5', '6-10', '11-25', '26+']
company_counts['категория_компании'] = pd.cut( # категоризация компаний
    company_counts['id_people'], 
    bins=bins, # список границ интервалов
    labels=labels, # список меток для категорий
    include_lowest=True # включая крайнее значение слева в первый интервал
)
grouped = pd.merge( # объединяем данные - присоединяем категории к основному датафрейму
    company_counts[['company_id', 'категория_компании']], 
    employee_merged_df,
    on='company_id'
)
grouped_by_category = grouped.groupby('категория_компании', observed=False) # группируем по категории

results_list = []

for category, group in grouped_by_category:
    total = len(group)  # считаем общее количество записей
    missing = group['institution'].isnull().sum()  # количество пропусков в столбце 'institution'

    if total == 0: # рассчитываем процент пропусков с проверкой на пустую группу
        percentage = 0.0
    else:
        percentage = (missing / total) * 100

    results_list.append({ # добавляем результат в список как словарь
        'категория_компании': category,
        'общее_количество_сотрудников': total,
        'пропуски_образования': missing,
        'процент_пропусков': percentage
    })

statistics_by_company_category = pd.DataFrame(results_list) # преобразуем список словарей
print("-" * 3)
print(f"статистика по категориям компаний в зависимости от заданных интервалов\n{statistics_by_company_category}")
print("-" * 3)
print(f"общая статистика по категориям компаний:\nсреднее количество сотрудников в компании - {company_counts['id_people'].mean():.2f}\nмаксимальное количество сотрудников - {company_counts['id_people'].max()}")

# проверяем полезно ли будет присоединить данные по employee_degrees_df к уже подготовленному нами employee_merged_df
print("-" * 3)
employee_merged_df.info()
print("-" * 3)
employee_degrees_df.info()
employee_all = pd.merge(employee_merged_df, employee_degrees_df[['object_id', 'degree_type']], left_on='id_people', right_on='object_id', how='left')
employee_all = employee_all.drop(columns=['person_id', 'object_id'])
records = employee_all[ # проверяем условие когда degree_type заполнен, а institution и id_education пустые
    (employee_all['degree_type'].notna()) &  # degree_type заполнен
    (employee_all['institution'].isna()) &   # institution nan
    (employee_all['id_education'].isna())    # id_education nan
]
print("-" * 3)
print(f" {len(records)} строки исключительно в датасете 'employee_degrees.csv', где сотрудники имеют степень об образовании")
# присоединение employee_degrees_df существенно не улучшит полноту информации об образовании сотрудников
print("-" * 3)
if not records.empty:
    print(f"примеры строк\n{records[['degree_type', 'institution', 'id_education']].head()}")
else:
    print("примеров cтрок нет")

    # 2.3 Поиск столбцов для объединения датасетов
# нам известно, что столбец company_id подходит для объединения с другими датасетами - network_username, который встречается в нескольких датасетах потенциально также может быть нам полезен: проверим это
def check_columns(column_name, df_list, names):
    """
    проверяет наличие столбца column_name в датафреймах из списка df_list и выводит результат совпадений
    параметры:
    column_name - имя столбца;
    df_list - список датафреймов;
    names - их имена
    """
    for name, df in zip(names, df_list):
        if column_name in df.columns:
            print(f"столбец {column_name} найден в {name}")
print("-" * 3)
check_columns('network_username', dfs, name_dfs)
# столбец network_username найден в startup_fin_df и employee_ed_df, проверяем
company_usernames = startup_fin_df['network_username'].dropna().str.lower()
people_usernames = employee_ed_df['network_username'].dropna().str.lower()
common_usernames = set(company_usernames) & set(people_usernames)
unique_company = company_usernames.nunique()
unique_people = people_usernames.nunique()
common_count = len(common_usernames)
print("-" * 3)
print(f"{unique_company} - уникальных никнеймов в компаниях\n{unique_people} - уникальных никнеймов у людей\n{common_count} - совпадений никнеймов")
# видим, что использование столбца network_username для объединения данных не подойдет из-за риска получения некорректных результатов

    # 2.4 Проблемный датасет и причина возникновения пропусков
# во время собственного анализа данных у заказчика больше всего вопросов возникло к датасету startup_fin.csv - в нем много пропусков как раз в информации о раундах, которая заказчику важна
# предположим, что изначально данные хранились в двух отдельных датасетах - компаний и раундов финансирования и при объединении таблиц возникла проблема с дублированием строк для компаний с несколькими раундами
print("-" * 3)
startup_fin_df.info()
missing_startup_fin_df = (pd.DataFrame({'Кол-во пропусков': startup_fin_df.isnull().sum(),
                        'Доля пропусков': startup_fin_df.isnull().sum() / len(startup_fin_df)})
          .style.background_gradient(cmap='coolwarm'))
missing_startup_fin_df.to_csv('missing_startup_fin_df.csv', encoding='utf-8')
# создадим датафрейм компаний с полным набором столбцов (столбец closed_at пропустим)
company_df = startup_fin_df[['company_id', 'name', 'category_code', 'status', 'founded_at', 'domain', 'network_username', 'country_code', 'investment_rounds', 'funding_rounds', 'funding_total', 'milestones']]
rounds_df = startup_fin_df[['company_id_round', 'funding_round_id', 'funded_at', 'funding_round_type', 'raised_amount', 'pre_money_valuation', 'participants', 'is_first_round', 'is_last_round']]
rounds_df = rounds_df.rename(columns={'company_id_round': 'company_id'}).reset_index(drop=True)
company_df = company_df.reset_index(drop=True)
rounds_df = rounds_df.dropna()
rounds_df = rounds_df.drop_duplicates(keep='first')
company_df = company_df.drop_duplicates(keep='first')
# приведем статистику по созданным датафреймам
missing_company_df = (pd.DataFrame({'Кол-во пропусков': company_df.isnull().sum(),
                        'Доля пропусков': company_df.isnull().sum() / len(company_df)})
          .style.background_gradient(cmap='coolwarm'))
missing_company_df.to_csv('missing_company_df.csv', encoding='utf-8')
missing_rounds_df = (pd.DataFrame({'Кол-во пропусков': rounds_df.isnull().sum(),
                        'Доля пропусков': rounds_df.isnull().sum() / len(rounds_df)})
          .style.background_gradient(cmap='coolwarm'))
missing_rounds_df.to_csv('missing_rounds_df.csv', encoding='utf-8')
print("-" * 3)
rounds_df.info()
print("-" * 3)
company_df.info()

# 3. Исследовательский анализ данных объединенного датасета
    # 3.1 Объединение данных
# заказчика интересуют прежде всего те компании, которые меняли или готовы менять владельцев (gолучение инвестиций или финансирования, по мнению заказчика - интерес к покупке или продаже компании)
condition = ((company_df['funding_rounds'] > 0) | (company_df['investment_rounds'] > 0) | (company_df['status'] == 'acquired'))
filtered_company_df = company_df[condition]
print("-" * 3)
filtered_company_df.info()
    # 3.2 Анализ выбросов
# заказчика интересует обычный для рассматриваемого периода размер средств, который предоставлялся компаниям
print("-" * 3)
filtered_company_df['funding_total'].describe()
# гистограмма - распределение финансирования
plt.figure(figsize=(11, 5))
sns.histplot(filtered_company_df['funding_total'],
             color = 'lightgray',
             edgecolor='gray',
             linewidth=0.5,
             log_scale=True) # ось X имеет логарифмический масштаб
plt.title('Распределение финансирования')
plt.xlabel('Размер финансирования', color='gray')
plt.ylabel('Количество компаний', color='gray')
show_plot_with_delay(delay=10, save_path='histplot_filtered_company_df_funding_total.png', dpi=300)
# ящик - разброс финансирования
plt.figure(figsize=(11, 5))
sns.boxplot(x=filtered_company_df['funding_total'], color='lightgray')
plt.xscale('log') # логарифмический масштаб для оси X (сжимая большие значения)
plt.title('Разброс финансирования')
plt.xlabel('Размер финансирования', color='gray')
show_plot_with_delay(delay=10, save_path='boxplot_filtered_company_df_funding_total.png', dpi=300)
# ящик - - разброс финансирования (исключим выбросы)
plt.figure(figsize=(11, 5))
sns.boxplot(x=filtered_company_df['funding_total'], 
            color='lightgray',
            showfliers=False) # отключаем выбросы
plt.xscale('log') # логарифмический масштаб для оси X (сжимая большие значения)
plt.title('Разброс финансирования (без выбросов)')
plt.xlabel('Размер финансирования', color='gray')
show_plot_with_delay(delay=10, save_path='boxplot_showfliers_False_filtered_company_df_funding_total.png', dpi=300)

    # 3.3 Исследуем компании, проданные за символическую стоимость
filtered_company_df = filtered_company_df.copy()
startup_acq_df = startup_acq_df.copy()
# присоединяем по 'acquired_company_id'
acquired_df = pd.merge(filtered_company_df, startup_acq_df, left_on='company_id', right_on='acquired_company_id', how='inner')
print("-" * 3)
print(f"{acquired_df['company_id'].duplicated().sum()} - дубликата ID компаний, причина возникновения дубликатов компаний по company_id из-за продажи по частям (видно по разным датам покупки одной и той же компании)")
print("-" * 3)
acquired_df.info()
# ищем компании по условию - нулевые или минимальные сделки при ненулевом финансировании
zero_deal_companies = acquired_df[(acquired_df['price_amount'] <= 1) & (acquired_df['funding_total'] > 0)]
print("-" * 3)
print(f"{len(zero_deal_companies)} компаний, отвечают заданному условию")
# границы выбросов сумм финансирования
    # расчет квартилей и IQR
Q1 = zero_deal_companies['funding_total'].quantile(0.25)
Q3 = zero_deal_companies['funding_total'].quantile(0.75)
IQR = Q3 - Q1
k = 1.5 # коэффициент для определения границ
lower_bound = max(0, Q1 - k * IQR) # нижнюю границу выставим с учетом неотрицательности
upper_bound = Q3 + k * IQR
print("-" * 3)
print(f"(границы выбросов сумм финансирования\nнижняя граница - {lower_bound}\nверхняя граница - {upper_bound}\n25‑й процентиль - {Q1}\n75‑й процентиль - {Q3}")
# границы выбросов сумм финансирования без нулевых значений
non_zero_funding = zero_deal_companies[zero_deal_companies['funding_total'] > 0]['funding_total']
Q21 = non_zero_funding.quantile(0.25)
Q23 = non_zero_funding.quantile(0.75)
IQR = Q23 - Q21
k = 1.5
lower_bound_non_zero = max(0, Q21 - k * IQR) 
upper_bound_non_zero = Q23 + k * IQR
print("-" * 3)
print(f"(границы выбросов сумм финансирования\nнижняя граница - {lower_bound_non_zero}\nверхняя граница - {upper_bound_non_zero}\n25‑й процентиль - {Q21}\n75‑й процентиль - {Q23}")
# количество выбросов
print("-" * 3)
zero_deal_companies['funding_total'].describe()
outliers = zero_deal_companies[(zero_deal_companies['funding_total'] < lower_bound) | (zero_deal_companies['funding_total'] > upper_bound)]
print("-" * 3)
print(f"{len(outliers)} - выбросов")

    # 3.4 Цены стартапов по категориям
# категории стартапов с типично высокими ценами покупки стартапов и значительным разбросом цен могут быть привлекательными для крупных инвесторов (выделим категории стартапов с типично высокими ценами и с наибольшим разбросом цен за стартап)
    # расчет по сумме сделки price_amount
# присоединим датафрейм с нужным нам полем 'price amount', чтобы обладать большим объемом данных для анализа
filtered_company_df = filtered_company_df.copy()
startup_acq_df = startup_acq_df.copy()
acquired_df_left = pd.merge(filtered_company_df, startup_acq_df, left_on='company_id', right_on='acquired_company_id', how='left')
# обозначим границы для удаления выбросов в 'price_amount', чтобы не получать сумасшедших цифр при подсчете std
Q1 = acquired_df_left['price_amount'].quantile(0.25)
Q3 = acquired_df_left['price_amount'].quantile(0.75)
IQR = Q3 - Q1
k = 1.5  # коэффициент для определения выбросов
# значения вне этих пределов считаем выбросами
lower_bound = Q1 - k * IQR
upper_bound = Q3 + k * IQR
# оставляем только строки в пределах границ
acquired_df_left = acquired_df_left[(acquired_df_left['price_amount'] >= lower_bound) & (acquired_df_left['price_amount'] <= upper_bound)]
category_stats = acquired_df_left.groupby(
    'category_code', 
    observed=True # учитываем только наблюдаемые категории (без пропусков) 
    ).agg({'price_amount': ['mean', 'std', 'count']})
category_stats = category_stats[category_stats[('price_amount', 'count')] > 0] # удаляем категории с нулевым количеством наблюдений
top_categories = category_stats.sort_values( 
    by=('price_amount', 'mean'), # сортируем по среднему значению
    ascending=False
)
# отбираем валидные категории
valid_categories_price = top_categories[
    (top_categories[('price_amount', 'count')] > 10) & # условие - количество наблюдений > 10
    (top_categories[('price_amount', 'mean')] > np.percentile( # условие - среднее значение 'price_amount' выше 90‑го перцентиля среди всех средних
        top_categories[('price_amount', 'mean')].dropna(), 90 # исключаем NaN значения средних / вычисляем 90‑й перцентиль
    )) &
    (top_categories[('price_amount', 'std')] > np.percentile( # исключаем NaN значения std / вычисляем 90‑й перцентиль
        top_categories[('price_amount', 'std')].dropna(), 90))]
print("-" * 3)
print(valid_categories_price)
    # расчет по суммам финансирования funding_total
# аналогичным образом выполним подход и для 'funding_total'
Q1 = filtered_company_df['funding_total'].quantile(0.25)
Q3 = filtered_company_df['funding_total'].quantile(0.75)
IQR = Q3 - Q1
k = 1.5  # коэффициент для определения выбросов
lower_bound = Q1 - k * IQR
upper_bound = Q3 + k * IQR
filtered_company_df = filtered_company_df[(filtered_company_df['funding_total'] >= lower_bound) & (filtered_company_df['funding_total'] <= upper_bound)]
category_stats = filtered_company_df.groupby('category_code', observed=True).agg({'funding_total': ['mean', 'std', 'count']})
top_categories = category_stats.sort_values(by=('funding_total', 'mean'), ascending=False)
# фильтруем категории с достаточным количеством наблюдений
# количество наблюдений > 10 (чтобы исключить категории с малым количеством данных)
# среднее финансирование выше 90-го процентиля (отбираем только категории с действительно высокими средними вложениями)
# стандартное отклонение выше 90-го процентиля (учитываем только категории с большим разбросом цен)
valid_categories_total = top_categories[
    (top_categories[('funding_total', 'count')] > 10) &
    (top_categories[('funding_total', 'mean')] > np.percentile(
        top_categories[('funding_total', 'mean')], 90
    )) &
    (top_categories[('funding_total', 'std')] > np.percentile(
        top_categories[('funding_total', 'std')], 90))]
print("-" * 3)
print(valid_categories_total)

    # 3.5 Количество раундов стартапа перед покупкой
# заказчика интересует типичное значение количества раундов funding_rounds для каждого возможного статуса стартапа
avg_rounds = (filtered_company_df.groupby('status', observed=False)['funding_rounds'].mean().round(2).reset_index())
avg_rounds['status'] = avg_rounds['status'].cat.rename_categories({
    'acquired': 'Приобретен',
    'closed': 'Закрыт',
    'ipo': 'Вышел на IPO',
    'operating': 'Действует'
})
print("-" * 3)
print(avg_rounds)

sns.barplot(
    data=avg_rounds,
    x='status',
    y='funding_rounds',
    hue='funding_rounds',  # окрашиваем по признаку
    palette='coolwarm',    
    dodge=False, # столбцы не смещаются в группы
    width=0.75,
    legend=False)  # отключим легенду
plt.title('Раунды финансирования по статусам')
plt.xlabel('Статус стартапа', color='gray') 
plt.ylabel('Количество раундов финансирования', color='gray')
show_plot_with_delay(delay=10, save_path='barplot_avg_rounds.png', dpi=300)
# 4. Итоговый вывод, рекомендации и подробное описание в тетрадке jupyter