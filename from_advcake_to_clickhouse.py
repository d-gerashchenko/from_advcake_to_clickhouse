import pandas as pd
import requests
from datetime import datetime
from datetime import date
from datetime import timedelta
from pandahouse import to_clickhouse, read_clickhouse

# Проекты для скачивания (логин: ['проект', 'магазин'])
projects = {'login1' : ['project1', 'store_name1'],
            'login2' : ['project2', 'store_name2'],
            'login3' : ['project3', 'store_name3']}

api_key = 'api_key'
date_from = str(date.today() - timedelta(days=180))

db = 'advcake'                             # База данных
table = 'commission'                       # Таблица
update_field = 'last_updated_at'           # Поле с датой для обновление
unique_fields = ['order_id', 'created_at'] # Уникальные поля
index_field = 'project'                    # Поле для индекса во время загрузки в clickhouse

# Данные для подключения к Clickhouse
connection = dict(host = 'host:8123',
                  user = 'default',
                  password = 'password',
                  database = db)

def import_data(projects):
    """
    Выгружаем данные из advcake. 
    Нужно учитывать, что есть предельное кол-во строк в выдаче. 
    Поэтому, если общее кол-во записей больше лимита - то делаем несколько запросов со сдвигом.
    Дополнительно добавляем столбцы с названиями проекта и магазина.
    Пустые выдачи пропускаем.

    :param projects: список проектов для выгрузки
    
    :return: датафрейм с данными по заказам со всех проектов
    """
    rows_limit = 5000 #Предельное кол-во строк в выдаче
    result = pd.DataFrame()
    for project in projects:
        df = pd.DataFrame()
        i = 0
        json_len = rows_limit
        while json_len == rows_limit:
            url = 'https://api.advcake.com/orders/advertiser/{0}?pass={1}&date_from={2}&offset={3}'\
                  .format(project, api_key, date_from, i)
            r = requests.get(url)        
            json = r.json()
            json_len = json['total']
            print(project, ': ', i, '-', json_len, r)
            df = df.append(pd.DataFrame(json['data']), ignore_index = True)
            i += rows_limit
        if len(df) == 0: continue
        df['project'] = projects[project][0]
        df['store_name'] = projects[project][1]
        result = result.append(df, ignore_index = True)
        
    return result

def replace_columns_type(df):
    """
    Выгружаем столбцы из df и меняем тип на подходящий для Clickhouse. 
    Для этого используем словарь для замены типа данных из Python на подходящий в Clickhouse
    
    :param df: датафрейм для экспорта в Clickhouse
    
    :return: словарь с названиями полей и их типами в Clickhouse
    """
    dtype_replace = {'object':'String',
                     'datetime64[ns]':'Date',
                     'int64':'UInt32',
                     'float64':'Decimal(9,2)'}
    fields = {}
    for each in df.columns:
        value = str(df[each].dtype)
        for key in dtype_replace:
            value = value.replace(key, dtype_replace[key])
        fields[each] = value
        
    return fields

def create_table(fields):
    """
    Проверяем на наличие готовой таблицы и создаем новую при необходимости.
    Сперва формируем запрос - для этого берем поля из fields, а также уникальные поля и поле с датой обновления
    
    :param fields: Преобразованные под Clickhouse поля из загружаемого датафрейма
    """
    unique_fields_str = ', '.join(map(str, unique_fields))
    string = ''
    for i, field in enumerate(fields):
        string += '`{}` {}'.format(field, fields[field])
        if i < len(fields) - 1: string += ', '            
    query = 'CREATE TABLE IF NOT EXISTS {0}.{1} ({2}) ENGINE = ReplacingMergeTree({3}, ({4}), 8192)'\
            .format(db, table, string, update_field, unique_fields_str)

    try: read_clickhouse(query, connection = connection)
    except KeyError: print('Проверяем наличие таблицы - ОК')


def export_to_table(df):
    """
    Загружаем данные в Clickhouse
    
    :param df: Датафрейм для загрузки
    """
    to_clickhouse(df.set_index(index_field), table=table, connection=connection)  


# Выгружаем данные из advcake
df = import_data(projects)

#Убираем столбец с DRR
df = df.drop(['drr'], axis=1)

# Оставляем только новые и одобренные заказы
df = df[df['status'].isin(['Новый', 'Одобрен'])]

# Преобразуем str(datetime) в date и добавляем дату последнего обновления данных
df[update_field] = str(datetime.today())
for x in ['created_at','updated_at', update_field]:
    df[x] = (pd.to_datetime(df[x], format='%Y-%m-%d %H:%M:%S').dt.date).astype('datetime64[ns]')

#Финальная комиссия - добавляем комиссии advcake (10% + НДС) и фин. перевод (10% + НДС, за искл. xxx)
advcake_commission = 1.12
extra_commission = 1.12

df['final_commission'] = df['commission'] * advcake_commission

#Добавляем доп. комиссию для всех проектов кроме xxx
df['final_commission'].loc[df['project'] != 'xxx'] = df['final_commission'] * extra_commission

# Округляем итогую комиссию
df['final_commission'] = round(df['final_commission'], 2)

# Меняем порядок столбцов на более удобный
df = df.reindex(columns=['project','store_name','order_id','created_at','updated_at','status',
                         'partner','webmaster','price','commission','final_commission','last_updated_at'])

# Подготавливаем поля для clickhouse
fields_clickhouse = replace_columns_type(df)

# Создаем таблицу в Clickhouse (если нужно) и загружаем данные
create_table(fields_clickhouse)
export_to_table(df)
print('Загрузка завершена')