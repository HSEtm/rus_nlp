import pandas as pd
import numpy as np
from os import path
from psycopg2 import connect


def insert_query(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print(e)
        connection.rollback()
    finally:
        cursor.close()


def build_array_based_query(source_df, target_table, list_of_target_columns, id_column, additional_condition,
                            query_type):
    '''
    Use this function to create plain SQL string to insert or select, where conditions(columns)are formed as single
    string of arrays and then passed to query string where those strings are unneseted. Such method shows performance
    second to using postgres /copy function which is considered as the fastest way to do bulk inserts.
    :param source_df: DataFrame to use as source for operations.
    :param target_table: Target table in database (table to select from or insert into).
    :param list_of_target_columns: List of columns used in select or insert. All columns should be present in source_df.
    :param id_column: Column with id of value or combination of values. Necessary only for select, pass None if insert.
    :param additional_condition: Plain SQL additional condition string append to join condition. None if doen't nedded.
    :param query_type: Either 'select' or 'upsert'.
    :return: Returns query string
    '''
    if list_of_target_columns:
        conditions_df = source_df[list_of_target_columns].drop_duplicates()
    else:  # if no list is provided - use all columns
        conditions_df = source_df.drop_duplicates()
        list_of_target_columns = conditions_df.columns.tolist()
    cte = []
    join_conditions = []
    # loop over columns to cast specific arrays
    # may need improvements in future in case of new column types (i.e. text arrays) within main schema
    for select_condition_column in list_of_target_columns:
        # if (df[select_condition_column].dtype == tuple):
        #     cte.append('unnest(array[$tt${0}$tt$])::int[] as {1}'.format(
        #         '$tt$,$tt$'.join(
        #             ['{' + ','.join(["%.f" % x for x in row[1][select_condition_column]]) + '}' for row in
        #              conditions_df.iterrows()]),
        #         select_condition_column))
        if (df[select_condition_column].dtype == int):
            cte.append('unnest(array[$tt${0}$tt$])::int as {1}'.format(
                '$tt$,$tt$'.join(["%.f" % x for x in conditions_df[select_condition_column].astype(int)]),
                select_condition_column))
        else:
            cte.append('unnest(array[$tt${0}$tt$])::text as {1}'.format(
                '$tt$,$tt$'.join(conditions_df[select_condition_column].astype(str)),
                select_condition_column))
    if query_type == 'select':
        if additional_condition is None:  # cast to empty string
            additional_condition = ''
        query = ('with cte as (select {0})\n'
                 'select {1} from {2} a inner join cte on {3};').format(', '.join(cte),
                                                                        ', '.join(['a.' + x for x in [
                                                                            id_column] + list_of_target_columns]),
                                                                        target_table,
                                                                        ' and '.join(
                                                                            join_conditions) + additional_condition)
    elif query_type == 'upsert':
        # result depends on existance of 'id_column'
        # if id_column is None is considered that query is used for final insert (to the *.deps table)
        if id_column:
            query = ('with cte as (select {0})\n'
                     'insert into {1}({2})\n'
                     '(select NEXTVAL(\'{3}\'), {4} from  cte )\n'
                     'on conflict do nothing;').format(', '.join(cte),
                                                       target_table,
                                                       ', '.join([id_column] + list_of_target_columns),
                                                       id_column + '_seq',
                                                       ', '.join(list_of_target_columns))
        else:
            query = ('with cte as (select {0})\n'
                     'insert into {1}({2})\n'
                     '(select {2} from  cte )\n'
                     'on conflict do nothing;').format(', '.join(cte),
                                                       target_table,
                                                       ', '.join(list_of_target_columns),
                                                       )
    elif query_type == 'create_insert':
        query = ('with cte as (select {0})\n'
                 'select {2} into {1} from  cte').format(', '.join(cte),
                                                         target_table,
                                                         ', '.join(list_of_target_columns),
                                                         )
    return query


df = pd.read_csv(filepath_or_buffer=path.abspath('/home/pavel/data/wos_translation/wos_ru2.csv'), sep='	',
                 names=['ngram_eng', 'ngram_ru', 'dictionary', 'block_id', 'block_name',
                        'translation_type', 'recommendation'],
                 dtype={'ngram_eng': 'str', 'ngram_ru': 'str', 'dictionary': 'str',
                        'block_id': 'int', 'block_name': 'str',
                        'translation_type': 'str', 'recommendation': 'str'})

df_mini = df[df['block_name'] == df['ngram_eng']]

# for column in df.columns.tolist():
#     if (column == 'block_id'):
#         df[column].astype(int)
#     else:
#         df[column].astype(str)

# df.to_sql(con=con, name='ngrams_ru', if_exists='replace')

CHUNK_SIZE = 10000

conn_string = "host='192.168.2.26' port='5432' dbname='wos_foresight2040'" \
              "user='ruser' password='blackshipsatethesky'"
conn = connect(conn_string)

main_insert_query = build_array_based_query(source_df=df_mini[:CHUNK_SIZE],
                                            target_table='ngrams_ru',
                                            list_of_target_columns=None,
                                            additional_condition=None,
                                            id_column=None,
                                            query_type='upsert')

insert_query(connection=conn, query=main_insert_query)

for iterat in range(2, len(df_mini) // CHUNK_SIZE + 1):
    main_insert_query = build_array_based_query(source_df=df_mini[(iterat - 1) * CHUNK_SIZE:iterat * CHUNK_SIZE],
                                                target_table='ngrams_ru',
                                                list_of_target_columns=None,
                                                additional_condition=None,
                                                id_column=None,
                                                query_type='upsert')
    insert_query(connection=conn, query=main_insert_query)
    print(iterat)

main_insert_query = build_array_based_query(
    source_df=df_mini[(len(df_mini) // CHUNK_SIZE) * CHUNK_SIZE:len(df_mini) + 1],
    target_table='ngrams_ru',
    list_of_target_columns=None,
    additional_condition=None,
    id_column=None,
    query_type='upsert')
insert_query(connection=conn, query=main_insert_query)

conn.close()
