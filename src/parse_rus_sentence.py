import pymorphy2
import pandas as pd
import re

morph = pymorphy2.MorphAnalyzer()
sentence = "В FutureForceWarrior (FFW) (США) ведутся работы по использованию сочетания следующих технологий в управлении компьютером пехотинца: Широкоугольный прозрачный дисплей — защитное стекло шлема Распознавание речевых команд Система слежения за зрачком с системо (...)"


def get_gender(row):
    if row['pos'] != 'NOUN':
        g = row['gov_gender']
    else:
        g = row['gender']
    if (row['case_group'] > 1) | ((row['case_group'] == 1) & (row['pos'] != 'NOUN')):
        c = 'gent'
    else:
        c = 'nomn'
    try:
        w = row['morphy'].inflect({g, c, 'sing'}).word
    except:
        try:
            w = row['morphy'].inflect({g, c, 'plur'}).word
        except:
            w = row['morphy'].normal_form
    return w


result = '{""}'
try:
    rows = []
    depid = 1
    tokens = re.split('([^0-9a-zA-Zа-яА-я]+)', sentence)
    for token in tokens:
        if (len(token) > 1) | (token in (',', '"')):
            p = morph.parse(token)[0]
            pos = p.tag.POS
            if pos is None:
                pos = p.tag
            gender = p.tag.gender
            pos = '{}'.format(pos)
            rows.append({'depid': depid, 'morphy': p, 'pos': pos, 'gender': gender})
            depid += 1
    df1 = pd.DataFrame.from_dict(rows)
    df2 = df1[(df1['pos'].isin(['LATN', 'NOUN', 'ADJF', 'PRTF', 'NUMR']))].reset_index(drop=True)
    df2['diff'] = df2['depid'].diff().fillna(0)
    df2['group'] = (df2['diff'] != 1).cumsum()
    # gov_gender in each group is needed for setting the right gender for all elements of the ngram
    df2['gov_gender'] = df2.groupby(['group'])['gender'].transform(lambda x: x.tail(1))
    # gov_pos in each group is needed for filter ngrams that do not end up with a noun
    df2['gov_pos'] = df2.groupby(['group'])['pos'].transform(lambda x: x.tail(1))
    # filtering of groups that do not end up with a noun
    df3 = df2.loc[(df2['gov_pos'].isin(['NOUN', 'LATN']))].reset_index()
    # case is set based on the order of nouns in the group
    df3['is_noun'] = (df3['pos'] == 'NOUN')
    df3['case_group'] = df3.groupby(['group'])['is_noun'].cumsum()
    # word is lemmatized version of token based on the group gender and appropriate case
    df3['word'] = df3[['morphy', 'gov_gender', 'case_group', 'gender', 'pos']].apply(get_gender, axis=1)
    df4 = df3.groupby(['group'])['word'].apply(lambda x: ' '.join(x)).to_frame().reset_index(
        drop=True)
    result = ('{"' + '", "'.join(df4['word'].astype('unicode')) + '"}')
except:
    print('error')
finally:
    # return result
    print(result)
