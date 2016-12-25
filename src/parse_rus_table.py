import pymorphy2
import pandas as pd
import re
import ast

morph = pymorphy2.MorphAnalyzer()
sents = list()
sents.append('{"docid": 1, "sent": "военный округ"}')
sents.append('{"docid": 2, "sent": "золотая осень"}')
sents.append('{"docid": 3, "sent": "военный округ"}')
sents.append('{"docid": 4, "sent": "золотая осень"}')
sents.append('{"docid": 5, "sent": "военный округ"}')
sents.append('{"docid": 6, "sent": "золотая осень"}')
sents.append('{"docid": 7, "sent": "военный округ"}')
sents.append('{"docid": 8, "sent": "золотая осень"}')
sents.append('{"docid": 9, "sent": "военный округ"}')
sents.append('{"docid": 10, "sent": "золотая осень"}')
sents.append('{"docid": 11, "sent": "военный округ"}')
sents.append('{"docid": 12, "sent": "золотая осень"}')
sents.append('{"docid": 12, "sent": "большой куб"}')
sents.append('{"docid": 12, "sent": "военный округ"}')
sents.append('')


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


result = pd.DataFrame(columns=['docid', 'word'])
try:
    rows = list()
    for sent in sents:
        try:
            s = ast.literal_eval(sent)
            docid = s['docid']
            sentence = s['sent']
            depid = 1
            tokens = re.split('([^0-9a-zA-Zа-яА-я]+)', sentence)
            for token in tokens:
                try:
                    if (len(token) > 1) | (token in (',', '"')):
                        p = morph.parse(token)[0]
                        pos = p.tag.POS
                        if pos is None:
                            pos = p.tag
                        gender = p.tag.gender
                        pos = '{}'.format(pos)
                        rows.append({'docid': docid, 'depid': depid, 'morphy': p, 'pos': pos, 'gender': gender})
                        depid += 1
                except:
                    print('error')
        except:
            print('error')
    df1 = pd.DataFrame.from_dict(rows)
    df2 = df1[(df1['pos'].isin(['LATN', 'NOUN', 'ADJF', 'PRTF', 'NUMR']))].reset_index(drop=True)
    df2['diff'] = df2.groupby(['docid'])['depid'].diff().fillna(0)
    df2['group'] = (df2['diff'] != 1).cumsum()
    # gov_gender in each group is needed for setting the right gender for all elements of the ngram
    df2['gov_gender'] = df2.groupby(['docid', 'group'])['gender'].transform(lambda x: x.tail(1))
    # gov_pos in each group is needed for filter ngrams that do not end up with a noun
    df2['gov_pos'] = df2.groupby(['docid', 'group'])['pos'].transform(lambda x: x.tail(1))
    # filtering of groups that do not end up with a noun
    df3 = df2.loc[(df2['gov_pos'].isin(['NOUN', 'LATN']))].reset_index()
    # case is set based on the order of nouns in the group
    df3['is_noun'] = (df3['pos'] == 'NOUN')
    df3['case_group'] = df3.groupby(['docid', 'group'])['is_noun'].cumsum()
    # word is lemmatized version of token based on the group gender and appropriate case
    df3['word'] = df3[['docid', 'morphy', 'gov_gender', 'case_group', 'gender', 'pos']].apply(get_gender, axis=1)
    df4 = df3.groupby(['docid', 'group'])['word'].apply(lambda x: ' '.join(x)).reset_index()
    result = df4[['docid', 'word']]
except:
    print('error')
finally:
    # return result.to_dict(orient='records')
    print(result.to_dict(orient='records'))
