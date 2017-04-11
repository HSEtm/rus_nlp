import pymorphy2
import pandas as pd
import treetaggerwrapper
morph = pymorphy2.MorphAnalyzer()
tagger = treetaggerwrapper.TreeTagger(TAGLANG='ru',
                                      TAGDIR='D:\\_TMS\\_external\\TreeTagger',
                                      TAGPARFILE='D:\\_TMS\\_external\\TreeTagger\\lib\\russian.par')
def get_ngram(x):
    ngram = x['lemma']
    try:
        best = False
        morphs = morph.parse(x['lemma'])
        if len(morphs) > 0:
            for m in morphs:
                if m.tag.POS == x['pos']:
                    if not best:
                        if 'plur' in x['inflect']:
                            if 'masc' in x['inflect']:
                                x['inflect'].remove('masc')
                            elif 'femn' in x['inflect']:
                                x['inflect'].remove('femn')
                            elif 'neut' in x['inflect']:
                                x['inflect'].remove('neut')
                        if m.inflect(x['inflect']) is not None:
                            ngram = m.inflect(x['inflect']).word
                            best = True
    except:
        print('error')
    return ngram
# rus_text = 'Масштабное тактико специальное учение. Я против масштабного тактико специального учения. Защита западного военного округа. Я голосую за защиту западного военного округа. Развивая качающимися листьями, дерево было наклонено. Красно-белый большой раровский кот Чубайс hero - черный пес любви людей на 5 стр. ест красную сосиску в настоящее время! Кот остался доволен США. Правда его масса в 40 кг. немного волновала хозяйку.'
try:
    tags = tagger.tag_text(rus_text)
    gender_id = {'N': 2, 'A': 3}
    number_id = {'N': 3, 'A': 4}
    case_id = {'N': 4, 'A': 5}
    # Category=Noun, Type = common, Gender = masculine, Number = singular, Case = accusative, Animate = no
    result = pd.DataFrame(columns=['docid', 'word'])
    rows = list()
    sentid = 1
    depid = 1
    old = []
    prev = []
    for t in [tag.split('\t') for tag in tags]:
        try:
            if len(old) > 0 and len(prev) > 0:
                if prev[1] == 'SENT' and len(old[2]) - len(old[0]) < 4 and t[0][0].upper() == t[0][0]:
                    sentid += 1
                    depid = 1
            rows.append({'sentid': sentid, 'depid': depid, 'token': t[0], 'lemma': t[2],
                         'pos': 'Foreign' if t[1] == '-' and len(t[1]) != len(t[0]) else t[1][0],
                         'gender': t[1][gender_id[t[1][0]]] if len(t[1]) > 3 and t[1][0] in (['N', 'A']) else '-',
                         'number': t[1][number_id[t[1][0]]] if len(t[1]) > 4 and t[1][0] in (['N', 'A']) else '-',
                         'case': t[1][case_id[t[1][0]]] if len(t[1]) > 5 and t[1][0] in (['N', 'A']) else '-'})
            depid += 1
            old = prev.copy()
            prev = t.copy()
        except:
            print('error')
    df1 = pd.DataFrame.from_dict(rows)
    df2 = df1[(df1['pos'].isin(['Foreign', 'N', 'A', 'M']))].reset_index(drop=True)
    df2['diff'] = df2.groupby(['sentid'])['depid'].diff().fillna(0)
    df2['group'] = (df2['diff'] != 1).cumsum()
    # gov_pos in each group is needed for filter ngrams that do not end up with a noun
    df2['gov_pos'] = df2.groupby(['sentid', 'group'])['pos'].transform(lambda x: x.tail(1))
    # filtering of groups that do not end up with a noun
    df3 = df2.loc[(df2['gov_pos'].isin(['N', 'Foreign']))].reset_index(drop=True)
    # case is set based on the order of nouns in the group
    df3['is_noun'] = (df3['pos'] == 'N')
    df3['case_group'] = df3.groupby(['sentid', 'group'])['is_noun'].cumsum()
    df3['case_final'] = df3.apply(
        lambda x: 'g' if ((x['case_group'] > 1.0) | ((x['case_group'] == 1.0) & (x['case'] != 'N'))) & (
            x['case'] == 'g') else 'n', axis=1)
    # ngram is lemmatized version of token based on the group gender and appropriate case
    df3['case_final'] = df3['case_final'].map({'n': 'nomn', 'g': 'gent'})
    df3['pos'] = df3['pos'].map({'N': 'NOUN', 'A': 'ADJF'})
    df3['gender'] = df3['gender'].map({'n': 'neut', 'f': 'femn', 'm': 'masc'})
    df3['number'] = df3['number'].map({'s': 'sing', 'p': 'plur'})
    df3.fillna('', inplace=True)
    df3['inflect'] = df3[['pos', 'case_final', 'gender', 'number']].apply(
        lambda x: {x['pos'], x['case_final'], x['gender'], x['number']},
        axis=1)
    df3['inflect'] = df3['inflect'].apply(lambda x: {y for y in x if y})
    df3['ngram'] = df3[['lemma', 'inflect', 'pos']].apply(get_ngram, axis=1)
    df4 = df3.groupby(['sentid', 'group'])['ngram'].apply(lambda x: ' '.join(x)).reset_index()
    # df4 = df3.groupby(['sentid', 'group'])['lemma'].apply(lambda x: ' '.join(x)).reset_index()
    # df4['ngram'] = df4['lemma']
    result = df4[['sentid', 'ngram']]
except:
    print('error')
finally:
    return result.to_dict(orient='records')
    # print(result.to_dict(orient='records'))