import os
import sys
activate_this = os.path.join('/var/lib/postgresql/ve/rus_nlp', 'bin', 'activate_this.py')
exec(open(activate_this).read(), dict(__file__=activate_this))
import pymorphy2
import pandas as pd
import treetaggerwrapper
from natasha import Combinator, DEFAULT_GRAMMARS
morph = pymorphy2.MorphAnalyzer()
tagger = treetaggerwrapper.TreeTagger(TAGLANG='ru',
                                      TAGDIR='/var/lib/postgresql/opt/treetagger',
                                      TAGPARFILE='/var/lib/postgresql/opt/lib/russian.par')
def get_ngram(x):
    ngram = x['token']
    try:
        best = False
        morphs = morph.parse(x['token'])
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
# rus_text = """«Группа ГАЗ» представила в рамках форума «Здоровье нации — основа процветания России» мобильный медицинский центр, созданный на базе микроавтобуса «ГАЗель NEXT». Автомобиль предназначен для проведения выездных медицинских осмотров, диагностики, диспансеризации и оказания срочной медицинской помощи. Мобильный центр состоит из двух независимых друг от друга модулей, которые могут быть оснащены необходимым набором медицинского оборудования и системами жизнеобеспечения. Фактически это два полноценных медицинских кабинета, где врачи разного профиля могут вести полноценную работу в комфортных условиях.
# В базовом исполнении центр представляет собой два медицинских кабинета площадью до 8 кв. м, каждый из которых включает в себя кушетку для осмотра пациентов, стол для врача, а также набор шкафов и тумб с креплениями для различного оборудования. Кабинеты имеют отдельные входы и оборудованы изолированными санузлами.
# В модулях, расположенных в микроавтобусе и прицепе, могут быть созданы кабинеты функциональной диагностики, стоматологии, офтальмологии, кардиологии, детского и женского здоровья, передвижной флюорографический и рентгенологический кабинеты, лаборатории различного назначения, передвижной донорский пункт, кабинет урологии и многое другое.
# Один из возможных примеров использования автомобиля — флюорографический комплекс. В этом случае в одном модуле может быть размещен кабинет флюорографии (цифровой флюорограф, рентгензащита отсека, бактерицидный облучатель воздуха), а в другом — кабинет рентгенолога (автоматизированное рабочее место врача с набором необходимого оборудования, компьютером и принтером). ООО "Ромашка" продала порцию стажеров Ильи Кузьминова группе компаний ООО вектор."""
try:
    combinator = Combinator(DEFAULT_GRAMMARS)
    token_ners = []
    token_ners_id = 0
    group_ner_id = 0
    for grammar, tokens in combinator.resolve_matches(combinator.extract(rus_text), strict=True):
        for tok in tokens:
            token_ners.append({'ner_id': group_ner_id,
                               'begin': tok.position[0],
                               'end': tok.position[1],
                               'token': tok.value,
                               'ner': str(grammar)})
        group_ner_id += 1
    df_ners = pd.DataFrame.from_dict(data=token_ners)
    df_ners.sort_values(['begin', 'end'], ascending=[True, True], inplace=True)
    df_ners.reset_index(drop=True, inplace=True)
    tags = tagger.tag_text(rus_text)
    gender_id = {'N': 2, 'A': 3}
    number_id = {'N': 3, 'A': 4}
    case_id = {'N': 4, 'A': 5}
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
            ner = ''
            ner_id = None
            if token_ners_id < len(df_ners):
                if df_ners['token'][token_ners_id] == t[0]:
                    ner = df_ners['ner'][token_ners_id]
                    ner_id = df_ners['ner_id'][token_ners_id]
                    token_ners_id += 1
            rows.append({'sentid': sentid, 'depid': depid, 'token': t[0], 'lemma': t[2],
                         'pos': 'Foreign' if t[1] == '-' and len(t[1]) != len(t[0]) else t[1][0],
                         'gender': t[1][gender_id[t[1][0]]] if len(t[1]) > 3 and t[1][0] in (['N', 'A']) else '-',
                         'number': t[1][number_id[t[1][0]]] if len(t[1]) > 4 and t[1][0] in (['N', 'A']) else '-',
                         'case': t[1][case_id[t[1][0]]] if len(t[1]) > 5 and t[1][0] in (['N', 'A']) else '-',
                         'ner': ner,
                         'ner_id': ner_id})
            depid += 1
            old = prev.copy()
            prev = t.copy()
        except:
            print('error')
    df1 = pd.DataFrame.from_dict(rows)
    df2_ners = df1[pd.notnull(df1['ner_id'])].reset_index(drop=True)
    df2_ners['is_noun'] = df2_ners['pos'].apply(lambda x: True if x in ('N', 'M') else False)
    df2_ners['case_group'] = df2_ners.groupby(['sentid', 'ner_id'])['is_noun'].cumsum()
    df2_ners['case_final'] = df2_ners.apply(
        lambda x: 'g' if ((x['case_group'] > 1.0) | ((x['case_group'] == 1.0) & (x['pos'] not in ['N', 'M']))) & (
            x['case'] == 'g') else 'n', axis=1)
    df2_ners['case_final'] = df2_ners['case_final'].map({'n': 'nomn', 'g': 'gent'})
    df2_ners['pos'] = df2_ners['pos'].map({'N': 'NOUN', 'A': 'ADJF'})
    df2_ners['gender'] = df2_ners['gender'].map({'n': 'neut', 'f': 'femn', 'm': 'masc'})
    df2_ners['number'] = df2_ners['number'].map({'s': 'sing', 'p': 'plur'})
    df2_ners.fillna('', inplace=True)
    df2_ners['inflect'] = df2_ners[['pos', 'case_final', 'gender', 'number']].apply(
        lambda x: {x['pos'], x['case_final'], x['gender'], x['number']},
        axis=1)
    df2_ners['inflect'] = df2_ners['inflect'].apply(lambda x: {y for y in x if y})
    df2_ners['ner'] = df2_ners[['lemma', 'token', 'inflect', 'pos']].apply(get_ngram, axis=1)
    df3_ners = df2_ners.groupby(['sentid', 'ner_id']).agg(
        {'ner': lambda x: ' '.join(x), 'depid': lambda y: tuple(y.tolist())}).reset_index()
    result = df3_ners[['sentid', 'ner', 'depid']]
    result.columns = ['sentid', 'ner', 'depids']
    return result.to_dict(orient='records')
except:
    return ''