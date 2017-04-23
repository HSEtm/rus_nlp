import pandas as pd
from os.path import join, exists
from os import makedirs
import pymorphy2
import nltk

# import json
# nltk.data.load('tokenizers/punkt')

morph = pymorphy2.MorphAnalyzer()


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


def process_survey(documents):
    rows = []
    docid = 1
    for text in documents:
        sentences = nltk.sent_tokenize(text)
        sentid = 1
        for sentence in sentences:
            depid = 1
            tokens = nltk.word_tokenize(sentence)
            for token in tokens:
                p = morph.parse(token)[0]
                pos = p.tag.POS
                gender = p.tag.gender
                rows.append(
                    {'docid': docid, 'sentid': sentid, 'depid': depid, 'morphy': p, 'pos': pos, 'gender': gender})
                depid += 1
            sentid += 1
        docid += 1
    df1 = pd.DataFrame.from_dict(rows)
    df2 = df1[(df1['pos'].isin(['NOUN', 'ADJF', 'PRTF', 'NUMR']))].reset_index(drop=True)
    df2['diff'] = df2.groupby(['docid', 'sentid'])['depid'].diff().fillna(0)
    df2['group'] = (df2['diff'] != 1).cumsum()
    # gov_gender in each group is needed for setting the right gender for all elements of the ngram
    df2['gov_gender'] = df2.groupby(['docid', 'sentid', 'group'])['gender'].transform(lambda x: x.tail(1))
    # gov_pos in each group is needed for filter ngrams that do not end up with a noun
    df2['gov_pos'] = df2.groupby(['docid', 'sentid', 'group'])['pos'].transform(lambda x: x.tail(1))
    # filtering of groups that do not end up with a noun
    df3 = df2.loc[(df2['gov_pos'] == 'NOUN')].reset_index()
    # case is set based on the order of nouns in the group
    df3['is_noun'] = (df3['pos'] == 'NOUN')
    df3['case_group'] = df3.groupby(['docid', 'sentid', 'group'])['is_noun'].cumsum()
    # word is lemmatized version of token based on the group gender and appropriate case
    df3['word'] = df3[['morphy', 'gov_gender', 'case_group', 'gender', 'pos']].apply(get_gender, axis=1)
    df4 = df3.groupby(['docid', 'sentid', 'group'])['word'].apply(lambda x: ' '.join(x)).to_frame().reset_index(
        drop=True)
    df5 = df4.groupby('word').size().to_frame(name='frequency').reset_index()
    return df5


filename = join('data', 'survey.xlsx')
questions = ['registration_date', 'birthday_date', 'city', 'employment', 'occupation', 'scientific_degree',
             'satisfaction_level', 'career_expectations', 'key_competences', 'leadership_training', 'leadership_kpi',
             'research_area', 'key_challenges', 'barriers']
sheet_names = ['Лидеры фундаментальной науки', 'Лидеры технологических разработ', 'Лидеры инновационных проектов']
data = dict()
output_folder = 'output'
writer_challenges = pd.ExcelWriter(path=join(output_folder, 'challenges_text_mining.xlsx'), engine='openpyxl')
writer_barriers = pd.ExcelWriter(path=join(output_folder, 'barriers_text_mining.xlsx'), engine='openpyxl')

for sheet_name in sheet_names:
    data[sheet_name] = pd.read_excel(io=filename, sheetname=sheet_name, header=0, names=questions)
    process_survey(data[sheet_name]['key_challenges'][~data[sheet_name]['key_challenges'].isnull()]).sort_values(
        'frequency', ascending=False).to_excel(excel_writer=writer_challenges,
                                               sheet_name=sheet_name, startrow=0)
    process_survey(data[sheet_name]['barriers'][~data[sheet_name]['barriers'].isnull()]).sort_values(
        'frequency', ascending=False).to_excel(excel_writer=writer_barriers,
                                               sheet_name=sheet_name, startrow=0)
if not exists(output_folder):
    makedirs(output_folder)
writer_challenges.save()
writer_barriers.save()