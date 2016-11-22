import pandas as pd
from os.path import join
import pymorphy2
import nltk

morph = pymorphy2.MorphAnalyzer()
filename = join('data', 'survey.xlsx')
questions = ['registration_date', 'birthday_date', 'city', 'employment', 'occupation', 'scientific_degree',
             'satisfaction_level', 'career_expectations', 'key_competences', 'leadership_training', 'leadership_kpi',
             'research_area', 'key_challenges', 'barriers']
sheet_names = ['Лидеры фундаментальной науки', 'Лидеры технологических разработ', 'Лидеры инновационных проектов']
data = dict()
challenges = dict()
barriers = dict()
tokens_challenges = dict()
tokens_barriers = dict()
df_challenges = dict()
df_barriers = dict()

# nltk.data.load('tokenizers/punkt')

for sheet_name in sheet_names:
    data[sheet_name] = pd.read_excel(io=filename, sheetname=sheet_name,
                                     header=0, names=questions)
    challenges[sheet_name] = data[sheet_name]['key_challenges'][~data[sheet_name]['key_challenges'].isnull()]
    tokens_challenges[sheet_name] = []
    docid = 1
    for challenge in challenges[sheet_name]:
        sentences = nltk.sent_tokenize(challenge)
        sentid = 1
        for sentence in sentences:
            tokenid = 1
            tokens = nltk.word_tokenize(sentence)
            tmp = dict()
            for token in tokens:
                tmp['docid'] = docid
                tmp['sentid'] = sentid
                tmp['tokenid'] = tokenid
                tmp['token'] = token
                p = morph.parse(token)[0]
                tmp['lemma'] = p.normal_form
                tmp['pos'] = p.tag.POS
                tokens_challenges[sheet_name].append(tmp.copy())
                tokenid += 1
            sentid += 1
        docid += 1
    df_challenges[sheet_name] = pd.DataFrame.from_dict(tokens_challenges[sheet_name])

    barriers[sheet_name] = data[sheet_name]['barriers'][~data[sheet_name]['barriers'].isnull()]
    tokens_barriers[sheet_name] = []
    docid = 1
    for barrier in barriers[sheet_name]:
        sentences = nltk.sent_tokenize(barrier)
        sentid = 1
        for sentence in sentences:
            tokenid = 1
            tokens = nltk.word_tokenize(sentence)
            tmp = dict()
            for token in tokens:
                tmp['docid'] = docid
                tmp['sentid'] = sentid
                tmp['tokenid'] = tokenid
                tmp['token'] = token
                p = morph.parse(token)[0]
                tmp['lemma'] = p.normal_form
                tmp['pos'] = p.tag.POS
                tokens_barriers[sheet_name].append(tmp.copy())
                tokenid += 1
            sentid += 1
        docid += 1
    df_barriers[sheet_name] = pd.DataFrame.from_dict(tokens_barriers[sheet_name])
