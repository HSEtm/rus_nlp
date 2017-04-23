import pandas as pd

train_data = []
train_file = 'data\\ru_syntagrus-ud-train.conllu'
# train_file = 'data\\ru_syntagrus-ud-dev.conllu'

train_file_output = 'output\\ru_syntagrus-ud-train.conll'

# train_file_output = 'output\\ru_syntagrus-ud-dev.conll'
with open(encoding='utf-8', file=train_file) as f:
    for line in f:
        if len(line) > 1:
            if line[0] != '#':
                train_data.append(line.split(sep='\t'))

train_df = pd.DataFrame.from_dict(train_data)
train_df[4] = train_df[3]
train_df[3] = '_'
train_df[8] = '_'
train_df[9] = '_'

train_df[6] = '_'
train_df[7] = '_'
# train_df.drop(train_df.columns[[4, 8, 9]], axis=1, inplace=True)
train_df[~train_df[0].str.contains('\.')].to_csv(encoding='utf-8', path_or_buf=train_file_output, sep='\t',
                                                 header=False, index=False)
# java -jar maltparser-1.9.0.jar -c russian -i "D:\_TMS\_PyCharm\rus_nlp\output\ru_syntagrus-ud-train.conll" -m learn
# java -jar maltparser-1.9.0.jar -c russian -i ru_syntagrus-ud-train.conll -m learn
# java -jar maltparser-1.9.0.jar -c russian -i ru_syntagrus-ud-dev.conll -o out.conll -m parse
# java -jar maltparser-1.9.0.jar -c russian -i test.conll -o out.conll -m parse
