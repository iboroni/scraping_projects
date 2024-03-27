import tabula
import os
import pandas as pd
# check my tabula-py environment
# tabula.environment_info() 
pdf_path = os.path.join('pdf_files','fatura_picpay_fevereiro_2024.pdf')
# later: don't specify the name of the file, but read every file of the folder

dfs = tabula.read_pdf(pdf_path, pages="all", stream=True) # to get a specific page just set the number of the page in the argument pages
# read_pdf returns list of DataFrames

print("Columns: ", list(dfs[0]))
select_columns = list(dfs[0])[:4]
print("Selected Columns: ", select_columns)
df_concat = pd.concat(dfs)
df_select_columns = df_concat[select_columns] # later: select directly the first 4 columns instead of doing line 13
df_select_columns.columns = ['data', 'descricao', 'valor', 'saldo']
head_num = 3
result_0 = []
for index, row in df_select_columns.head(head_num).items():
    result_0.append({index: row})

print(result_0)
#print(df_select_columns)
# csv_path = os.path.join('csv_files','fatura_picpay_fevereiro_2024.csv')
# later: don't specify the name of the file, but use the same name as read in line 7

# all_tables.to_csv(csv_path, sep=';', index=True, encoding='utf-8')

### OBSERVACOES ###

## OBS 1
# 3 linhas do dataframe equivale a 1 linha da tabela original
# todo: forma de mergear as 3 linhas
# neste caso, NaN pode ser ignorado

# problema, se lermos linha por linha, nao funcionara em arquivos com muitas linhas"

# https://nbviewer.org/github/chezou/tabula-py/blob/master/examples/tabula_example.ipynb