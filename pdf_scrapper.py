import tabula
import os
import pandas as pd
# check my tabula-py environment
# tabula.environment_info() 
pdf_path = os.path.join('pdf_files','fatura_picpay_fevereiro_2024.pdf')


dfs = tabula.read_pdf(pdf_path, pages="all", stream=True) # to get a specific page just set the number of the page in the argument pages
# read_pdf returns list of DataFrames
cont_tb = 1
print("Columns: ", list(dfs[0]))
all_tables = pd.concat(dfs)
print(all_tables)
csv_path = os.path.join('output_files','fatura_picpay_fevereiro_2024.csv')

all_tables.to_csv(csv_path, sep=';', index=True, encoding='utf-8')

### OBSERVACOES ###

## OBS 1
# 3 linhas do dataframe equivale a 1 linha da tabela original
# todo: forma de mergear as 3 linhas
# neste caso, NaN pode ser ignorado

# https://nbviewer.org/github/chezou/tabula-py/blob/master/examples/tabula_example.ipynb