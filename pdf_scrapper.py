import tabula
import os
import pandas as pd
import math

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
new_column_names =  ['data', 'descricao', 'valor', 'saldo']
df_select_columns.columns = new_column_names
head_num = 3
result_0 = []
selected_df = df_select_columns.head(head_num)

# setting new variables for the new column values for the specific row
new_data, new_descricao, new_valor, new_saldo = '', '', '', ''
for row in range(3):
    for column in new_column_names:
        value = str(selected_df[column][row])
        # ignoring nan's and concatenating possible values in other rows
        if value != 'nan':
            if column == 'data':
                new_data += value + ' '
            elif column == 'descricao':
                new_descricao += value + ' '
            elif column == 'valor':
                new_valor += value + ' '
            elif column == 'saldo':
                new_saldo += value + ' '
new_data = new_data.strip()
new_descricao = new_descricao.strip()
new_valor = new_valor.replace('R$ ' , '').strip()
new_saldo = new_saldo.replace('R$ ' , '').strip()

new_row = (new_data, new_descricao, new_valor, new_saldo)
print(new_row)
# agora só falta fazer isso pro dataframe inteiro e voila!!

# for row in df_select_columns.head(head_num).iterrows():
#     for item in row
#     print(row[1])
#     #result_0.append(row})


#print(df_select_columns)
# csv_path = os.path.join('csv_files','fatura_picpay_fevereiro_2024.csv')
# later: don't specify the name of the file, but use the same name as read in line 7

# all_tables.to_csv(csv_path, sep=';', index=True, encoding='utf-8')

# problema, se lermos linha por linha, nao funcionara em arquivos com muitas linhas"

# https://nbviewer.org/github/chezou/tabula-py/blob/master/examples/tabula_example.ipynb