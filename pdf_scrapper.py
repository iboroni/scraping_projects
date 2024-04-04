import tabula
import os
import pandas as pd
import math
from datetime import datetime


# the goal is to read a pdf and generate a csv with a dataset which can be eaisly manipulated 
# (e.g. easily sum all values in a column)
# it's done using the concept of Object Oriented Programming 

class PdfToCsvPipeline:
    # the following variables are Class Variables
    # they are the same for all instances, unless it's modified in the process
    pipeline_type = None
    num_of_pipes = 0

    def __init__(self, data_path):
        self.data_path=data_path
        self.outcome_path=None
        self.dataframes=None

        PdfToCsvPipeline.num_of_pipes += 1 # increments the numbe rof pipes every time an instance is created

    @classmethod
    def set_pipeline_type(cls, type):
        cls.pipeline_type = type

    @staticmethod
    def is_weekend(day):
        if day.weekday() == 5 or day.weekday() == 6:
            return True 
        return False

    def extract(self):
        self.dataframes = tabula.read_pdf(pdf_path, pages="all", stream=True) # to get a specific page just set the number of the page in the argument pages
        # read_pdf returns list of DataFrames

    def transform(self):
        first_df = self.dataframes[0]
        print("Columns: ", list(first_df))
        select_columns = list(first_df)[:4]
        print("Selected Columns: ", select_columns)
        df_concat = pd.concat(self.dataframes)
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


        # print(df_select_columns)
        # csv_path = os.path.join('csv_files','fatura_picpay_fevereiro_2024.csv')
        # later: don't specify the name of the file, but use the same name as read in line 7

        # all_tables.to_csv(csv_path, sep=';', index=True, encoding='utf-8')

        # problema, se lermos linha por linha, nao funcionara em arquivos com muitas linhas"

print("Num of pipes before creating any instances: ", PdfToCsvPipeline.num_of_pipes) 
pdf_path = os.path.join('pdf_files','fatura_picpay_fevereiro_2024.pdf')
pipeline = PdfToCsvPipeline(pdf_path)
print(pipeline.__dict__) # print a dic with the pipeline object attributes
pipeline.extract()
pipeline.transform()
PdfToCsvPipeline.set_pipeline_type('pdf2csv') # setter - setting the class variable using a class method

print("Num of pipes in the instance created: ", pipeline.num_of_pipes) 
print(pipeline.is_weekend(datetime.now()))

# later: don't specify the name of the file, but read every file of the folder

# references
# tabula: https://nbviewer.org/github/chezou/tabula-py/blob/master/examples/tabula_example.ipynb

