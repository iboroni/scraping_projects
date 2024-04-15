import tabula
import os
import pandas as pd
from datetime import datetime


# the goal is to read a pdf and generate a csv with a dataset which can be eaisly manipulated 
# (e.g. easily sum all values in a column)
# it's done using the concept of Object Oriented Programming 

class PdfToCsvPipeline:
    # the following variables are Class Variables
    # they are the same for all instances, unless it's modified in the process
    pipeline_type = None
    num_of_pipes = 0

    def __init__(self, input_file):
        self.input_file=input_file
        self.input_path=None
        self.output_path=None
        self.dataframes=None
        self.set_input_path()
        self.set_output_path()
        PdfToCsvPipeline.num_of_pipes += 1 # increments the numbe rof pipes every time an instance is created

    @classmethod
    def set_pipeline_type(cls, type):
        cls.pipeline_type = type

    @staticmethod
    def is_weekend(day):
        if day.weekday() in [5,6]:
            return True 
        return False

    def extract(self):
        self.dataframes = tabula.read_pdf(self.input_path, pages="all", stream=True) 
        # to get a specific page just set the number of the page in the argument pages
        # read_pdf returns list of DataFrames

    def set_input_path(self):
        self.input_path = os.path.join('pdf_files', self.input_file)

    def set_output_path(self):
        self.output_path = os.path.join('csv_files', (self.input_file).replace('.pdf', '.csv'))
    
    def transform(self):
        csv_path = self.output_path.replace('.csv', '_before.csv')
        df_concat = pd.concat(self.dataframes)
        # first csv generated before transforming and cleaning data
        df_concat.to_csv(csv_path, sep=';', index=True, encoding='utf-8')
        first_df = self.dataframes[0]
        select_columns = list(first_df)[:4]
        final_df = df_concat[select_columns]
        new_column_names =  ['data', 'descricao', 'valor', 'saldo']
        final_df.columns = new_column_names
        final_df = final_df.reset_index().astype(str).replace('nan', '')
        final_df = final_df.groupby(final_df.index//3).agg(lambda x: ' '.join(x))
        final_df.to_csv(csv_path.replace('before', 'after'), sep=';', index=True, encoding='utf-8')

#print("Num of pipes before creating any instances: ", PdfToCsvPipeline.num_of_pipes) 
PdfToCsvPipeline.set_pipeline_type('pdf2csv') # setter - setting the class variable using a class method
pipeline = PdfToCsvPipeline('fatura_picpay_fevereiro_2024.pdf')
#print(pipeline.__dict__) # print a dic with the pipeline object attributes
pipeline.extract()
pipeline.transform()

# print("Num of pipes created: ", pipeline.num_of_pipes) 
# print(pipeline.is_weekend(datetime.now()))

# references
# tabula: https://nbviewer.org/github/chezou/tabula-py/blob/master/examples/tabula_example.ipynb

