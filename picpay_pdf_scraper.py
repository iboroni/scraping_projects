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

    def __init__(self, source):
        self.input_files=[]
        self.output_files=[]
        self.dataframes=[]
        self.source=source
        self.set_input_files()
        self.set_output_files()
        PdfToCsvPipeline.num_of_pipes += 1 # increments the numbe rof pipes every time an instance is created

    @classmethod
    def set_pipeline_type(cls, type):
        cls.pipeline_type = type

    @staticmethod
    def is_weekend(day):
        if day.weekday() in [5,6]:
            return True 
        return False

    def set_input_files(self):
        # using list comprehension to add a prefix of the pipeline source in the file name
        files = [f for f in os.listdir('pdf_files') if f != '.gitignore']
        self.input_files = files
        print(f"Read {len(files)} files.")

    def set_output_files(self):
        prefix = self.source + '_'
        self.output_files = [os.path.join('csv_files', (prefix + f).replace('.pdf', '.csv')) for f in self.input_files ]
        
    def extract(self, input_file):
        file_path = os.path.join('pdf_files', input_file)
        file_dfs = tabula.read_pdf(file_path, pages="all", stream=True)
        # to get a specific page just set the number of the page in the argument pages
        # read_pdf returns list of DataFrames so we'll concatenate all in one single df for this file
        print('row 50 ', len(file_dfs))
        file_dfs_concat = pd.concat(file_dfs)
        print('row 52 ', len(file_dfs))
        return {'file': input_file, 'dataframe': file_dfs_concat}
        
        
    def transform(self):
        csv_path = self.output_path.replace('.csv', '_before.csv')
        # first csv generated before transforming and cleaning data
        df_concat.to_csv(csv_path, sep=';', index=True, encoding='utf-8')
        first_df = self.dataframes[0]
        select_columns = list(first_df)[:4]
        final_df = df_concat[select_columns]
        new_column_names =  ['data', 'descricao', 'valor', 'saldo']
        final_df.columns = new_column_names
        final_df = final_df.reset_index().astype(str).replace('nan', '')

        #group and join every 3 rows
        final_df = final_df.groupby(final_df.index//3).agg(lambda x: ' '.join(x))
        
        # last csv generated after transforming and cleaning data
        final_df.to_csv(csv_path.replace('before', 'after'), sep=';', index=True, encoding='utf-8')
    
    def run(self):
        for input_file in self.input_files:
            print(f"Processing file {input_file}...")
            self.extract(input_file)
            self.transform(input_file)

#print("Num of pipes before creating any instances: ", PdfToCsvPipeline.num_of_pipes) 
PdfToCsvPipeline.set_pipeline_type('pdf2csv') # setter - setting the class variable using a class method
picpay_pdf_pipe = PdfToCsvPipeline('picpay')
picpay_pdf_pipe.run()
print(picpay_pdf_pipe.dataframes)
# picpay_pdf_pipe.transform()


# references
# tabula: https://nbviewer.org/github/chezou/tabula-py/blob/master/examples/tabula_example.ipynb

