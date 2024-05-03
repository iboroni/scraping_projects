import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import tabula
import os
import pandas as pd

# description: this class is an object that reads from folder pdf_files and after transforming lands de result in csv_files

class Pdf2CsvPipe:
    # the following variables are Class Variables
    # they are the same for all instances, unless it's modified in the process
    pipeline_type = 'pdf2csv'
    num_of_pipes = 0

    def __init__(self, source):
        self.input_files=[]
        self.output_files=[]
        self.dataframes=[]
        self.source=source
        self.set_input_files()
        Pdf2CsvPipe.num_of_pipes += 1 # increments the number of pipes every time an instance is created

    @classmethod
    def set_pipeline_type(cls, type):
        cls.pipeline_type = type

    def set_input_files(self):
        # using list comprehension to add a prefix of the pipeline source in the file name
        files = [f for f in os.listdir('pdf_files') if f not in ['.gitignore', '.DS_Store']]
        self.input_files = files
        print(f"Read {len(files)} files.")

    def add_output_file(self, output_file):
        self.output_files.append(output_file)
        
    def get_output_name(self, input_file):
        prefix = self.source + '_'
        return os.path.join('csv_files', (prefix + input_file).replace('.pdf', '.csv'))

    def extract(self, input_file):
        file_path = os.path.join('pdf_files', input_file)
        file_dfs = tabula.read_pdf(file_path, pages="all", stream=True)
        # to get a specific page just set the number of the page in the argument pages
        # read_pdf returns list of DataFrames so we'll concatenate all in one single df for this file
        file_dfs_concat = pd.concat(file_dfs)
        return {'file': input_file, 'dataframe': file_dfs_concat}
         
    def transform(self, df_dict):
        df = df_dict['dataframe']
        output_name = self.get_output_name(df_dict['file']) 
        df.to_csv(output_name, sep=';', index=True, encoding='utf-8')
        self.add_output_file(output_name)
        print("### Finsihed transforming dataframe into csv | output file: ", output_name)
    
    def run(self):
        for input_file in self.input_files:
            print(f"Processing file {input_file}...")
            df_dict = self.extract(input_file) # here we're transforming the current pdf file into a pandas dataframe
            self.transform(df_dict) # here we're transforming the dataframe into a csv

    def print_input_files(self):
        print(self.input_files)

# references
# tabula: https://nbviewer.org/github/chezou/tabula-py/blob/master/examples/tabula_example.ipynb

# troubleshooting steps
# - identify error -> Error from tabula-java: Error: Error: End-of-File, expected line
# - easier faster thing to do eg. updating version of java
# - checking if trouble is with inheritance or the method that uses tabula
# - found error in method set_input_files it was reading ds store which is not a pdf and throw an error