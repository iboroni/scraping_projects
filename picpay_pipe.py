import pandas as pd
from datetime import datetime
from pdf2csv  import Pdf2CsvPipe

# the goal is to read a pdf and generate a csv with a dataset which can be eaisly manipulated 
# (e.g. easily sum all values in a column)
# it's done using the concept of Object Oriented Programming 

class PicPayPipe(Pdf2CsvPipe):
    def __init__(self):
        super().__init__('picpay')
     
    def transform(self, df_dict):
        # this function is overwriting from Super because it requires a different transformation 
        # picpay report for some reason when extracted returns 1 row divided into 3 rows
        df = df_dict['dataframe']
        output_name = self.get_output_name(df_dict['file']) 
        select_columns = list(df)[:4]
        final_df = df[select_columns]
        new_column_names =  ['data', 'descricao', 'valor', 'saldo']
        final_df.columns = new_column_names
        final_df = final_df.reset_index().astype(str).replace('nan', '')

        # group and join every 3 rows
        final_df = final_df.groupby(final_df.index//3).agg(lambda x: ' '.join(x))
        final_df.to_csv(output_name, sep=';', index=True, encoding='utf-8')
        self.add_output_file(output_name)
        print("### Finished transforming dataframe into csv | output file: ", output_name)
