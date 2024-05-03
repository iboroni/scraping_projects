from picpay_pipe import PicPayPipe
from pdf2csv import Pdf2CsvPipe

class Main:
    def __init__(self):
        self.pdf_pipe = Pdf2CsvPipe('picpay_raw')
        self.picpay_pipe = PicPayPipe()

    def run(self):
        self.transform_picpay_data_with_super_class()
        self.transform_picpay_data_with_sub_class()

    def transform_picpay_data_with_super_class(self):
        try:
            self.pdf_pipe.run()
        except ValueError as e:
            print(f"Error occurred while transforming PDF to CSV: {e}")

    def transform_picpay_data_with_sub_class(self):
        try:
            self.picpay_pipe.run()
        except ValueError as e:
            print(f"Error occurred while transforming PicPay data: {e}")

if __name__ == "__main__":
    main_instance = Main()
    main_instance.run()
