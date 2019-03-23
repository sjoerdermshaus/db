import pandas as pd
import numpy as np
from numpy.random import choice
from numpy.random import seed
import string
from sqlalchemy import Integer, Float, String

from utils.custom_logger import CustomLogger


class RandomDataGenerator(object):

    def __init__(self,
                 number_of_rows,
                 number_of_float_columns,
                 number_of_integer_columns,
                 number_of_string_columns,
                 csv,
                 to_excel,
                 logger):
        self.number_of_rows = number_of_rows
        self.number_of_float_columns = number_of_float_columns
        self.number_of_integer_columns = number_of_integer_columns
        self.number_of_string_columns = number_of_string_columns
        self.csv = csv
        self.to_excel = to_excel

        self.seed = seed(1234)
        self.column_names = []
        self.dtype = dict()
        self.df = pd.DataFrame()
        self.logger = logger

    def create_column_names(self):
        self.column_names.append(['DB_ID', 'int'])
        self.dtype = {'DB_ID': Integer()}
        for m in range(1, self.number_of_float_columns + 1):
            self.column_names.append(['float{:d}'.format(m), 'float'])
            self.dtype[f'float{m}'] = Float()

        for m in range(1, self.number_of_integer_columns + 1):
            self.column_names.append(['integer{:d}'.format(m), 'integer'])
            self.dtype[f'integer{m}'] = Integer()

        for m in range(1, self.number_of_string_columns + 1):
            self.column_names.append(['string{:d}'.format(m), 'nvarchar(max)'])
            self.dtype[f'string{m}'] = String(255)

    @staticmethod
    def create_random_string(size=10, number_of_rows=10):
        full_string = ''.join(choice(list(string.ascii_lowercase), size=size * number_of_rows, replace=True))
        return [full_string[i:i + size] for i in range(0, len(full_string), size)]

    def create_random_data(self):
        self.logger.info('Creating random data')

        self.logger.info('Creating index data')
        data_index = np.arange(1, self.number_of_rows + 1, dtype=np.int32).reshape((self.number_of_rows, 1))
        columns_index = ['DB_ID']
        df_index = pd.DataFrame(data=data_index, columns=columns_index)

        self.logger.info('Creating float data')
        data_float = np.random.rand(self.number_of_rows, self.number_of_float_columns)
        columns_float = [col[0] for col in self.column_names if col[1] == 'float']
        df_float = pd.DataFrame(data=data_float, columns=columns_float)

        self.logger.info('Creating integer data')
        data_integer = np.random.randint(low=0,
                                         high=100,
                                         size=(self.number_of_rows, self.number_of_integer_columns),
                                         dtype=np.int64)
        columns_integer = [col[0] for col in self.column_names if col[1] == 'integer']
        df_integer = pd.DataFrame(data=data_integer, columns=columns_integer)

        self.logger.info('Creating string data')
        columns_string = [col[0] for col in self.column_names if col[1] == 'nvarchar(max)']
        df_string = pd.DataFrame(columns=columns_string)
        for col in columns_string:
            df_string[col] = self.create_random_string(number_of_rows=self.number_of_rows)

        self.logger.info('Concatenating DataFrames')
        frames = [df_index, df_float, df_integer, df_string]
        self.df = pd.concat(frames, axis=1)
        self.df.set_index(columns_index, inplace=True)

        self.logger.info('Creating random data finished')

    def export_random_data(self):
        self.logger.info('Exporting random data')
        self.df.to_csv(self.csv, sep=',', index=True)
        if self.to_excel:
            self.df.to_excel(f"{self.csv.split('.')[0]}.xlsx", index=True)
        self.logger.info('Exporting random data finished')

    def run(self):
        self.create_column_names()
        self.create_random_data()
        self.export_random_data()


def main():
    kwargs = {'number_of_rows': 2000,
              'number_of_float_columns': 130,
              'number_of_string_columns': 20,
              'csv': 'random_data_2K.csv',
              'to_excel': False,
              'logger': CustomLogger('random_data_generator.yaml').logger}
    RandomDataGenerator(**kwargs).run()


if __name__ == '__main__':
    main()
