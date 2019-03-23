from datetime import datetime
import logging

import numpy as np
import pandas as pd
from numpy.random import seed
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.types import Integer, Float, String

from utils.custom_logger import CustomLogger
from configparser import ConfigParser

from utils.profiler import profiler, runtime

from random_data_generator.core import RandomDataGenerator


class DatabaseConnector(object):

    def __init__(self, logger, config_file):
        # parser
        self.config_file = config_file
        self.config = ConfigParser(defaults=None)
        self.config.read(self.config_file)

        # [database]
        self.db = self.config['database']['db']
        self.dbname = self.config['database']['dbname']
        self.schema = self.config['database']['schema']
        self.driver = self.config['database']['driver']
        self.dsn = self.config['database']['dsn']
        self.table = self.config['database']['table']
        self.fast_executemany = self.config['database']['fast_executemany']
        self.table_full = '[{:s}].[{:s}].[{:s}]'.format(self.dbname, self.schema, self.table)

        # [run]
        self.generate_random_data = self.config['run'].getboolean('generate_random_data')
        self.number_of_rows = int(self.config['run'].getfloat('number_of_rows'))
        self.number_of_float_columns = self.config['run'].getint('number_of_float_columns')
        self.number_of_integer_columns = self.config['run'].getint('number_of_integer_columns')
        self.number_of_string_columns = self.config['run'].getint('number_of_string_columns')
        self.chunksize = self.config['run'].getint('chunksize')
        self.csv = self.config['run']['csv']

        # other
        self.logger = logger
        self.engine = None
        self.conn = None
        self.seed = seed(1234)
        self.dtype = dict()

    def open_db(self):
        # create an engine and a connection
        self.logger.info('Establishing connection')
        engine_string = '{:s}+{:s}://{:s}'.format(self.db,
                                                  self.driver,
                                                  self.dsn)
        self.engine = create_engine(engine_string, fast_executemany=self.fast_executemany)

        # if SET_FAST_EXECUTEMANY_SWITCH:
        #    @event.listens_for(self.engine, 'before_cursor_execute')
        #    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        #        if executemany:
        #            cursor.fast_executemany = True

        self.conn = self.engine.connect()
        self.logger.info('Establishing connection finished')

    def _generate_random_data(self):
        kwargs = {'number_of_rows': self.number_of_rows,
                  'number_of_float_columns': self.number_of_float_columns,
                  'number_of_integer_columns': self.number_of_integer_columns,
                  'number_of_string_columns': self.number_of_string_columns,
                  'csv': self.csv,
                  'to_excel': False,
                  'logger': logging.getLogger('random_data_generator')}
        RandomDataGenerator(**kwargs).run()

    def _load_random_data(self):
        self.logger.info(f'Loading random data from {self.csv}')
        self.df = pd.read_csv(self.csv, sep=',')
        self.df.set_index(['DB_ID'], inplace=True)
        self.logger.info(f'Loading random data from {self.csv} finished')

    def _insert_data(self):
        self.logger.info('Inserting data')

        start_time = datetime.now()
        try:
            for i, df_chunk in enumerate(pd.read_csv(self.csv, sep=',', chunksize=self.chunksize)):
                df_chunk.set_index(['DB_ID'], inplace=True)
                start_time_batch = datetime.now()
                if_exists = 'replace' if i == 0 else 'append'
                df_chunk.to_sql(name=self.table,
                                con=self.engine,
                                schema=self.schema,
                                if_exists=if_exists,
                                index=True,
                                dtype=self.dtype)
                format_string = '    Chunk {:7d} of size {:7d}: {:s}'
                self.logger.info(format_string.format(i + 1,
                                                      self.chunksize,
                                                      str(datetime.now() - start_time_batch).split('.')[0]
                                                      )
                                 )
        except IntegrityError:
            self.logger.error('Primary key constraint')
        run_time = datetime.now() - start_time
        self.logger.info('Total runtime: {:s}'.format(str(run_time).split('.')[0]))

        self.logger.info('Inserting data finished')
        return run_time

    def get_max_db_id(self):
        query = 'SELECT MAX(DB_ID) AS MAX_DB_ID FROM {:s}'.format(self.table_full)
        df = pd.read_sql(query, con=self.engine, index_col=None)
        max_db_id = 0 if df.loc[0, 'MAX_DB_ID'] is None else df.iloc[0, 0]
        return max_db_id

    def get_top_data(self, top=10):
        query = f'SELECT TOP {top} * FROM {self.table_full} ORDER BY DB_ID'
        return pd.read_sql(query, con=self.engine, index_col=['DB_ID'])

    def close_db(self):
        self.logger.info('Closing connection')
        self.conn.close()
        self.logger.info('Closing connection finished')

    def _set_dtype(self):
        self.logger.info('Determining dtype')
        df = pd.read_csv(self.csv, sep=',', nrows=1)

        self.dtype = dict()
        for col in df.columns:
            if df[col].dtype == np.float64:
                self.dtype[col] = Float()
            elif df[col].dtype == np.object:
                self.dtype[col] = String(255)
            elif df[col].dtype == np.int64:
                self.dtype[col] = Integer()

    def run(self):
        self.open_db()
        if self.generate_random_data is True:
            self._generate_random_data()
        self._set_dtype()
        run_time = self._insert_data()
        self.close_db()
        return run_time


def performance():
    my_logger = CustomLogger('main.yaml').logger
    my_config_file = 'main.ini'
    chunksizes = [5000, 10000, 20000, 50000]
    df = pd.DataFrame(columns=['chunksize', 'time'])
    for i, chunksize in enumerate(chunksizes):
        db = DatabaseConnector(logger=my_logger, config_file=my_config_file)
        db.chunksize = chunksize
        run_time = db.run()
        df.loc[i, 'chunksize'] = db.chunksize
        df.loc[i, 'time'] = str(run_time).split('.')[0]
    print(df)


@profiler('main.prof')
def main():
    my_logger = CustomLogger(file='main.yaml').logger
    my_config_file = 'main.ini'
    my_db = DatabaseConnector(logger=my_logger, config_file=my_config_file)
    my_db.run()
    print(my_db.get_top_data(top=5))


if __name__ == '__main__':
    main()
