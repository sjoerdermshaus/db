import string
from datetime import datetime

import numpy as np
import pandas as pd
from numpy.random import choice
from numpy.random import seed
from sqlalchemy import create_engine, event
from sqlalchemy.exc import IntegrityError
from sqlalchemy.types import Integer, Float, String

from custom_logger import CustomLogger
from configparser import ConfigParser

from profiler import profiler, runtime


class DatabaseConnector(object):

    def __init__(self, logger, config_file, batch_size=100000):
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
        self.table_full = '[{:s}].[{:s}].[{:s}]'.format(self.dbname, self.schema, self.table)

        # [run]
        self.drop_table = self.config['run'].getboolean('drop_table')
        self.create_table = self.drop_table
        self.export_data = self.config['run'].getboolean('export_data')
        self.use_primary_key = self.config['run'].getboolean('use_primary_key')
        self.if_exists = self.config['run']['if_exists']
        self.number_of_rows = int(self.config['run'].getfloat('number_of_rows'))
        self.number_of_float_columns = self.config['run'].getint('number_of_float_columns')
        self.number_of_string_columns = self.config['run'].getint('number_of_string_columns')
        self.csv = self.config['run']['csv']

        # other
        self.column_names = []
        self.df = pd.DataFrame()
        self.batch_size = min(batch_size, self.number_of_rows)
        self.logger = logger
        self.engine = None
        self.conn = None
        self.seed = seed(1234)
        self.dtype = None

    def open_db(self, SET_FAST_EXECUTEMANY_SWITCH=True):
        # create an engine and a connection
        self.logger.info('Establishing connection')
        engine_string = '{:s}+{:s}://{:s}'.format(self.db,
                                                  self.driver,
                                                  self.dsn)
        self.engine = create_engine(engine_string)

        if SET_FAST_EXECUTEMANY_SWITCH:
            @event.listens_for(self.engine, 'before_cursor_execute')
            def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                if executemany:
                    cursor.fast_executemany = True

        self.conn = self.engine.connect()
        self.logger.info('Establishing connection finished')

    def open_db_pymsql(self):
        self.db = 'mssql'
        self.driver = 'pymssql'
        user = ''
        hostname = '169.254.153.39'
        port = 1433
        dbname = 'test_db'
        # create an engine and a connection
        self.logger.info('Establishing connection')
        engine_string = '{:s}+{:s}://{:s}{:s}:{:d}/{:s}'.format(self.db,
                                                                self.driver,
                                                                user,
                                                                hostname,
                                                                port,
                                                                dbname)
        print(engine_string)
        try:
            self.engine = create_engine(engine_string)
            self.conn = self.engine.connect()
            self.logger.info('Establishing connection finished')
        except Exception as e:
            print(e)

    def delete_table(self):
        self.logger.info('Dropping table')
        if self.engine.has_table(table_name=self.table, schema=self.schema):
            query = f'DROP TABLE {self.table_full}'
            self.engine.execute(query)
        self.logger.info('Dropping table finished')

    def create_column_names(self):
        self.column_names = []
        self.column_names.append(['DB_ID', 'int'])
        self.dtype = {'DB_ID': Integer()}
        for m in range(1, self.number_of_float_columns + 1):
            self.column_names.append(['float{:d}'.format(m), 'float'])
            self.dtype[f'float{m}'] = Float()

        for m in range(1, self.number_of_string_columns + 1):
            self.column_names.append(['string{:d}'.format(m), 'nvarchar(max)'])
            self.dtype[f'string{m}'] = String(255)

    def make_table(self):
        self.logger.info('Creating table')

        if self.use_primary_key:
            query = 'CREATE TABLE {:s} (DB_ID int NOT NULL PRIMARY KEY\n'.format(self.table_full)
        else:
            query = 'CREATE TABLE {:s} (DB_ID int\n'.format(self.table_full)

        for col in self.column_names[1:]:
            query = '{:s},\n{:s} {:s}'.format(query, col[0], col[1])

        query = '{:s}\n);'.format(query)
        self.conn.execute(query)

        self.logger.info('Creating table finished')

    def create_random_data(self):
        self.logger.info('Creating random data')

        if self.if_exists == 'append':
            max_db_id = self.get_max_db_id()
        else:
            max_db_id = 0
        data_index = np.arange(1 + max_db_id, self.number_of_rows + 1 + max_db_id,
                               dtype=np.int32).reshape((self.number_of_rows, 1))
        columns_index = ['DB_ID']
        df_index = pd.DataFrame(data=data_index, columns=columns_index)

        data_float = np.random.rand(self.number_of_rows, self.number_of_float_columns)
        columns_float = [col[0] for col in self.column_names if col[1] == 'float']
        df_float = pd.DataFrame(data=data_float, columns=columns_float)

        columns_string = [col[0] for col in self.column_names if col[1] == 'nvarchar(max)']
        df_string = pd.DataFrame(columns=columns_string)
        for col in columns_string:
            df_string[col] = self.create_random_string(number_of_rows=self.number_of_rows)

        frames = [df_index, df_float, df_string]
        self.df = pd.concat(frames, axis=1)
        self.df.set_index(columns_index, inplace=True)
        self.logger.info('Creating random data finished')

    def export_random_data(self):
        self.logger.info('Exporting random data')
        self.df.to_csv(self.csv, sep=',', index=True)
        self.logger.info('Exporting random data finished')

    @staticmethod
    def create_random_string(size=20, number_of_rows=10):
        full_string = ''.join(choice(list(string.ascii_lowercase), size=size * number_of_rows, replace=True))
        return [full_string[i:i + size] for i in range(0, len(full_string), size)]

    @runtime()
    def insert_data(self):
        self.logger.info('Inserting data')

        chunksize = self.batch_size
        number_of_batches = int(self.number_of_rows / self.batch_size)
        start_time = datetime.now()
        try:
            for i in range(number_of_batches):
                start_time_batch = datetime.now()
                index = list(range(i * self.batch_size, (i + 1) * self.batch_size))
                df_batch = self.df.iloc[index, :]
                df_batch.to_sql(name=self.table,
                                con=self.engine,
                                schema=self.schema,
                                if_exists=self.if_exists,
                                index=True,
                                chunksize=chunksize,
                                dtype=self.dtype)
                format_string = '    Batch {:d}/{:d}, size {:d}/{:d}: {:s}'
                self.logger.info(format_string.format(i + 1,
                                                      number_of_batches,
                                                      self.batch_size,
                                                      self.number_of_rows,
                                                      str(datetime.now() - start_time_batch).split('.')[0]
                                                      )
                                 )
        except IntegrityError:
            self.logger.error('Primary key constraint')
        run_time = datetime.now() - start_time
        self.logger.info('Total runtime: {:s}'.format(str(runtime).split('.')[0]))

        self.logger.info('Inserting data finished')
        return run_time

    def get_max_db_id(self):
        query = 'SELECT MAX(DB_ID) AS MAX_DB_ID FROM {:s}'.format(self.table_full)
        df = pd.read_sql(query, con=self.engine, index_col=None)
        max_db_id = 0 if df.loc[0, 'MAX_DB_ID'] is None else df.iloc[0, 0]
        return max_db_id

    def get_data(self):
        query = 'SELECT * FROM {:s}'.format(self.table)
        return pd.read_sql(query, con=self.engine, index_col=['DB_ID'])

    def close_db(self):
        self.logger.info('Closing connection')
        self.conn.close()
        self.logger.info('Closing connection finished')

    def run(self):
        self.open_db()
        if self.drop_table is True:
            self.delete_table()
        self.create_column_names()
        if self.create_table is True:
            self.make_table()
        self.create_random_data()
        if self.export_data is True:
            self.export_random_data()
        run_time = self.insert_data()
        self.close_db()
        return run_time


def performance():
    my_logger = CustomLogger('main_db_logging.yaml').logger
    my_config_file = 'main_db_settings.ini'
    batch_sizes = [5000, 10000, 20000, 50000]
    df = pd.DataFrame(columns=['batch_size', 'time'])
    for i, batch_size in enumerate(batch_sizes):
        db = DatabaseConnector(logger=my_logger, batch_size=batch_size, config_file=my_config_file)
        run_time = db.run()
        df.loc[i, 'batch_size'] = db.batch_size
        df.loc[i, 'time'] = str(run_time).split('.')[0]
    print(df)


@profiler('main_db.prof')
@runtime()
def main():
    # performance()
    my_logger = CustomLogger('main_db_logging.yaml').logger
    my_config_file = 'main_db_settings.ini'
    my_db = DatabaseConnector(logger=my_logger, config_file=my_config_file)
    my_db.run()
    if my_db.number_of_rows <= 10:
        my_db.open_db()
        print('-' * 100)
        print(my_db.get_max_db_id())
        print('-' * 100)
        print(my_db.get_data())
        print('-' * 100)
        my_db.close_db()


if __name__ == '__main__':
    main()
