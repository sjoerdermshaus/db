from datetime import datetime

import numpy as np
import pandas as pd

from sqlalchemy import create_engine, event
from sqlalchemy.types import Integer, Float, String
import pyodbc
import turbodbc

from custom_logger import CustomLogger
from configparser import ConfigParser

from multiprocessing import Pool


class DatabaseConnector(object):

    def __init__(self, logger, config_file, csv, interface, chunksize=100000, processes=1):
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
        self.table_full = '{:s}.{:s}.{:s}'.format(self.dbname, self.schema, self.table)
        self.interface = interface

        # other
        self.column_names = []
        self.df = pd.DataFrame()
        self.chunksize = chunksize
        self.processes = processes
        self.logger = logger
        self.csv = csv
        self.engine = None
        self.conn = None
        self.dtype = dict()

    def open_db(self, SET_FAST_EXECUTEMANY_SWITCH=True):
        # create an engine and a connection
        s = f'Establishing connection using interface: {self.interface}'
        self.logger.info('-' * len(s))
        self.logger.info(s)
        self.logger.info('-' * len(s))

        if self.interface == 'pyodbc:sqlalchemy':
            engine_string = f'{self.db}+pyodbc://{self.dsn}'
            self.engine = create_engine(engine_string)
            self.conn = self.engine.raw_connection()

            if SET_FAST_EXECUTEMANY_SWITCH:
                @event.listens_for(self.engine, 'before_cursor_execute')
                def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True

        elif self.interface == 'pyodbc':
            connection_string = f'DSN={self.dsn}'
            self.conn = pyodbc.connect(connection_string, autocommit=True)

        elif self.interface == 'turbodbc:sqlalchemy':
            engine_string = f'{self.db}+turbodbc://{self.dsn}'
            self.engine = create_engine(engine_string)
            self.conn = self.engine.raw_connection()

        elif self.interface == 'turbodbc':
            connection_string = f'dsn={self.dsn}'
            options = turbodbc.make_options(autocommit=True, parameter_sets_to_buffer=self.chunksize)
            self.conn = turbodbc.connect(connection_string=connection_string, turbodbc_options=options)

        else:
            pass

        self.logger.info('Establishing connection finished')

    def close_db(self):
        self.logger.info('Closing connection')
        self.conn.close()
        self.logger.info('Closing connection finished')

    def drop_table(self):
        cursor = self.conn.cursor()
        query = 'SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES'
        results = cursor.execute(query).fetchall()
        for result in results:
            if self.schema == result[0] and self.table == result[1]:

                self.logger.info('Dropping table')

                query = f"DROP TABLE {self.table_full}"
                cursor.execute(query)
                cursor.close()

                self.logger.info('Dropping table finished')

    def create_table(self):
        self.logger.info('Creating table')

        query = 'CREATE TABLE {:s} (DB_ID int NOT NULL PRIMARY KEY\n'.format(self.table_full)

        for i, col in enumerate(self.df.columns[1:]):
            dtype = None
            if self.df[col].dtype == np.float64:
                dtype = 'float'
            elif self.df[col].dtype == np.object:
                dtype = 'nvarchar(max)'
            query = '{:s},\n{:s} {:s}'.format(query, col, dtype)

        query = '{:s}\n);'.format(query)
        cursor = self.conn.cursor()
        cursor.execute(query)
        cursor.close()

        self.logger.info('Creating table finished')

    def set_dtype_and_column_names(self):
        self.logger.info('Determining dtype')
        self.df = pd.read_csv(self.csv, sep=',', nrows=10)
        self.column_names = self.df.columns

        self.dtype = dict()
        for col in self.df.columns:
            if self.df[col].dtype == np.float64:
                self.dtype[col] = Float()
            elif self.df[col].dtype == np.object:
                self.dtype[col] = String(255)
            elif self.df[col].dtype == np.int64:
                self.dtype[col] = Integer()

    def df_to_sql(self):

        self.logger.info('Inserting data')

        start_time = datetime.now()
        for i, df_chunk in enumerate(pd.read_csv(self.csv, chunksize=self.chunksize)):
            start_time_chunk = datetime.now()
            if i == 0:
                if_exists = 'replace'
            else:
                if_exists = 'append'
            df_chunk.to_sql(name=self.table,
                            con=self.engine,
                            schema=self.schema,
                            if_exists=if_exists,
                            index=False,
                            dtype=self.dtype)
            run_time = str(datetime.now() - start_time_chunk).split('.')[0]
            self.logger.info(f'    Chunk #{i + 1}, size: {len(df_chunk)}, upload time: {run_time}')

        run_time = datetime.now() - start_time
        self.logger.info('Total runtime: {:s}'.format(str(run_time).split('.')[0]))
        self.logger.info('Inserting data finished')

        return run_time

    @staticmethod
    def helper(args):
        df = args[0]
        csv_i = args[1]
        kwargs = args[2]
        engine_string = f'mssql+pyodbc://main_db_17'
        engine = create_engine(engine_string)
        raw_conn = engine.connect()

        @event.listens_for(engine, 'before_cursor_execute')
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                cursor.fast_executemany = True
        kwargs['con'] = engine

        chunksize = min(10000, len(df))
        number_of_chunks = int(len(df) / chunksize)
        for i in range(number_of_chunks):
            df_chunk = df.iloc[i * chunksize: (i + 1) * chunksize, :]
            kwargs['if_exists'] = 'replace' if i == 0 and csv_i == 0 else 'append'
            df_chunk.to_sql(**kwargs)

        raw_conn.close()

    def chunk_name(self, i):
        return f'{self.table}' if i == 0 else f'{self.table}_chunk{i}'

    def df_to_sql_mp(self):

        self.logger.info('Inserting data')
        self.set_dtype_and_column_names()

        start_time = datetime.now()
        for csv_i, df_chunk in enumerate(pd.read_csv(self.csv, chunksize=self.chunksize)):


            chunksize = int(len(df_chunk) / self.processes)
            x = [[df_chunk.iloc[i * chunksize:(i + 1) * chunksize, :],
                  csv_i,
                  {'name': self.chunk_name(i),
                   'schema': self.schema,
                   'index': False,
                   'dtype': self.dtype}] for i in range(self.processes)]

            start_time_chunk = datetime.now()
            with Pool(processes=self.processes) as p:
                p.map(self.helper, x)

            run_time = str(datetime.now() - start_time_chunk).split('.')[0]
            self.logger.info(f'    Chunk #{csv_i + 1}, size: {len(df_chunk)}, upload time: {run_time}')

        engine_string = f'mssql+pyodbc://main_db_17'
        engine = create_engine(engine_string)
        raw_conn = engine.connect().execution_options(autocommit=True)

        str_insert0 = f"INSERT INTO {self.dbname}.{self.schema}.{x[0][2]['name']}"
        for i in range(1, self.processes):
            str_insert = f"{str_insert0} SELECT * FROM {self.dbname}.{self.schema}.{x[i][2]['name']}"
            self.logger.info(str_insert)
            raw_conn.execute(str_insert)
            str_drop = f"DROP TABLE {self.dbname}.{self.schema}.{x[i][2]['name']}"
            self.logger.info(str_drop)
            raw_conn.execute(str_drop)

        raw_conn.close()

        run_time = datetime.now() - start_time
        self.logger.info('Total runtime: {:s}'.format(str(run_time).split('.')[0]))
        self.logger.info('Inserting data finished')

        return run_time

    def insert_data(self):

        self.logger.info('Inserting data')

        query = None
        start_time = datetime.now()
        for i, df_chunk in enumerate(pd.read_csv(self.csv, chunksize=self.chunksize)):
            # Create query
            if i == 0:
                str_cols = ','.join(df_chunk.columns)
                question_marks = ','.join(list('?' * len(df_chunk.columns)))
                query = f'INSERT INTO {self.table_full} ({str_cols}) VALUES ({question_marks})'

            params = None
            if self.interface == 'pyodbc':
                params = [tuple(l) for l in df_chunk.values]
            elif self.interface == 'turbodbc':
                params = df_chunk.values.tolist()

            cursor = self.conn.cursor()
            if self.interface == 'pyodbc':
                cursor.fast_executemany = True

            start_time_chunk = datetime.now()
            cursor.executemany(query, params)
            cursor.close()
            run_time = str(datetime.now() - start_time_chunk).split('.')[0]
            self.logger.info(f'    Chunk #{i + 1}, size: {len(df_chunk)}, upload time: {run_time}')

        run_time = datetime.now() - start_time
        self.logger.info('Total runtime: {:s}'.format(str(run_time).split('.')[0]))
        self.logger.info('Inserting data finished')

        return run_time

    def executemany(self):
        self.drop_table()
        self.create_table()
        run_time = self.insert_data()
        return run_time

    def get_data(self):
        self.open_db()
        query = 'SELECT TOP 10 * FROM {:s}'.format(self.table_full)
        df = pd.read_sql(query, con=self.conn, index_col=['DB_ID'])
        self.close_db()
        return df

    def run(self):
        self.open_db()
        self.set_dtype_and_column_names()
        if 'sqlalchemy' in self.interface:
            if self.processes == 1:
                run_time = self.df_to_sql()
            else:
                run_time = self.df_to_sql_mp()
        else:
            run_time = self.executemany()
        self.close_db()

        return run_time


def performance():
    kwargs = {'logger': CustomLogger('main_db_logging.yaml').logger,
              'config_file': 'main_db_settings.ini',
              'csv': 'random_data_10K.csv',
              'interface': None,
              'chunksize': None}

    interfaces = ['turbodbc', 'turbodbc:sqlalchemy', 'pyodbc:sqlalchemy']
    chunksizes = [100000]

    df = pd.DataFrame(columns=['interface', 'chunksize', 'run_time'])

    i = -1
    for interface in interfaces:
        kwargs['interface'] = interface
        for chunksize in chunksizes:
            kwargs['chunksize'] = chunksize

            i += 1
            run_time = DatabaseConnector(**kwargs).run()
            df.loc[i, 'interface'] = interface
            df.loc[i, 'chunksize'] = chunksize
            df.loc[i, 'run_time'] = str(run_time).split('.')[0]

    df.sort_values(by=['run_time'], ascending=True, inplace=True)
    df.to_excel(f"performance_{kwargs['csv'].split('.')[0]}.xlsx", index=False)
    print(df)


def main():
    kwargs = {'logger': CustomLogger('main_db_logging.yaml').logger,
              'config_file': 'main_db_settings.ini',
              'csv': 'random_data_2M.csv',
              'interface': 'pyodbc:sqlalchemy',
              'chunksize': 100000,
              'processes': 1}
    DatabaseConnector(**kwargs).run()


if __name__ == '__main__':
    main()
    # performance()
