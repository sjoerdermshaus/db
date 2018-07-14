from unittest import TestCase
from main_db import DatabaseConnector
import pandas as pd
from unittest.mock import MagicMock
from unittest import mock


class TestDatabaseConnector(TestCase):

    number_of_tests = 0

    def setUp(self):
        self.config_file = 'test_main_db_settings.ini'
        self.logger = MagicMock()
        self.sut = DatabaseConnector(logger=self.logger, config_file=self.config_file)
        self.sut.column_names = []
        self.sut.df = pd.DataFrame()
        self.test_column_names = [['DB_ID', 'int'],
                                  ['float1', 'float'],
                                  ['float2', 'float'],
                                  ['string1', 'nvarchar(max)'],
                                  ['string2', 'nvarchar(max)'],
                                  ['string3', 'nvarchar(max)']
                                  ]
        self.test_df = pd.DataFrame(
            data=[[1, 0.191519, 0.622109, 'yqfqjwpsqmfcgdhlajlq', 'rvhexhruavujsjbodmvj', 'wntjpehhrhsfcxsgiwrj'],
                  [2, 0.437728, 0.785359, 'dctmbltlrothklorxnua', 'ntaeeawyuimryjpicqlc', 'ptlmdkuzlxcibzoekdrp'],
                  [3, 0.779976, 0.272593, 'mfrfzzunqjiymgywmtpr', 'spdoczxeybzkcxnudsmf', 'axnyqjdztzqvjstrkhre']
                  ],
            columns=[col[0] for col in self.test_column_names],
        )
        self.test_df.set_index(['DB_ID'], inplace=True)
        self.sut.csv = MagicMock()

    def test_connection(self):
        self.sut.open_db()
        self.assertEqual(self.sut.conn.closed, False)
        self.sut.close_db()
        self.assertEqual(self.sut.conn.closed, True)

    def test_create_random_data(self):
        self.sut.create_column_names()
        self.assertEqual(self.sut.column_names, self.test_column_names)
        self.sut.create_random_data(max_db_id=0)
        self.sut.df = self.sut.df.iloc[0:3, :]
        pd.testing.assert_frame_equal(self.sut.df, self.test_df, check_dtype=False)

    def test_table(self):
        self.sut.open_db()
        self.sut.delete_table()
        self.sut.make_table()
        self.assertEqual(self.sut.engine.has_table(table_name=self.sut.table), True)
        self.sut.delete_table()
        self.assertEqual(self.sut.engine.has_table(table_name=self.sut.table), False)
        self.sut.close_db()

    def test_insert_and_get_data(self):
        self.sut.run()
        self.sut.df = self.sut.get_data()
        self.sut.export_random_data()
        self.sut.df = self.sut.df.iloc[0:3, :]
        pd.testing.assert_frame_equal(self.sut.df, self.test_df, check_dtype=False)

    def test_multiple_inserts(self):
        self.sut.run()
        self.sut.drop_table = False
        self.sut.create_table = False
        self.sut.run()
        self.sut.run()
        self.sut.df = self.sut.get_data()
        self.assertEqual(len(self.sut.df), 15)
        self.assertNotEqual(self.sut.df.loc[1, 'float1'], self.sut.df.loc[6, 'float1'])

    def tearDown(self):
        TestDatabaseConnector.number_of_tests += 1
        print('tearDown Test', TestDatabaseConnector.number_of_tests)
        self.sut.open_db()
        self.sut.delete_table()
        self.sut.close_db()
