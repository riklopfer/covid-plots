import unittest

import pandas as pd

import data


def get_test_column_names(df):
    return [_ for _ in df.columns if
            _.endswith(data.ROLLING_WINDOW_TEST_SUFFIX)]


class AverageTestRateTest(unittest.TestCase):

    def test_no_window(self):
        df = data.load_state_test_df('pa', windows=())
        test_window_columns = get_test_column_names(df)
        self.assertEqual(0, len(test_window_columns))

    def test_one_window(self):
        df = data.load_state_test_df('pa', windows=(3,))
        test_window_columns = get_test_column_names(df)
        self.assertEqual(1, len(test_window_columns))
        self.assertListEqual(['3' + data.ROLLING_WINDOW_TEST_SUFFIX],
                             test_window_columns)

    def test_two_windows(self):
        df = data.load_state_test_df('pa', windows=(3, 7,))
        test_window_columns = get_test_column_names(df)
        self.assertEqual(2, len(test_window_columns))
        self.assertListEqual(['3' + data.ROLLING_WINDOW_TEST_SUFFIX,
                              '7' + data.ROLLING_WINDOW_TEST_SUFFIX],
                             test_window_columns)

    def test_bad_abbrev(self):
        with self.assertRaises(ValueError):
            data.load_state_test_df('usa', windows=())

    def test_usa_data(self):
        df = data.load_usa_test_df((7,))
        self.assertFalse(df.empty)

    def test_start_date(self):
        start = pd.to_datetime('2020-05-01')
        df = data.load_state_test_df('pa', windows=(), start_date=start)
        self.assertFalse(df.empty)
        self.assertTrue((df.date >= start).all())

    def test_end_date(self):
        end = pd.to_datetime('2020-07-01')
        df = data.load_state_test_df('pa', windows=(), start_date=start)
        self.assertFalse(df.empty)
        self.assertTrue((df.date <= end).all())


if __name__ == '__main__':
    unittest.main()
