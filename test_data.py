import unittest

import data


def get_test_column_names(df):
    return [_ for _ in df.columns if
            _.endswith(data.ROLLING_WINDOW_TEST_SUFFIX)]


class AverageTestRateTest(unittest.TestCase):

    def test_no_window(self):
        df = data.load_test_df('pa', windows=())
        test_window_columns = get_test_column_names(df)
        self.assertEqual(0, len(test_window_columns))

    def test_one_window(self):
        df = data.load_test_df('pa', windows=(3,))
        test_window_columns = get_test_column_names(df)
        self.assertEqual(1, len(test_window_columns))
        self.assertListEqual(['3' + data.ROLLING_WINDOW_TEST_SUFFIX],
                             test_window_columns)

    def test_two_windows(self):
        df = data.load_test_df('pa', windows=(3, 7,))
        test_window_columns = get_test_column_names(df)
        self.assertEqual(2, len(test_window_columns))
        self.assertListEqual(['3' + data.ROLLING_WINDOW_TEST_SUFFIX,
                              '7' + data.ROLLING_WINDOW_TEST_SUFFIX],
                             test_window_columns)

    def test_bad_abbrev(self):
        with self.assertRaises(ValueError):
            data.load_test_df('usa', windows=())


if __name__ == '__main__':
    unittest.main()
