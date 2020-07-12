import unittest

import pandas as pd

import data


def get_test_column_names(df):
    return [_ for _ in df.columns if
            _.endswith(data.ROLLING_WINDOW_TEST_SUFFIX)]


class CovidTrackingTest(unittest.TestCase):

    def setUp(self) -> None:
        self.data_provider = data.CovidTracking()

    def test_zero_window(self):
        df = self.data_provider.load_state_df('pa', window=0)
        test_window_columns = get_test_column_names(df)
        # self.assertEqual(0, len(test_window_columns))
        self.assertEqual(1, len(test_window_columns))
        self.assertListEqual(['0' + data.ROLLING_WINDOW_TEST_SUFFIX],
                             test_window_columns)

    def test_window(self):
        df = self.data_provider.load_state_df('pa', window=3)
        test_window_columns = get_test_column_names(df)
        self.assertEqual(1, len(test_window_columns))
        self.assertListEqual(['3' + data.ROLLING_WINDOW_TEST_SUFFIX],
                             test_window_columns)

    def test_bad_abbrev(self):
        with self.assertRaises(ValueError):
            self.data_provider.load_state_df('usa', window=0)

    def test_usa_data(self):
        df = self.data_provider.load_usa_df(window=7)
        self.assertFalse(df.empty)

    def test_start_date(self):
        start = pd.to_datetime('2020-05-01')
        df = self.data_provider.load_state_df('pa', window=0,
                                              start_date=start)
        self.assertFalse(df.empty)
        self.assertTrue((df.date >= start).all())

    def test_end_date(self):
        end = pd.to_datetime('2020-07-01')
        df = self.data_provider.load_state_df('pa', window=0,
                                              end_date=end)
        self.assertFalse(df.empty)
        self.assertTrue((df.date <= end).all())


class NyTimesTest(unittest.TestCase):

    def setUp(self) -> None:
        self.data_provider = data.NyTimes()

    def test_bad_abbrev(self):
        with self.assertRaises(ValueError):
            self.data_provider.load_state_df('usa', window=0)

    def test_usa_data(self):
        df = self.data_provider.load_usa_df(window=7)
        self.assertFalse(df.empty)

    def test_start_date(self):
        start = pd.to_datetime('2020-05-01')
        df = self.data_provider.load_state_df('pa', window=0,
                                              start_date=start)
        self.assertFalse(df.empty)
        self.assertTrue((df.date >= start).all())

    def test_end_date(self):
        end = pd.to_datetime('2020-07-01')
        df = self.data_provider.load_state_df('pa', window=0,
                                              end_date=end)
        self.assertFalse(df.empty)
        self.assertTrue((df.date <= end).all())


class NyTimesDataTest(unittest.TestCase):
    def setUp(self) -> None:
        self.data = data.NyTimesData()

    def test_usa_data(self):
        national = self.data.get_df()
        self.assertFalse(national.empty)

        with_average = self.data.with_moving_averages(7)
        self.assertFalse(with_average.empty)

    def test_pa_data(self):
        pa = self.data.get_state_data('Pennsylvania')
        pa_data = pa.get_df()
        self.assertFalse(pa_data.empty)

    def test_allegheny(self):
        allegheny = (self.data
                     .get_state_data('Pennsylvania')
                     .get_county_data('Allegheny'))
        adf = allegheny.get_df()
        self.assertFalse(adf.empty)

        avg = allegheny.with_moving_averages(7)
        self.assertFalse(avg.empty)


if __name__ == '__main__':
    unittest.main()
