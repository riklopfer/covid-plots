#!/usr/bin/env python3.7
import argparse
import sys

import pandas as pd
import plotly.express as px

import data


def main(argv):
    parser = argparse.ArgumentParser(description=argv[0])
    parser.add_argument('county_state',
                        help='County, state (full). e.g. Clark,OH',
                        type=str,
                        nargs='+'
                        )
    parser.add_argument('--start',
                        help='start date',
                        type=pd.to_datetime,
                        default=None
                        )
    parser.add_argument('--end',
                        help='end date',
                        type=pd.to_datetime,
                        default=None
                        )
    parser.add_argument('--window',
                        help='size of the moving window',
                        type=int,
                        default=7
                        )
    parser.add_argument('--include_usa',
                        help='include data for whole USA',
                        type=eval,
                        choices=('True', 'False'),
                        default=False
                        )
    parser.add_argument('--metric',
                        help='Metric to look at.',
                        type=str,
                        nargs='+',
                        choices=['cases', 'deaths']
                        )
    args = parser.parse_args(argv[1:])
    county_states = [_.split(',') for _ in args.county_state]
    window = args.window
    include_usa = args.include_usa
    start_date = args.start
    end_date = args.end

    national = data.NyTimesData()

    def load_df(_county, _state):
        _df = (national.
               get_state_data(_state).
               get_county_data(_county).
               get_avg_df(window))
        _df = data.date_filter(_df, start_date, end_date)
        return _df

    if include_usa:
        df = national.get_avg_df(window)
        df = data.date_filter(df, start_date, end_date)
    else:
        df = None

    for county, state in county_states:
        if df is None:
            df = load_df(county, state)
        else:
            df = df.append(load_df(county, state))

    fig = px.line(df,
                  x="date",
                  y='cases_{}day-avg'.format(window),
                  color='location')
    fig.show()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
