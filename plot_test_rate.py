#!/usr/bin/env python3.7
import argparse
import sys

import pandas as pd
import plotly.express as px

import data


def main(argv):
    parser = argparse.ArgumentParser(description=argv[0])
    parser.add_argument('state',
                        help='State abbreviation or Full Name',
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
                        default=True
                        )
    args = parser.parse_args(argv[1:])
    states = args.state
    window = args.window
    include_usa = args.include_usa
    start_date = args.start
    end_date = args.end

    national = data.CovidTrackingData()

    def load_state_df(abbrev):
        state_df = national.get_state_data(abbrev).get_avg_df(window)
        state_df = data.date_filter(state_df, start_date, end_date)
        return state_df

    if include_usa:
        df = national.get_avg_df(window)
        df = data.date_filter(df, start_date, end_date)
    else:
        df = None

    for state in states:
        if df is None:
            df = load_state_df(state)
        else:
            df = df.append(load_state_df(state))

    fig = px.line(df, x="date",
                  y='test-rate_{}day-avg'.format(window),
                  color='state')
    fig.show()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
