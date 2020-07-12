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
                        default=True
                        )
    args = parser.parse_args(argv[1:])
    state_abbrevs = [_.lower() for _ in args.state]
    window = args.window
    include_usa = args.include_usa
    start_date = args.start
    end_date = args.end

    data_provider = data.CovidTracking()

    def load_state_df(abbrev):
        return data_provider.load_state_df(abbrev, window,
                                           start_date=start_date,
                                           end_date=end_date)

    if include_usa:
        df = data_provider.load_usa_df(window,
                                       start_date=start_date,
                                       end_date=end_date)
    else:
        df = None

    for state in state_abbrevs:
        if df is None:
            df = load_state_df(state)
        else:
            df = df.append(load_state_df(state))

    fig = px.line(df, x="date", y=f"{window}DayRollingTestRate", color='state')
    fig.show()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
