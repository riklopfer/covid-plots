#!/usr/bin/env python3.7
import argparse
import sys

import plotly.express as px

import data


def main(argv):
    parser = argparse.ArgumentParser(description=argv[0])
    parser.add_argument('state',
                        help='State abbreviation',
                        type=str,
                        nargs='+'
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

    def load_state_df(abbrev):
        return data.load_state_test_df(abbrev, [window])

    df = None if not include_usa else data.load_usa_test_df([window])

    for state in state_abbrevs:
        if df is None:
            df = load_state_df(state)
        else:
            df = df.append(load_state_df(state))

    fig = px.line(df, x="date", y=f"{window}DayRollingTestRate", color='state')
    fig.show()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
