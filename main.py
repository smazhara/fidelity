#!/usr/bin/env python

# watch ~/Downloads directory
# if new file is added, load it into the database
# Just write python code. Use directory watcher.
import os
import time
import pdb
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import ipdb
import sqlite3
from functools import lru_cache
import re


def conn():
    return sqlite3.connect("accounts_history.db")


def watch_for_accounts_history():
    class MyHandler(FileSystemEventHandler):
        def on_created(self, event):
            ensure_sqlite_db()

            file = event.src_path

            # only process Accounts_History.csv
            if event.is_directory \
                    or not re.search(r"Accounts_History(\s\(\d+\))?\.csv$", file) \
                    or event.event_type != 'created' \
                    or not os.path.isfile(file):
                    return

            process_accounts_history(file)
            print("Closed positions")
            print(closed_position_totals())
            os.remove(file)


    folder_to_track = "/fidelity"
    print(f"Waiting for Accounts_History.csv (all accounts). Simply download it.")
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, folder_to_track, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


def process_accounts_history(accounts_history_file_path):
    df = cleanup_accounts_history(accounts_history_file_path)
    save_accounts_history(df)
    generate_closed_positions_view()


def cleanup_accounts_history(accounts_history_file_path):

    # read file into array of lines
    with open(accounts_history_file_path) as f:
        content = f.readlines()

    # replace 'COINBASE, INC." 83853' with 'COINBASE, INC."" 83853' in content
    content = [x.replace('"COINBASE, INC." 83853', 'COINBASE INC. 83853') for x in content]
    content = [x.replace('"BrokerageLink Roth" 652301714', 'BrokerageLink Roth 652301714') for x in content]
    content = [x.replace('"BrokerageLink" 652301713', 'BrokerageLink 652301713') for x in content]

    # drop first 5 lines and last 15 lines
    content = content[5:-16]

    # load content into pandas dataframe, first row is header
    df = pd.DataFrame([x.strip().split(',') for x in content[1:]],
                      columns=[
                          "Run Date",
                          "Account",
                          "Action",
                          "Symbol",
                          "Security Description",
                          "Security Type",
                          "Quantity",
                          "Price ($)",
                          "Commission ($)",
                          "Fees ($)",
                          "Accrued Interest ($)",
                          "Amount ($)",
                          "Settlement Date",
                          "foo" # explained below
                      ]
                      )

    # rename columns so they are valid for sqlite
    df = df.rename(columns={
        "Run Date": "run_date",
        "Account": "account",
        "Action": "action",
        "Symbol": "symbol",
        "Security Description": "security_description",
        "Security Type": "security_type",
        "Quantity": "quantity",
        "Price ($)": "price",
        "Commission ($)": "commission",
        "Fees ($)": "fees",
        "Accrued Interest ($)": "accrued_interest",
        "Amount ($)": "amount",
        "Settlement Date": "settlement_date",
        "hash": "hash"
    })

    # simplify account names
    df['account'] = df.account.apply(lambda x: ' '.join(x.split(' ')[:-1]))

    # Convert columns to correct types
    df['run_date'] = pd.to_datetime(df['run_date'])
    df['settlement_date'] = pd.to_datetime(df['settlement_date'])

    # strip quotes from all strings
    df = df.apply(lambda x: x.str.strip('"') if x.dtype == "object" else x)

    # strip leading and trailing spaces
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    df["action_type"] = df.action.apply(transform_action)

    # HACK: drop df['foo'] columns
    # Coinbase contributions rows have 14 columns, while other rows have 13
    df = df.drop(columns=['foo'])

    # add column hash with hash of all columns - this will be the primary key
    # since Fidelity is not providing one
    df['hash'] = df.apply(lambda row: hash(tuple(row)), axis=1)

    # replace empty strings with None
    df = df.replace(r'^\s*$', None, regex=True)

    # We don't need it anymore. Also, if it is still there, next time we
    # download it, it'll get a new name.
    # os.remove(accounts_history_file_path)

    return df

def save_accounts_history(records):
    # read existing records from db
    existing_records = pd.read_sql_query("select * from accounts_history", conn())
    dup_keys = records["hash"].isin(existing_records["hash"])
    new_keys = ~dup_keys
    new_records = records[~dup_keys]

    new_records.to_sql('accounts_history', conn(), if_exists='append', index=False)


def transform_action(row):
    if row == 'Exchanges':
        return 'exchange'
    elif row.startswith('Dividend') or row.startswith('DIVIDEND'):
        return 'dividend'
    elif row == 'Contributions':
        return 'contribution'
    elif row == 'Transfer' or row.startswith('TRANSFERRED'):
        return 'transfer'
    elif row.startswith('ASSIGNED'):
        return 'assignment'
    elif row.startswith('YOU BOUGHT ASSIGNED'):
        return 'buy_assigned'
    elif row.startswith('YOU SOLD OPENING'):
        return 'sell_to_open'
    elif row.startswith('YOU SOLD CLOSING'):
        return 'sell_to_close'
    elif row.startswith('YOU BOUGHT CLOSING'):
        return 'buy_to_close'
    elif row.startswith('YOU BOUGHT OPENING'):
        return 'buy_to_open'
    elif row == 'Realized Gain/Loss':
        return 'realized_gain_loss'
    elif row.startswith('REINVESTMENT'):
        return 'reinvestment'
    else:
        raise Exception(f"Unknown action: {row}")


def accounts_history():
    return pd.read_sql_query("select * from accounts_history", conn())


def trading_transactions():
    return pd.read_sql_query("""
        select * from accounts_history
        where action_type in ('sell_to_open', 'buy_to_close', 'sell_to_close', 'buy_to_open')
    """, conn())


def closed_positions():
    return pd.read_sql_query("select * from closed_positions", conn())


def closed_position_totals():
    return pd.read_sql_query("""
        select
            strftime('%Y-%m', close_settlement_date) month,
            account,
            sum(gain_loss) gain_loss
        from closed_positions
        group by month, account
""", conn())


def generate_closed_positions_view():
    try:
        conn().execute("""
            create view closed_positions as
            select
                open.run_date as open_date,
                open.account as account,
                open.action as open_action,
                open.symbol as symbol,
                open.security_description as security_description,
                open.security_type as security_type,
                abs(open.quantity) as quantity,
                open.price as open_price,
                open.commission as open_commission,
                open.fees as open_fees,
                open.accrued_interest as open_accrued_interest,
                open.amount as open_amount,
                open.settlement_date as open_settlement_date,
                close.run_date as close_date,
                close.action as close_action,
                close.price as close_price,
                close.commission as close_commission,
                close.fees as close_fees,
                close.accrued_interest as close_accrued_interest,
                close.amount as close_amount,
                close.settlement_date as close_settlement_date,
                open.amount + close.amount as gain_loss

            from accounts_history open

            inner join accounts_history close

            where open.action_type in ('sell_to_open', 'buy_to_open')
              and open.account = close.account
              and close.action_type in ('sell_to_close', 'buy_to_close')
              and open.symbol = close.symbol
              and abs(open.quantity) = abs(close.quantity)
              and open.run_date <= close.run_date
        """)
    except sqlite3.OperationalError as e:
        pass


def open_positions():
    return pd.read_sql_query("""
        select
            open.run_date as open_date,
            open.account as account,
            open.action as open_action,
            open.symbol as symbol,
            open.security_description as security_description,
            open.security_type as security_type,
            abs(open.quantity) as quantity,
            open.price as open_price,
            open.commission as open_commission,
            open.fees as open_fees,
            open.accrued_interest as open_accrued_interest,
            open.amount as open_amount,
            open.settlement_date as open_settlement_date,
            close.run_date as close_date,
            close.action as close_action,
            close.price as close_price,
            close.commission as close_commission,
            close.fees as close_fees,
            close.accrued_interest as close_accrued_interest,
            close.amount as close_amount,
            close.settlement_date as close_settlement_date,
            open.amount + close.amount as gain_loss
        from accounts_history open
        left join accounts_history close
        where open.action_type in ('sell_to_open', 'buy_to_open')
          and close.action_type in ('sell_to_close', 'buy_to_close')
          and open.symbol = close.symbol
          and close.run_date is null
    """, conn())


def ensure_sqlite_db():

    # raw transactions
    conn().cursor().execute('''create table if not exists accounts_history (
        hash integer not null,
        run_date date not null,
        account text not null,
        action text not null,
        action_type text not null,
        symbol text,
        security_description text not null,
        security_type text ,
        quantity real not null,
        price real,
        commission real,
        fees real,
        accrued_interest real,
        amount real not null,
        settlement_date date,
        primary key (hash)
        )
    ''')



watch_for_accounts_history()
