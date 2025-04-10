import sqlite3
from sqlite3 import Connection


def create_database(database_name: str) -> Connection:
    conn = sqlite3.connect(database_name)
    return conn


def create_raw_candles_table(conn: Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_candles (
            symbol TEXT NOT NULL,
            date_ts TEXT NOT NULL,
            period TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (symbol, date_ts, period)
        );
        """
    )
    conn.commit()


def create_days_table(conn: Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS days (
            symbol TEXT NOT NULL,
            date_ts TEXT NOT NULL,
            data BLOB NOT NULL,
            PRIMARY KEY (symbol, date_ts)
        );
        """
    )
    conn.commit()


def create_profiles_table(conn: Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS backtested_profiles (
            strategy_name TEXT NOT NULL,
            profile_key TEXT NOT NULL,
            profile_symbol TEXT NOT NULL,
            profile_year TEXT NOT NULL,
            win INTEGER NOT NULL,
            lose INTEGER NOT NULL,
            guessed INTEGER NOT NULL,
            missed INTEGER NOT NULL,
            pnl REAL NOT NULL,
            trades BLOB NOT NULL,
            PRIMARY KEY (profile_key, strategy_name, profile_symbol, profile_year)
        );
        """
    )
    conn.commit()


def create_notifier_trades_table(conn: Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS notifier_trades (
            strategy_name TEXT NOT NULL,
            open_date_utc TEXT NOT NULL,
            symbol TEXT NOT NULL,
            pnl REAL NOT NULL,
            deadline_close TEXT NOT NULL,
            session_trade BLOB NOT NULL,
            full_close_date_utc TEXT,
            PRIMARY KEY (strategy_name, open_date_utc, symbol)
        );
        """
    )
    conn.commit()


def create_trades_table(conn: Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            entry_date TEXT,
            entry_price REAL,
            initial_stop REAL,
            consolidation_days INTEGER,
            adr20 REAL,
            volume20 REAL,
            partial_target_date TEXT,
            partial_target_price REAL,
            partial_target_reached INTEGER,
            exit_date TEXT,
            exit_price REAL,
            exit_reason TEXT,
            days_held INTEGER,
            FOREIGN KEY (symbol) REFERENCES stock_data (symbol)
        );
        """
    )
    conn.commit()


def add_cluster_column_to_trades_table(conn: Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        ALTER TABLE trades ADD COLUMN cluster INTEGER;
        """
    )
    conn.commit()


def add_subcluster_column_to_trades_table(conn: Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        ALTER TABLE trades ADD COLUMN subcluster INTEGER;
        """
    )
    conn.commit()


# def get_last_full_days_15m_rows(conn: Connection):
#     c = conn.cursor()



def connect_to_db(year):
    conn = sqlite3.connect(f"stock_market_research_{year}.db")
    conn.row_factory = sqlite3.Row
    return conn


def setup_db(year):
    db_name = f"stock_market_research_{year}.db"
    conn = create_database(db_name)
    create_raw_candles_table(conn)
    create_days_table(conn)
    create_profiles_table(conn)
    create_notifier_trades_table(conn)
    # create_trades_table(conn)
    # add_cluster_column_to_trades_table(conn)
    # add_subcluster_column_to_trades_table(conn)
    print(f"Database {db_name} created successfully")
    conn.close()


if __name__ == "__main__":
    for db_year in [
        2021,
        2022,
        2023,
        2024,
        2025
    ]:
        setup_db(db_year)
