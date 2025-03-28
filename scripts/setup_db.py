import sqlite3
from sqlite3 import Connection


def create_database(database_name: str) -> Connection:
    conn = sqlite3.connect(database_name)
    return conn


def create_stock_data_table(conn: Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_data (
            symbol TEXT NOT NULL PRIMARY KEY,
            exchange TEXT,
            sector TEXT,
            industry TEXT,
            delisted INTEGER,
            daily BLOB,
            hourly BLOB,
            fifteen_minutely BLOB
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
            date_ts INTEGER,
            data BLOB,
            PRIMARY KEY (symbol, date_ts)
        );
        """
    )
    conn.commit()


def create_sessions_table(conn: Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            symbol TEXT NOT NULL,
            day_ts INTEGER,
            session_ts INTEGER,
            session TEXT NOT NULL,
            data BLOB,
            PRIMARY KEY (symbol, day_ts, session)
        );
        """
    )
    conn.commit()


def create_sessions_sequence_table(conn: Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions_sequence (
            symbol TEXT NOT NULL,
            tree_name TEXT NOT NULL,
            session TEXT NOT NULL,
            candle_type TEXT NOT NULL,
            parent_session TEXT NOT NULL,
            parent_candle_type TEXT NOT NULL,
            count INTEGER,
            PRIMARY KEY (symbol, tree_name, session, candle_type, parent_session, parent_candle_type)
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


def connect_to_db(year):
    conn = sqlite3.connect(f"stock_market_research_{year}.db")
    conn.row_factory = sqlite3.Row
    return conn


def setup_db(year):
    db_name = f"stock_market_research_{year}.db"
    conn = create_database(db_name)
    create_stock_data_table(conn)
    create_days_table(conn)
    create_sessions_table(conn)
    create_sessions_sequence_table(conn)
    create_trades_table(conn)
    add_cluster_column_to_trades_table(conn)
    add_subcluster_column_to_trades_table(conn)
    print(f"Database {db_name} created successfully")
    conn.close()


if __name__ == "__main__":
    setup_db(2021)
