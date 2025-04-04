# Stock Market Research Kit

Please note that this is currently very much a work in progress.

The Stock Market Research Kit is an open-source Python project brought to you by [base.report](https://base.report). It allows you to conduct systematic backtesting and analysis of the stock market. This research kit is designed to be flexible and extensible, enabling you to customize and improve upon the provided tools and methods.

## Prerequisites

1. Conda
- `source /opt/miniconda3/bin/activate`


### Financial Modeling Prep

Currently, the stock data is retrieved from Financial Modeling Prep. As of April 2023, you will need at least the [Professional plan](https://site.financialmodelingprep.com/developer/docs/pricing) to be able to use the bulk endpoint for fetching all profiles.

Before getting started, ensure that you have the following tools installed on your system:

- Conda: A package and environment manager for Python ([https://docs.conda.io/en/latest/miniconda.html](https://docs.conda.io/en/latest/miniconda.html))
- SQLite: A small, fast, self-contained, high-reliability, full-featured, SQL database engine (https://sqlite.org)

## Installation

1.  Clone the repository using Git:

```bash
git clone https://github.com/base-report/stock_market_research_kit.git
```

2.  Navigate to the project directory:

```bash
cd stock_market_research_kit
```

3.  Create a Conda environment for the project:

```bash
conda create --name smrk python=3.x
```

Replace `3.x` with the desired Python 3 version (e.g., 3.11.3). This command will create a new Conda environment named `smrk` with the specified Python version.

4.  Activate the Conda environment:

```bash
conda activate smrk
```

5.  Install the required dependencies:

```bash
conda install --file requirements.txt
```

This command installs all the necessary packages specified in the `requirements.txt` file.

## Basic Usage

### Set up database

Set up the database by running the `setup_db.py` script:

```bash
python -m scripts.setup_db
```

### Populate data/csv with csv data from [binance](https://data.binance.vision/?prefix=data/futures/um/monthly/klines)
```md
data/
└─ csv/
   └─ BTCUSDT/
      ├─ 1d/
      │  ├─ BTCUSDT-1d-2024-01.csv
      │  └─ BTCUSDT-1d-2024-02.csv
      ├─ 1h/
      │  ├─ BTCUSDT-1h-2024-01.csv
      │  └─ BTCUSDT-1h-2024-02.csv
      └─ 15m/
         ├─ BTCUSDT-15m-2024-01.csv
         └─ BTCUSDT-15m-2024-02.csv
```

### Load csv data to DB
```bash
python -m scripts.download_timeseries
```


### Markup data by sessions
```bash
python -m scripts.run_day_markuper
```


### Run backtest

To run the backtest for all of the results we have in the `stock_data` table, run the `run_backtest.py` script:

```bash
python scripts/run_backtest.py
```

_This should popoulate the `trades` table. With the default parameters and data as of early April 2023, there should be ~27,000 trades. This step should take ~3-5 minutes (possibly shorter or longer depending on the machine you are running this on)._

### Data Cleaning

After running the backtest, make sure to look for potentiall inaccurate data. For example, as of April 2023, the tickers `CBIO` and `VATE` contain some price data where some `adjClose` (adjusted close) are negative. As of result, any rows associated with these tickers will need to be deleted from the `trades` and `stock_data` tables.

### Analysis

To analyze data based on different factors like seasonality or ADR%, run the `analyze_trades.py` script:

```bash
python scripts/analyze_trades.py
```

This should create a series of CSV files in the `data/csv/` folder.

## ML Usage

### Plot stock charts

To plot a stock chart of the 200 daily candles prior to each trade entry, run the `plot_stock_charts.py` script:

```bash
python scripts/plot_stock_charts.py
```

_Please note that this loads all of the timeseries data into memory and uses 10 background workers to process the trades in parallel. This steps takes ~20 minutes to complete. Feel free to adjust the number of workers in `plot_stock_charts.py` to your machine's capabilities._

### Cluster images

To cluster the stock charts into different clusters based on visual appearance, run the `update_clusters.py` script:

```bash
python scripts/update_clusters.py
```

_This step takes ~70 minutes. The clustering uses `MiniBatchKMeans` and 20% training size. Change the `N_CLUSTERS` variable to experiment with different numbers of clusters. After completion, the `trades` table should have its `cluster` column updated. You should also find folders for each cluster under `data/images/clusters/`._

### Average cluster images

To get an average image of each cluster, run the `average_cluster_charts.py` script:

```bash
python scripts/average_cluster_charts.py
```

_This steps takes ~17 seconds. After complettion, you should find the images under `data/images/averages/`._

### Subcluster images

To divide each cluster into additional subclusters, run the `update_subclusters.py` script:

```bash
python scripts/update_subclusters.py
```

_This step takes ~6.5 minutes. Change the `N_SUBCLUSTERS` variable to experiment with different numbers of subclusters. After completion, the `trades` table should have its `subcluster` column updated. You should also find folders for each subcluster under `data/images/clusters/<CLUSTER>/`._

### Average subcluster images

To get an average image of each subcluster, run the `average_subcluster_charts.py` script:

```bash
python scripts/average_subcluster_charts.py
```

_This steps takes ~20 seconds. After complettion, you should find the images under `data/images/averages/`._

## Contributing

Contributions to the Stock Market Research Kit are welcome. If you have ideas for improvements or new features, please open an issue on GitHub or submit a pull request with your proposed changes.
