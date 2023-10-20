from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest, OrderRequest
from alpaca.trading.enums import AssetClass
from datetime import datetime
import pandas as pd
import yfinance as yf


class Quant_FSA:

    def __init__(self, keys):
        self.stock_client = StockHistoricalDataClient(keys[0], keys[1])
        self.trading_client = TradingClient(keys[0], keys[1])

    def get_hist_data_EQ_Yahoo(self, start_year, end_year, symbols=None, a_filter=False):
        """
        Method that gets available historical data from Yahoo Finance for tradable US equities that are shortable and no ETFs for
        given timeframe and saves data to .feather files for each year
        :param start_year: int that represents first year of timeframe for requested data
        :param end_year: int that represents end year of timeframe for requested data
        :param symbols: list of strings with tickers for which data should be requested, if None all tickers are
                        requested
        :param a_filter: boolean to decide if universe should be restricted to equities that are tradable in Alpaca
        """
        # check if universe is restricted
        if a_filter:
            label = "filtered"

            # get available tickers
            search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)
            investable_universe = self.trading_client.get_all_assets(search_params)
            available_tickers = [stock.symbol for stock in investable_universe if stock.tradable and stock.shortable and "ETF" not in stock.name]
            if symbols is None:
                symbols = available_tickers

            else:
                # check if requested symbols are in investable universe
                non_available_tickers = [symbol for symbol in symbols if symbol not in available_tickers]
                if len(non_available_tickers) > 0:
                    raise Exception(f"Certain requested symbols are not in investable universe: "
                                    f"{[symbol for symbol in symbols if symbol not in available_tickers]}")
        else:
            label = "non_filtered"
            # get all tickers for NYSE, NASDAQ & AMEX listed equities
            if symbols is None:
                symbols = [symbol for symbol in pd.read_feather("tickers.feather")["Symbol"] if symbol is not None]

        tickers = yf.Tickers(symbols)

        for year in range(start_year, end_year + 1):
            tickers_hist = tickers.history(period="max", start=f"{year}-01-01", end=f"{year}-12-31")
            tickers_hist.reset_index().to_feather(f"US_Stocks_{year}_Yahoo_{label}.feather")

    def get_hist_data_EQ_Alpaca(self, start_year, end_year, symbols=None):
        """
        Method that gets available historical data from Alpaca for tradable US equities that are shortable and no ETFs for
        given timeframe and saves data to .feather files for each year
        :param start_year: int that represents first year of timeframe for requested data
        :param end_year: int that represents end year of timeframe for requested data
        :param symbols: list of strings with tickers for which data should be requested, if None all tickers are
                        requested
        """
        # get tickers of tradable and shortable US EQ that are not ETFs if symbols=None
        if symbols is None:
            search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)
            investable_universe = self.trading_client.get_all_assets(search_params)

            tickers = [stock.symbol for stock in investable_universe if stock.tradable and stock.shortable and "ETF" not in stock.name]

        else:
            tickers = [symbols]

        # determine historical data range
        years = [year for year in range(start_year, end_year)]

        # get all trading days (from 1970 to 2029)
        calendar = self.trading_client.get_calendar()

        for year in years:
            # get trading days for year
            dates = [pd.to_datetime(date.close.date()) for date in calendar if date.date.year == year]
            data_df = pd.DataFrame(data=dates, columns=["timestamp"])
            print("Request data")
            request_params = StockBarsRequest(symbol_or_symbols=tickers, timeframe=TimeFrame.Day, start=datetime(year, 1, 1),
                                                  end=datetime(year, 12, 31))
            bars = self.stock_client.get_stock_bars(request_params)
            print(year)
            if bars.data:
                # format timestamp column (cutoff time)
                df = bars.df.reset_index()
                df["timestamp"] = df["timestamp"].astype(str).str.split().str[0]
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df.set_index("timestamp", inplace=True)

                for ticker in df["symbol"].unique():
                    print(ticker)
                    data_ticker = df[df["symbol"] == ticker].copy()
                    data_ticker.drop(columns="symbol", inplace=True)
                    # rename columns such that columns can be assigned to ticker
                    columns = {"open": f"open_{ticker}", "high": f"high_{ticker}", "low": f"low_{ticker}", "close": f"close_{ticker}",
                               "volume": f"volume_{ticker}", "trade_count": f"trade_count_{ticker}", "vwap": f"vwap_{ticker}"}
                    data_ticker.rename(columns=columns, inplace=True)

                    # merge trading data of ticker with parent df
                    data_df = data_df.merge(data_ticker.set_index("timestamp"), on="timestamp")

                # save data in .feather file
                data_df.reset_index().to_feather(f"US_Stocks_{year}_Alpaca.feather")

    def get_positions(self):
        """
        Method that returns current positions of portfolio
        :return: dict containing tickers with corresponding position size
        """
        positions = self.trading_client.get_all_positions()
        positions_dict = {asset.symbol: asset.qty for asset in positions}
        return positions_dict

    def adjust_portfolio(self, new_positions, order_type_buy="market", order_type_sell="market"):
        """
        Method that adjusts portfolio and places corresponding short and long trades
        :param new_positions: dict containing tickers that are adjusted with corresponding position size (final)
        :param order_type_buy: string that determines which order type is used for increasing position (default: market order)
        :param order_type_sell: string that determines which order type is used for decreasing position (default: market order)
        :return: two lists with information on closed and executed trades
        """
        # get current positions
        current_positions = self.get_positions()

        # close positions that are not in new_positions
        close_positions = [ticker for ticker in current_positions.keys() if ticker not in new_positions.keys()]
        close_order_info = []
        for ticker in close_positions:
            close_order_info.append(self.trading_client.close_position(ticker))

        # adjust position for each ticker
        adjustment_order_info = []
        for ticker in new_positions.keys():
            # check if already invested and position different
            if ticker in current_positions.keys() and current_positions[ticker] != new_positions[ticker]:
                # determine if current position needs to be increased or decreased
                side_info = ("buy", order_type_buy) if new_positions[ticker] > current_positions[ticker] else ("sell", order_type_sell)
                quantity = abs(new_positions[ticker] - current_positions[ticker])

                # send order
                order_request = OrderRequest(symbol=ticker, qty=quantity, side=side_info[0], type=side_info[0], time_in_force="day")
                adjustment_order_info.append(self.trading_client.submit_order(order_request))

            else:
                # send order
                side = "buy" if new_positions[ticker] >= 0 else "sell"
                order_request = OrderRequest(symbol=ticker, qty=new_positions[ticker], side=side, type=order_type_buy, time_in_force="day")
                adjustment_order_info.append(self.trading_client.submit_order(order_request))

        return close_order_info, adjustment_order_info

    def get_performance(self):
        """
        Method that provides overview over performance
        :return: return since initialization
        """
        return int(self.trading_client.get_account().equity) / 100000 - 1


if __name__ == "__main__":
    keys = ["PK63EMCGHNEWJTXMVH00",  "71apnYcpN6j5Mc9qGdEjPpnd1csGIz3q0yM1uhIc"]
    start_year = 2000
    end_year = 2022
    # list with tickers, tickers need to be strings, if tickers = None: all tickers are loaded
    # ticker = "PBUS"  # benchmark etf for US stocks
    App = Quant_FSA(keys)
    # print(App.get_performance())
    App.get_hist_data_EQ_Yahoo(start_year, end_year, a_filter=True)
    x = 0
