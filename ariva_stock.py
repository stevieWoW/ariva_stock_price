#!/usr/bin/env python
# coding: utf-8

import csv
import requests
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine,MetaData, Table, Column
from datetime import datetime,date,timedelta
from sqlalchemy.types import DECIMAL, VARCHAR, DateTime, Integer, String
from sqlalchemy.dialects.mysql import insert
from dotenv import load_dotenv
import os
import argparse


load_dotenv()


class finance():
    STOCK_LIST = {
        "AAPL":472,
        "MSFT":415,
        "FRE.DE":2038,
        "WDI.F":891,
        "LDDFF":140893852,
        "AMZN":348,
        "SAP":910,
        "FB":107840887,
        "TSLA":103220186,
        "VAR1.DE":100266587,
        "DIS":423,
        "AAD.DE":1088,
        "HOT.DE":2299,
        "BABA":117217535,
        "ALV.DE":292,
        "DPW.DE":754,
        "KO":400,
        "GOOGL":269125,
        "JNJ":412,
        "VLKPF":1753,
        }

    EXCHANGE_LIST = {
            "NASDAQ":40,
            "XETRA":6,
            "FRANKFURT":1,
            "HAMBURG":2
        }
    
    JOB={
        'h':{
            'table':'STOCK_HISTORY',
            'if_exists':'append',
            'dtype':{
                    "SYMBOL": VARCHAR,
                    "HIGH": DECIMAL,
                    "LOW": DECIMAL,
                    "OPEN":  DECIMAL,
                    "CLOSED": DECIMAL,
                    "VOLUME": DECIMAL,
                    "T_DATE": DateTime,
                    "UPDATED_AT": DateTime,
                    "X_OK": Integer
                }
        },
        'a':{
            'table':'STOCK_CUR_STOCK_PRICE',
            'if_exists':'append',
            'dtype':{
                    "SYMBOL": VARCHAR,
                    "CURRENT_STOCK_PRICE": DECIMAL,
                    "PREVIOUS_STOCK_PRICE": DECIMAL,
                    "DATE_OF_PRICE": DateTime,
                    "UPDATED_AT": DateTime
                }
        }
    }


    def __init__(self,exchange):
        self.exchange = exchange
        connection_string = os.environ["stock_DB_User"]
        self.engine = create_engine(connection_string, echo=True)
        self.job = 'h' # history (default)


    def __iteration(self,symbols:list,dfrom,dto):
        for symbol in symbols:
            if symbol not in finance.STOCK_LIST.keys():
                print(f'Symbol {symbol} not in STOCK_LIST')
                continue
            elif self.exchange.upper() not in finance.EXCHANGE_LIST.keys():
                print(f'Exchange {self.exchange} does not exists') 
                continue
            
            url = self.__generate_url(symbol,dfrom,dto)
            self.__download_csv(url,symbol)


    def __generate_url(self,symbol,dfrom,dto):
        url = f"https://ariva.de/quote/historic/historic.csv?secu={self.STOCK_LIST[symbol]}&boerse_id={self.EXCHANGE_LIST[self.exchange]}&clean_split=1&clean_payout=0&clean_bezug=1&min_time={dfrom}&max_time={dto}&trenner=%3B&go=Download"
        print(url)
        return url


    def history_data(self,symbols:list, dfrom:date, dto:date):
        self.__iteration(symbols,dfrom,dto)
        

    def current_data(self,symbols:list):
        self.job = 'a'
        dto=date.today().strftime("%d.%m.%Y")
        dfrom=date.today() - timedelta(days = 1)
        dfrom=dfrom.strftime("%d.%m.%Y")
        
        self.__iteration(symbols,dfrom,dto)
    

    def __insert_on_conflict_update(self, table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = (
                insert(table.table)
                .values(data)
        )
        
        if self.job == 'h':
            stmt = stmt.on_duplicate_key_update(
                HIGH=stmt.inserted.HIGH, 
                LOW=stmt.inserted.LOW, 
                OPEN=stmt.inserted.OPEN,
                CLOSED=stmt.inserted.CLOSED,
                VOLUME=stmt.inserted.VOLUME,
                UPDATED_AT=datetime.now(),
                X_OK=0
            )
        if self.job == 'a':
            stmt = stmt.on_duplicate_key_update(
                CURRENT_STOCK_PRICE=stmt.inserted.CURRENT_STOCK_PRICE,
                UPDATED_AT=datetime.now(),
                DATE_OF_PRICE=stmt.inserted.DATE_OF_PRICE
            )

        result = conn.execute(stmt)
        return result.rowcount
    

    def __normalize_df_for_a(self,symbol):
        # sanitize and normalize df
        self.df = self.df.dropna()
        self.df['SYMBOL'] = symbol.upper()
        self.df['UPDATED_AT'] = datetime.now()
        self.df = self.df.drop(['Stuecke','Erster','Hoch','Tief','Volumen'], axis=1)
        self.df = self.df.rename(columns={
            'Datum':'DATE_OF_PRICE',
            'Schlusskurs':'CURRENT_STOCK_PRICE',
            }
        )
        self.df['PREVIOUS_STOCK_PRICE'] = 0

        self.df.drop(self.df.index.to_list()[0:], axis=0)

        print(self.df)


    def __normalize_df_for_h(self,symbol):
        # sanitize and normalize df
        self.df = self.df.dropna()
        self.df['SYMBOL'] = symbol.upper()
        self.df['UPDATED_AT'] = datetime.now()
        self.df = self.df.drop(['Stuecke'], axis=1)
        self.df = self.df.rename(columns={
            'Datum':'T_DATE',
            'Erster':'OPEN',
            'Hoch':'HIGH',
            'Tief':'LOW',
            'Schlusskurs':'CLOSED',
            'Volumen':'VOLUME'
            }
        )
        self.df['VOLUME'] = self.df['VOLUME'].astype('Int64')
        self.df['X_OK'] = 0


    def __normalize_df(self,symbol):
        if self.job == 'a':
            self.__normalize_df_for_a(symbol)
            return 0
        
        self.__normalize_df_for_h(symbol)
        

    def __download_csv(self,url,symbol):
        response = requests.get(url)
        with open('out.csv', 'w') as f:
            writer = csv.writer(f)
            for line in response.iter_lines():
                writer.writerow(line.decode('utf-8').split(','))
        # read CSV
        self.df = pd.read_csv(
            "out.csv",
            sep=";",
            decimal=',',
            thousands='.',
            parse_dates=["Datum"]
        )

        self.__normalize_df(symbol)
        
        self.__write_to_db(**finance.JOB[self.job])


    def get_symbols_from_db(self):
        symbols = []
        meta = MetaData()

        STOCK_AVAILABLE_STOCK = Table(
            'STOCK_AVAILABLE_STOCK', meta,
            Column('COMPANY', String),
            Column('QUANTITY', Integer)
        )

        conn = self.engine.connect()
        s = STOCK_AVAILABLE_STOCK.select().where(STOCK_AVAILABLE_STOCK.c.QUANTITY>0)
        result = conn.execute(s)

        for row in result:
            symbols.append(row[0])

        return symbols


    def __write_to_db(self, **kwargs):
        # write to DB one by one
        try:
            self.df.to_sql(
                kwargs["table"],
                self.engine,
                if_exists=kwargs["if_exists"],
                index=False,
                chunksize=1,
                method=self.__insert_on_conflict_update,
                dtype=kwargs["dtype"]
            )
        except Exception as e:
            print(f'ERROR: {e.args}')         


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a","--actual", help="Retrieve actual stock data",action="store_true")
    parser.add_argument("--dfrom", help="retrieve data from this date")
    parser.add_argument("--dto", help="retrieve data to this date")
    parser.add_argument("-d","--database",help="Fetch Symbols from DB", action="store_true")
    parser.add_argument("symbols",nargs="*")
    
    args = parser.parse_args()

    fin = finance("XETRA")

    if not args.symbols and not args.database:
        raise ValueError("neither symbols nor database parameter added") 
    
    if args.database:
        symbols = fin.get_symbols_from_db()

    if args.symbols:
        symbols = args.symbols
        
    if args.actual:
        fin.current_data(symbols)

    if not args.actual:
        fin.history_data(symbols,args.dfrom,args.dto)


if __name__ == "__main__":
    main()




