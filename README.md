# ariva_stock_price

downloads history stock prices of symbols entered by commandline or by database. 
download a csv that is provided by ariva. that csv is loadad in to a pandas Dataframe. 
normalize date for either actual or history data. 
history and actual data actually inserting data in two different tables. 
Two class constants are defined for link stock symbol/exchange with integers required by ariva

## Examples

**get symbols from db and retrieve stock history data from 08.09.2023 to 01.10.2023**
+ python3 stockprice.py --dfrom 08.09.2023 --dto 01.10.2023 --database

**get symbols by commandline and retrieve stock history data from 08.09.2023 to 01.10.2023**
+ python3 stockprice.py --dfrom 08.09.2023 --dto 01.10.2023 AAPL MSFT HOT.DE

**get symbols from db and retrieve actual stock data**
+ python3 stockprice.py --actual --database

**get symbols from commandline and retrieve actual stock data**
+ python3 stockprice.py --actual AAPL MSFT HOT.DE
