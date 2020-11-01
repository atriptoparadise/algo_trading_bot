## Algorithmic trading bot
   
#### How to use:



1. Update data 
   - Data has been stored in a 15-min time frame, which includes the highest volume and highest price every day in 15-min unit, last day open and close (difference and ratio) of volume & price, etc. There're more than 3000 stocks stored, mostly from Russell 3000.
   - Run "python update.py" after regular trading hours (or set midnight=True to exclude current day's data);
   - Add symbols in watch_list to track during live trading


2. Find signals during trading hours
   - Run "python main.py", which has been scheduled to run uninterruptedly;
   - Go to the logs folder to catch the signals;
   - If you connect with Alpaca live/paper trading platform, it will automatically create orders by your setting
   - Run "python monitor.py" to monitor holding stocks and sell by your setting


3. V0 v.s. V1
   - V0 data are from Alpaca API, which request limit is 200/min and thus we need at least 15 minutes to go through all stocks; It only includes one exchange data;
   - V1 data are from Polygon.io API, request is unlimited and with multi-processing, we can run 3000 stocks within 1 minute.<br/>
<br/>


#### Find naive signals in live trading

1. Moving or current 15-min aggregated volume is larger than the threshold (> 1) * the highest volume in previous period;

2. Current price is larger than the threshold (< 1) * highest price in previous period;

3. Current price is less than the threshold (> 1) * open price;

4. After 3 pm: 
   - current > open;
   - (high - current) / (current - open) <= threshold 
