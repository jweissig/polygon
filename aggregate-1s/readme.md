# Polygon - Create 1s Aggregate Bars

Use this to create aggregate bars from historical stock trades over a 1s window size. Download all the trades using the [Trades API](https://polygon.io/docs/stocks/get_v3_trades__stockticker) and save that to a `json` file. Use that as input into this tool to generate 1s aggregate bars.

This is for educational use only.

## TODO:

* Use flag to load in json file vs hardcode it
* Output to file vs stdout
* Exclude trades based on condition codes
* The volume weighted average price
