# Polygon - Downloader

Use this to download all trades/quotes for a given day and save them into a sorted golang gob using lz4 compression. This is very customized for my use-case but you can modify as needed. What's cool about this is that it can download all market data in about 10 minutes and saves these into compressed files for later processing.


This is for educational use only.

## TODO:

* Use flag to inject API key
* use flag to select stocks vs all stocks