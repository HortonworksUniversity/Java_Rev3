#!/bin/bash

pig << DONE

fs -put /root/java/labs/demos/stocks.csv
fs -mkdir dividends
fs -mkdir counties
fs -put /root/java/labs/Lab2.1/constitution.txt
fs -mkdir populations
fs -mkdir smallfiles
fs -mkdir multiinputs
fs -mkdir stocks
fs -put /root/java/workspace/BloomFilter/stocks/NYSE_daily_prices_B.csv stocks
fs -mkdir logfiles
fs -mkdir stocksA
fs -mkdir -p bloom/lib
fs -mkdir bloom/dividends
fs -put /root/java/labs/data/stock_dividends/NYSE_dividends_A.csv bloom/dividends
fs -mkdir bloom/stocks
fs -put /root/java/labs/data/stock_prices/NYSE_daily_prices_A.csv bloom/stocks
fs -mkdir enron
fs -put /root/java/workspace/TFIDF/enron/mann.avro enron

DONE

hadoop fs -put /root/java/labs/data/stock_dividends/*.csv dividends
hadoop fs -put /root/java/labs/Lab1.2/HDFS_API/counties/*.csv counties
hadoop fs -put /root/java/workspace/TotalOrderPartitioner/populations/* populations
hadoop fs -put /root/java/labs/demos/WordCount2/*.txt smallfiles
hadoop fs -put /root/java/labs/demos/MultipleInputs/*.txt multiinputs
hadoop fs -put /root/java/workspace/MovingAverage/stocks/* stocks
hadoop fs -put /root/java/workspace/MapSideJoin/stocks/* stocks
hadoop fs -put /root/java/labs/Lab6.1/logs/* logfiles
hadoop fs -put /root/java/workspace/HBaseImport/stocksA/* stocksA

