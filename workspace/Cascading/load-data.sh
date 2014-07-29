#!/bin/bash

hadoop fs -mkdir stocks
hadoop fs -mkdir dividends
hadoop fs -put NYSE_daily_prices_A.csv stocks
hadoop fs -put NYSE_dividends_A.csv dividends

