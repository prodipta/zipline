# -*- coding: utf-8 -*-
"""
Created on Wed Apr 04 18:33:27 2018

@author: Prodipta
"""

import pandas as pd
import datetime as dt
import numpy as np
from pyfolio.utils import extract_rets_pos_txn_from_zipline
from pyfolio.timeseries import perf_stats
from empyrical.stats import cum_returns_final, aggregate_returns
import json


VaR_CUTOFF = 0.05
MAX_TXN = 50000

def convert_zipline_results_to_json(results):
    #make the json out of the day-wise dataframe
    res = results.drop(['transactions','orders','positions',],1)
    s = res.to_json(orient='index', date_format ='iso')
    jsonDict = json.loads(s)
    jsonArray = []

    for index in jsonDict:
        jsonDict[index]['start'] = index
        jsonArray.append(jsonDict[index])

    # compute drawdowns seperately
    value = results['portfolio_value']
    max_dd = np.zeros(len(value))
    for i in range(len(value)):
        max_dd[i] = (value[i]/value[0:i].max() - 1)
    results["max_drawdown"] = max_dd
    
    # algo returns vs benchmark returns for the main plot - this is cumulative returns
    returns = results['algorithm_period_return']
    benchmark_rets = results['benchmark_period_return']
    max_dd = results['max_drawdown']
    positions = results['positions']
    transactions = results['transactions']
    gross_lev = results['gross_leverage']
    average_txn = 0

    # extract structured info using pyfolio
    if any(results['transactions'].apply(len)) > 0:
        returns, positions, transactions, gross_lev = extract_rets_pos_txn_from_zipline(results)
        average_txn = np.mean(abs(transactions['txn_dollars']))
        # for plotting daily transaction, use <transactions>
        transactions['id'] = pd.Series([str(i) for i in range(len(transactions))],index=transactions.index)
        transactions = transactions.set_index(keys='id')
        transactions = transactions.drop(['sid'],1)
        # resize transactions if exceeds limit
        if len(transactions) > MAX_TXN:
            frames = [transactions.head(MAX_TXN/2),transactions.tail(MAX_TXN/2)]
            transactions = pd.concat(frames)
        transactions['symbol'] = transactions['symbol'].apply(getattr, args=('symbol',))


    benchmark_rets = results['benchmark_period_return'].diff().fillna(0)

    # the block of statistics to show
    out = perf_stats(returns, factor_returns=benchmark_rets)
    start = dt.datetime.strftime(results.index[0].to_datetime(), format='%Y-%m-%d')
    end = dt.datetime.strftime(results.index[len(results)-1].to_datetime(), format='%Y-%m-%d')
    cum_ret = cum_returns_final(returns)
    bemchmark_cum_ret = cum_returns_final(benchmark_rets)
    part1 = pd.Series([start,end,cum_ret,bemchmark_cum_ret], index=['Start Date','End Date','Cumulative Returns','Benchmark Cumulative Returns'])
    var = np.percentile(returns, 100 * VaR_CUTOFF)
    gross_lev_mean = np.mean(gross_lev)
    part2 = pd.Series([var,gross_lev_mean,average_txn], index=['Value at Risk','Average Gross Leverage','Average Transaction Value'])
    out = part1.append(out)
    out = out.append(part2)

    # get the histogram data
    #n = returns.size
    #sd = returns.std()
    #w = 3.5*sd/(n**0.33)
    #N = int((returns.max() - returns.min())/w)
    #hist = np.histogram(returns,N)
    #bins = hist[1]
    #counts = np.append(hist[0],0)
    #hist = [{"count": c, "bin": b} for c, b in zip(counts, bins)]

    # get the monthly returns for heatmap. For histogram of daily returns, use <returns>
    monthly_rets = aggregate_returns(returns,'monthly')

    # example of json dumping
    jsonOut = {
            'perf_stats': json.loads(out.to_json()),
            'algo_rets': json.loads(returns.to_json()),
            'benchmark_rets': json.loads(benchmark_rets.to_json()),
            'portfolio_rets': jsonArray,
            'transactions': json.loads(transactions.to_json()),
            'max_drawdown': json.loads(max_dd.to_json()),
            #'histogram':json.loads(json.dumps(str(hist))),
            'monthly': json.loads(monthly_rets.to_json())
            }

    return jsonOut