import chinadata.ca_data as ts #tushare的加速通道，不可修改
import time
from collections import deque
import pandas as pd
from config import TUSHARE_TOKEN, MAX_REQUESTS_PER_MINUTE

# Initialize Tushare API
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

# Request rate limiting configuration
request_timestamps = deque(maxlen=MAX_REQUESTS_PER_MINUTE)

def wait_for_rate_limit():
    """Wait if necessary to respect the rate limit of requests per minute"""
    current_time = time.time()

    if len(request_timestamps) < MAX_REQUESTS_PER_MINUTE:
        request_timestamps.append(current_time)
        return

    oldest_request = request_timestamps[0]
    if current_time - oldest_request < 60:
        wait_time = 60 - (current_time - oldest_request)
        print(f"[PERF] Rate limit reached. Waiting {wait_time:.2f} seconds...")
        time.sleep(wait_time)

    request_timestamps.popleft()
    request_timestamps.append(time.time())

def get_tushare_data(api_name, **kwargs):
    """A general function to fetch data from Tushare API with rate limiting."""
    wait_for_rate_limit()
    try:
        api_func = getattr(pro, api_name)
        df = api_func(**kwargs)
        if not isinstance(df, pd.DataFrame):
            print(f"API '{api_name}' did not return a DataFrame (returned type: {type(df)}). Returning empty DataFrame.")
            return pd.DataFrame(), 'success' # API call was successful, but data is not as expected.
        return df, 'success'
    except Exception as e:
        print(f"An error occurred when calling {api_name}: {e}")
        return pd.DataFrame(), 'error'

# Specific functions for different data types

def get_stock_basic(fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type'):
    """获取股票基础信息数据"""
    dfs = []
    for status in ['L', 'D', 'P']:
        df, api_status = get_tushare_data('stock_basic', exchange='', list_status=status, fields=fields)
        if api_status == 'success' and not df.empty:
            dfs.append(df)
            print(f"Fetched {len(df)} stocks with status {status}")
        elif api_status == 'error':
            print(f"Error fetching stocks with status {status}")

    if not dfs:
        return pd.DataFrame(), 'error'

    result_df = pd.concat(dfs, ignore_index=True)
    print(f"Total fetched: {len(result_df)} stocks")
    return result_df, 'success'

def get_trade_cal(exchange='', **kwargs):
    """获取交易日历数据"""
    # 默认获取所有字段，并允许通过kwargs覆盖
    params = {
        'exchange': exchange,
        'fields': 'exchange,cal_date,is_open,pretrade_date'
    }
    params.update(kwargs)
    return get_tushare_data('trade_cal', **params)

def get_stock_st(trade_date: str, **kwargs):
    """获取ST股票列表"""
    params = {
        'trade_date': trade_date
    }
    params.update(kwargs)
    return get_tushare_data('stock_st', **params)


def get_stk_rewards(ts_code: str, **kwargs):
    """获取上市公司管理层薪酬和持股"""
    params = {
        'ts_code': ts_code
    }
    params.update(kwargs)
    return get_tushare_data('stk_rewards', **params)


def _get_stock_hsgt(trade_date: str, hsgt_type: str, **kwargs):
    """获取沪深港通成份股的内部函数"""
    params = {
        'trade_date': trade_date,
        'type': hsgt_type
    }
    params.update(kwargs)
    return get_tushare_data('stock_hsgt', **params)

def get_stock_hsgt_hk_sz(trade_date: str, **kwargs):
    """获取深股通(港->深)成份股"""
    return _get_stock_hsgt(trade_date, 'HK_SZ', **kwargs)

def get_stock_hsgt_sz_hk(trade_date: str, **kwargs):
    """获取港股通(深->港)成份股"""
    return _get_stock_hsgt(trade_date, 'SZ_HK', **kwargs)

def get_stock_hsgt_hk_sh(trade_date: str, **kwargs):
    """获取沪股通(港->沪)成份股"""
    return _get_stock_hsgt(trade_date, 'HK_SH', **kwargs)

def get_stock_hsgt_sh_hk(trade_date: str, **kwargs):
    """获取港股通(沪->港)成份股"""
    return _get_stock_hsgt(trade_date, 'SH_HK', **kwargs)



def get_index_weight(index_code, start_date, end_date):
    return get_tushare_data('index_weight', index_code=index_code, start_date=start_date, end_date=end_date)




def get_trade_cal(start_date='20000101', end_date='20301231', exchange=''):
    return get_tushare_data('trade_cal', exchange=exchange, start_date=start_date, end_date=end_date, fields='exchange,cal_date,is_open,pretrade_date')

def get_income_vip(period=None, fields=None):
    """获取利润表数据 - VIP接口"""
    params = {}
    if period:
        params['period'] = period
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    return get_tushare_data('income_vip', **params)

def get_balancesheet_vip(period=None, fields=None):
    """获取资产负债表数据 - VIP接口"""
    params = {'period': period}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    return get_tushare_data('balancesheet_vip', **params)

def get_cashflow_vip(period=None, fields=None):
    """获取现金流量表数据 - VIP接口"""
    params = {'period': period}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    return get_tushare_data('cashflow_vip', **params)

def get_fina_indicator_vip(period=None, fields=None):
    """获取财务指标数据 - VIP接口"""
    params = {'period': period}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    return get_tushare_data('fina_indicator_vip', **params)

def get_fina_mainbz_vip(period=None, fields=None):
    """获取主营业务构成数据 - VIP接口"""
    params = {'period': period}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    return get_tushare_data('fina_mainbz_vip', **params)

def get_forecast_vip(period=None, fields=None):
    """获取业绩预告数据 - VIP接口"""
    params = {'period': period}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    return get_tushare_data('forecast_vip', **params)

def get_express_vip(period=None, fields=None):
    """获取业绩快报数据 - VIP接口"""
    params = {'period': period}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    return get_tushare_data('express_vip', **params)

def get_dividend(ann_date=None, ts_code=None, fields=None):
    """获取分红送股数据"""
    params = {}
    if ann_date:
        params['ann_date'] = ann_date
    if ts_code:
        params['ts_code'] = ts_code
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    # 注意：分红接口没有VIP版本，直接调用 dividend
    return get_tushare_data('dividend', **params)

def get_fina_audit(period=None, ts_code=None, fields=None):
    """获取财务审计意见数据"""
    params = {}
    if period:
        params['period'] = period
    if ts_code:
        params['ts_code'] = ts_code
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    return get_tushare_data('fina_audit', **params)



