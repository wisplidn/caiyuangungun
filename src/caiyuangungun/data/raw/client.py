import chinadata.ca_data as ts #tushare的特殊客户通道，修改会导致token失效
import time
from collections import deque
import pandas as pd
from caiyuangungun.config import TUSHARE_TOKEN, MAX_REQUESTS_PER_MINUTE

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
            # 如果返回值不是DataFrame（包括None），则返回一个空的DataFrame
            return pd.DataFrame()
        return df
    except Exception as e:
        error_message = f"An error occurred when calling {api_name}: {e}"
        print(error_message)
        raise IOError(error_message)

def get_tushare_data_with_pagination(api_name, page_size=4000, max_pages=10, **kwargs):
    """
    带自动分页功能的Tushare数据获取函数
    
    Args:
        api_name: API接口名称
        page_size: 每页数据量，默认4000
        max_pages: 最大页数限制，防止无限循环，默认10
        **kwargs: API参数
    
    Returns:
        pd.DataFrame: 完整的分页数据
    """
    all_data = []
    offset = 0
    page_num = 1
    total_records = 0
    
    print(f"    📄 开始分页获取 {api_name} 数据...")
    
    while page_num <= max_pages:
        try:
            # 添加offset参数
            params = kwargs.copy()
            params['offset'] = str(offset)
            
            # 调用API
            wait_for_rate_limit()
            api_func = getattr(pro, api_name)
            df = api_func(**params)
            
            if df is None or df.empty:
                print(f"    📄 第 {page_num} 页: 无数据，结束分页")
                break
            
            current_page_records = len(df)
            total_records += current_page_records
            
            # 添加分页元数据到DataFrame
            df = df.copy()
            df['_page_number'] = page_num
            df['_page_offset'] = offset
            df['_page_records'] = current_page_records
            
            all_data.append(df)
            
            print(f"    📄 第 {page_num} 页: {current_page_records} 条记录 (累计: {total_records}) [offset: {offset}]")
            
            # 如果返回数据少于页面大小，说明已经是最后一页
            if current_page_records < page_size:
                print(f"    📄 第 {page_num} 页数据量 < {page_size}，这是最后一页")
                break
            
            # 重要修正：offset基于实际获取的记录数减去100，保留小量重叠用于验证连续性
            offset += current_page_records - 100
            page_num += 1
            
        except Exception as e:
            print(f"    ❌ 第 {page_num} 页获取失败: {e}")
            if not all_data:  # 如果第一页就失败，抛出异常
                raise IOError(f"Failed to fetch first page of {api_name}: {e}")
            break
    
    if not all_data:
        print(f"    ⚠️  {api_name} 未获取到任何数据")
        return pd.DataFrame()
    
    final_pages = len(all_data)
    print(f"    ✅ 分页完成: 共 {final_pages} 页，总计 {total_records} 条记录")
    
    # 合并所有页面的数据
    return pd.concat(all_data, ignore_index=True)

# Specific functions for different data types

def get_stock_basic(fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type'):
    """获取股票基础信息数据"""
    dfs = []
    for status in ['L', 'D', 'P']:
        try:
            df = get_tushare_data('stock_basic', exchange='', list_status=status, fields=fields)
            if not df.empty:
                dfs.append(df)
                print(f"Fetched {len(df)} stocks with status {status}")
        except IOError as e:
            print(f"Error fetching stocks with status {status}: {e}")

    if not dfs:
        raise IOError("Failed to fetch any stock_basic data after trying all list statuses.")

    result_df = pd.concat(dfs, ignore_index=True)
    print(f"Total fetched: {len(result_df)} stocks")
    return result_df

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
    try:
        df = get_tushare_data('stock_st', **params)
        if df is None:
            df = pd.DataFrame()
        return df, 'success'
    except Exception as e:
        print(f"Error fetching stock_st data: {e}")
        return pd.DataFrame(), 'error'

def get_hk_hold(trade_date: str, **kwargs):
    """获取沪深港股通持股明细"""
    params = {
        'trade_date': trade_date
    }
    params.update(kwargs)
    try:
        df = get_tushare_data('hk_hold', **params)
        if df is None:
            df = pd.DataFrame()
        return df, 'success'
    except Exception as e:
        print(f"Error fetching hk_hold data: {e}")
        return pd.DataFrame(), 'error'

def get_stk_managers(ts_code: str, **kwargs):
    """获取上市公司管理层"""
    params = {
        'ts_code': ts_code
    }
    params.update(kwargs)
    try:
        df = get_tushare_data('stk_managers', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching stk_managers data: {e}")
        return pd.DataFrame(), 'error'


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

def get_daily(trade_date: str, **kwargs):
    """获取A股日线行情"""
    params = {
        'trade_date': trade_date
    }
    params.update(kwargs)
    try:
        df = get_tushare_data('daily', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching daily data: {e}")
        return pd.DataFrame(), 'error'


def get_daily_basic(trade_date: str, **kwargs):
    """获取每日指标"""
    params = {
        'trade_date': trade_date
    }
    params.update(kwargs)
    try:
        df = get_tushare_data('daily_basic', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching daily_basic data: {e}")
        return pd.DataFrame(), 'error'



def get_suspend_d(trade_date: str, **kwargs):
    """获取每日停复牌信息"""
    params = {
        'trade_date': trade_date
    }
    params.update(kwargs)
    return get_tushare_data('suspend_d', **params)


def get_repurchase(start_date: str, end_date: str, **kwargs):
    """获取股票回购信息"""
    params = {
        'start_date': start_date,
        'end_date': end_date
    }
    params.update(kwargs)
    try:
        df = get_tushare_data('repurchase', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching repurchase data: {e}")
        return pd.DataFrame(), 'error'

def get_share_float(start_date: str, end_date: str, **kwargs):
    """获取限售股解禁信息"""
    params = {
        'start_date': start_date,
        'end_date': end_date
    }
    params.update(kwargs)
    return get_tushare_data('share_float', **params)


def get_top10_holders(ts_code: str, **kwargs):
    """获取前十大股东"""
    params = {
        'ts_code': ts_code
    }
    params.update(kwargs)
    try:
        df = get_tushare_data('top10_holders', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching top10_holders data: {e}")
        return pd.DataFrame(), 'error'

def get_top10_floatholders(ts_code: str, **kwargs):
    """获取前十大流通股东"""
    params = {
        'ts_code': ts_code
    }
    params.update(kwargs)
    return get_tushare_data('top10_floatholders', **params)

def get_pledge_detail(ts_code: str, **kwargs):
    """获取股权质押明细"""
    params = {
        'ts_code': ts_code
    }
    params.update(kwargs)
    return get_tushare_data('pledge_detail', **params)

def get_adj_factor(trade_date: str, **kwargs):
    """获取复权因子"""
    params = {
        'trade_date': trade_date
    }
    params.update(kwargs)
    try:
        df = get_tushare_data('adj_factor', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching adj_factor data: {e}")
        return pd.DataFrame(), 'error'


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
    try:
        df = get_tushare_data('income_vip', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching income_vip data: {e}")
        return pd.DataFrame(), 'error'

def get_balancesheet_vip(period=None, fields=None):
    """获取资产负债表数据 - VIP接口"""
    params = {'period': period}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    try:
        df = get_tushare_data('balancesheet_vip', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching balancesheet_vip data: {e}")
        return pd.DataFrame(), 'error'

def get_cashflow_vip(period=None, fields=None):
    """获取现金流量表数据 - VIP接口（原版本，保持向后兼容）"""
    params = {'period': period}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    try:
        df = get_tushare_data('cashflow_vip', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching cashflow_vip data: {e}")
        return pd.DataFrame(), 'error'

def get_cashflow_vip_complete(period=None, fields=None, enable_pagination=True):
    """
    获取现金流量表数据 - VIP接口（完整版本，支持自动分页）
    
    Args:
        period: 财报期间
        fields: 字段列表
        enable_pagination: 是否启用分页功能，默认True
    
    Returns:
        tuple: (DataFrame, status)
    """
    params = {'period': period}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    
    try:
        if enable_pagination:
            # 使用分页功能获取完整数据
            df = get_tushare_data_with_pagination('cashflow_vip', **params)
        else:
            # 使用原始方法
            df = get_tushare_data('cashflow_vip', **params)
        
        return df, 'success'
    except Exception as e:
        print(f"Error fetching cashflow_vip data: {e}")
        return pd.DataFrame(), 'error'

def get_income_vip_complete(period=None, fields=None, enable_pagination=True):
    """
    获取利润表数据 - VIP接口（完整版本，支持自动分页）
    
    Args:
        period: 财报期间
        fields: 字段列表
        enable_pagination: 是否启用分页功能，默认True
    
    Returns:
        tuple: (DataFrame, status)
    """
    params = {'period': period}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    
    try:
        if enable_pagination:
            df = get_tushare_data_with_pagination('income_vip', **params)
        else:
            df = get_tushare_data('income_vip', **params)
        
        return df, 'success'
    except Exception as e:
        print(f"Error fetching income_vip data: {e}")
        return pd.DataFrame(), 'error'

def get_balancesheet_vip_complete(period=None, fields=None, enable_pagination=True):
    """
    获取资产负债表数据 - VIP接口（完整版本，支持自动分页）
    
    Args:
        period: 财报期间
        fields: 字段列表
        enable_pagination: 是否启用分页功能，默认True
    
    Returns:
        tuple: (DataFrame, status)
    """
    params = {'period': period}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    
    try:
        if enable_pagination:
            df = get_tushare_data_with_pagination('balancesheet_vip', **params)
        else:
            df = get_tushare_data('balancesheet_vip', **params)
        
        return df, 'success'
    except Exception as e:
        print(f"Error fetching balancesheet_vip data: {e}")
        return pd.DataFrame(), 'error'

def get_daily_complete(trade_date=None, fields=None, enable_pagination=True):
    """
    获取日交易数据（完整版本，支持自动分页）
    
    Args:
        trade_date: 交易日期
        fields: 字段列表
        enable_pagination: 是否启用分页功能，默认True
    
    Returns:
        tuple: (DataFrame, status)
    """
    params = {'trade_date': trade_date}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    
    try:
        if enable_pagination:
            df = get_tushare_data_with_pagination('daily', **params)
        else:
            df = get_tushare_data('daily', **params)
        
        return df, 'success'
    except Exception as e:
        print(f"Error fetching daily data: {e}")
        return pd.DataFrame(), 'error'

def get_daily_basic_complete(trade_date=None, fields=None, enable_pagination=True):
    """
    获取日基本信息数据（完整版本，支持自动分页）
    
    Args:
        trade_date: 交易日期
        fields: 字段列表
        enable_pagination: 是否启用分页功能，默认True
    
    Returns:
        tuple: (DataFrame, status)
    """
    params = {'trade_date': trade_date}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    
    try:
        if enable_pagination:
            df = get_tushare_data_with_pagination('daily_basic', **params)
        else:
            df = get_tushare_data('daily_basic', **params)
        
        return df, 'success'
    except Exception as e:
        print(f"Error fetching daily_basic data: {e}")
        return pd.DataFrame(), 'error'

def get_adj_factor_complete(trade_date=None, fields=None, enable_pagination=True):
    """
    获取复权因子数据（完整版本，支持自动分页）
    
    Args:
        trade_date: 交易日期
        fields: 字段列表
        enable_pagination: 是否启用分页功能，默认True
    
    Returns:
        tuple: (DataFrame, status)
    """
    params = {'trade_date': trade_date}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    
    try:
        if enable_pagination:
            df = get_tushare_data_with_pagination('adj_factor', **params)
        else:
            df = get_tushare_data('adj_factor', **params)
        
        return df, 'success'
    except Exception as e:
        print(f"Error fetching adj_factor data: {e}")
        return pd.DataFrame(), 'error'

def get_fina_indicator_vip(period=None, fields=None):
    """获取财务指标数据 - VIP接口"""
    params = {'period': period}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    try:
        df = get_tushare_data('fina_indicator_vip', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching fina_indicator_vip data: {e}")
        return pd.DataFrame(), 'error'

def get_fina_mainbz_vip(period=None, fields=None):
    """获取主营业务构成数据 - VIP接口"""
    params = {'period': period}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    try:
        df = get_tushare_data('fina_mainbz_vip', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching fina_mainbz_vip data: {e}")
        return pd.DataFrame(), 'error'

def get_forecast_vip(period=None, fields=None):
    """获取业绩预告数据 - VIP接口"""
    params = {'period': period}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    try:
        df = get_tushare_data('forecast_vip', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching forecast_vip data: {e}")
        return pd.DataFrame(), 'error'

def get_express_vip(period=None, fields=None):
    """获取业绩快报数据 - VIP接口"""
    params = {'period': period}
    if fields:
        params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
    try:
        df = get_tushare_data('express_vip', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching express_vip data: {e}")
        return pd.DataFrame(), 'error'

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
    try:
        df = get_tushare_data('dividend', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching dividend data: {e}")
        return pd.DataFrame(), 'error'

def get_fina_audit(ts_code: str, **kwargs):
    """获取财务审计意见数据"""
    params = {
        'ts_code': ts_code
    }
    params.update(kwargs)
    try:
        df = get_tushare_data('fina_audit', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching fina_audit data: {e}")
        return pd.DataFrame(), 'error'


# ===== 新增接口 - 按交易日遍历 (TradeDateArchiver) =====

def get_block_trade(trade_date=None, ts_code=None, **kwargs):
    """获取大宗交易数据"""
    params = {}
    if trade_date:
        params['trade_date'] = trade_date
    if ts_code:
        params['ts_code'] = ts_code
    params.update(kwargs)
    try:
        df = get_tushare_data('block_trade', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching block_trade data: {e}")
        return pd.DataFrame(), 'error'

def get_cyq_perf(trade_date=None, ts_code=None, **kwargs):
    """获取每日筹码及胜率数据"""
    params = {}
    if trade_date:
        params['trade_date'] = trade_date
    if ts_code:
        params['ts_code'] = ts_code
    params.update(kwargs)
    try:
        df = get_tushare_data('cyq_perf', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching cyq_perf data: {e}")
        return pd.DataFrame(), 'error'

def get_cyq_chips(trade_date=None, ts_code=None, **kwargs):
    """获取每日筹码分布数据"""
    params = {}
    if trade_date:
        params['trade_date'] = trade_date
    if ts_code:
        params['ts_code'] = ts_code
    params.update(kwargs)
    try:
        df = get_tushare_data('cyq_chips', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching cyq_chips data: {e}")
        return pd.DataFrame(), 'error'

def get_ah_price(trade_date=None, ts_code=None, **kwargs):
    """获取AH股比价数据"""
    params = {}
    if trade_date:
        params['trade_date'] = trade_date
    if ts_code:
        params['ts_code'] = ts_code
    params.update(kwargs)
    try:
        df = get_tushare_data('ah_price', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching ah_price data: {e}")
        return pd.DataFrame(), 'error'

def get_stk_surv(trade_date=None, ts_code=None, **kwargs):
    """获取机构调研数据"""
    params = {}
    if trade_date:
        params['trade_date'] = trade_date
    if ts_code:
        params['ts_code'] = ts_code
    params.update(kwargs)
    try:
        df = get_tushare_data('stk_surv', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching stk_surv data: {e}")
        return pd.DataFrame(), 'error'

def get_margin(trade_date=None, exchange_id=None, **kwargs):
    """获取融资融券交易汇总数据"""
    params = {}
    if trade_date:
        params['trade_date'] = trade_date
    if exchange_id:
        params['exchange_id'] = exchange_id
    params.update(kwargs)
    try:
        df = get_tushare_data('margin', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching margin data: {e}")
        return pd.DataFrame(), 'error'

def get_margin_detail(trade_date=None, ts_code=None, **kwargs):
    """获取融资融券交易明细数据"""
    params = {}
    if trade_date:
        params['trade_date'] = trade_date
    if ts_code:
        params['ts_code'] = ts_code
    params.update(kwargs)
    try:
        df = get_tushare_data('margin_detail', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching margin_detail data: {e}")
        return pd.DataFrame(), 'error'

def get_fz_trade(trade_date=None, ts_code=None, **kwargs):
    """获取转融资交易汇总数据"""
    params = {}
    if trade_date:
        params['trade_date'] = trade_date
    if ts_code:
        params['ts_code'] = ts_code
    params.update(kwargs)
    try:
        df = get_tushare_data('fz_trade', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching fz_trade data: {e}")
        return pd.DataFrame(), 'error'

def get_moneyflow(trade_date=None, ts_code=None, **kwargs):
    """获取个股资金流向数据"""
    params = {}
    if trade_date:
        params['trade_date'] = trade_date
    if ts_code:
        params['ts_code'] = ts_code
    params.update(kwargs)
    try:
        df = get_tushare_data('moneyflow', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching moneyflow data: {e}")
        return pd.DataFrame(), 'error'

def get_hsgt_top10(trade_date=None, ts_code=None, **kwargs):
    """获取沪深港通资金流向数据"""
    params = {}
    if trade_date:
        params['trade_date'] = trade_date
    if ts_code:
        params['ts_code'] = ts_code
    params.update(kwargs)
    try:
        df = get_tushare_data('hsgt_top10', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching hsgt_top10 data: {e}")
        return pd.DataFrame(), 'error'

# ===== 新增接口 - 按公告日期/报告日期遍历 (DateArchiver) =====


def get_index_classify(**kwargs):
    """获取申万行业分类（分级）"""
    params = {}
    params.update(kwargs)
    try:
        df = get_tushare_data('index_classify', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching index_classify data: {e}")
        return pd.DataFrame(), 'error'


def get_stk_holdertrade(ann_date=None, ts_code=None, **kwargs):
    """获取股东增减持数据"""
    params = {}
    if ann_date:
        params['ann_date'] = ann_date
    if ts_code:
        params['ts_code'] = ts_code
    params.update(kwargs)
    try:
        df = get_tushare_data('stk_holdertrade', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching stk_holdertrade data: {e}")
        return pd.DataFrame(), 'error'

def get_report_rc(report_date=None, ts_code=None, **kwargs):
    """获取券商盈利预测数据"""
    params = {}
    if report_date:
        params['report_date'] = report_date
    if ts_code:
        params['ts_code'] = ts_code
    params.update(kwargs)
    try:
        df = get_tushare_data('report_rc', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching report_rc data: {e}")
        return pd.DataFrame(), 'error'

# ===== 新增接口 - 按股票代码遍历 (StockDrivenArchiver) =====

def get_index_weight(index_code: str, trade_date: str, **kwargs):
    """获取指数成分和权重"""
    # Tushare接口对于月度数据，建议用当月的最后一天作为trade_date
    params = {
        'index_code': index_code,
        'start_date': trade_date, # 使用同一个月末日期作为开始和结束
        'end_date': trade_date
    }
    params.update(kwargs)
    try:
        df = get_tushare_data('index_weight', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching index_weight for {index_code} on {trade_date}: {e}")
        return pd.DataFrame(), 'error'


def get_index_daily(ts_code: str, **kwargs):
    """获取指数日线行情"""
    params = {
        'ts_code': ts_code
    }
    params.update(kwargs)
    try:
        df = get_tushare_data('index_daily', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching index_daily for {ts_code}: {e}")
        return pd.DataFrame(), 'error'


def get_stk_holdernumber(ts_code=None, ann_date=None, **kwargs):
    """获取股东人数数据"""
    params = {}
    if ts_code:
        params['ts_code'] = ts_code
    if ann_date:
        params['ann_date'] = ann_date
    params.update(kwargs)
    try:
        df = get_tushare_data('stk_holdernumber', **params)
        return df, 'success'
    except Exception as e:
        print(f"Error fetching stk_holdernumber data: {e}")
        return pd.DataFrame(), 'error'


def get_index_basic(**kwargs):
    """获取所有市场的指数基本信息"""
    markets = ['MSCI', 'CSI', 'SSE', 'SZSE', 'CICC', 'SW', 'OTH']
    all_indices = []
    print("Fetching index basic info for all markets...")
    for market in markets:
        try:
            print(f"  - Fetching from market: {market}")
            df = get_tushare_data('index_basic', market=market, **kwargs)
            if not df.empty:
                all_indices.append(df)
        except Exception as e:
            print(f"Could not fetch data for market {market}: {e}")

    if not all_indices:
        print("Warning: No index data was fetched.")
        return pd.DataFrame(), 'success' # Return success with empty df

    full_df = pd.concat(all_indices, ignore_index=True).drop_duplicates(subset=['ts_code'])
    print(f"Total unique indices fetched: {len(full_df)}")
    return full_df, 'success'







