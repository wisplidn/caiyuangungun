"""
Processor Registry（Norm）

职责：
- 维护 InterfaceType -> Processor 的映射
- 暴露 get_processor(interface_type) 与可扩展注册接口

TODO:
- 与 contracts.InterfaceType 对齐
- 注册：quotes_daily, fin_is, fin_bs, fin_cf, analyst, corp_actions, universe, calendar, ref
"""
# --- Minimal runnable implementation ---
from typing import Dict, Type, Tuple, Optional, Union

from caiyuangungun.contracts import InterfaceType, DataSource
from caiyuangungun.data.norm.processors.quotes.quotes_daily_processor import QuotesDailyProcessor
from caiyuangungun.data.norm.processors.financials.fin_bs_processor import FinBSProcessor
from caiyuangungun.data.norm.processors.analyst.analyst_processor import AnalystProcessor
from caiyuangungun.data.norm.processors.corp_actions.corp_actions_processor import CorpActionsProcessor
from caiyuangungun.data.norm.core.utils.universe_processor import UniverseProcessor
from caiyuangungun.data.norm.processors.calendar.calendar_processor import CalendarProcessor
from caiyuangungun.data.norm.processors.ref.ref_processor import RefProcessor


# 支持两种Key：
# - InterfaceType（默认）
# - (InterfaceType, DataSource)（来源特化）
_REGISTRY: Dict[Union[InterfaceType, Tuple[InterfaceType, DataSource]], Type] = {
    InterfaceType.QUOTES_DAILY: QuotesDailyProcessor,
    # 临时：将 FINANCIALS 映射到 FinBSProcessor（占位）。后续可引入组合处理器协调 IS/BS/CF。
    InterfaceType.FINANCIALS: FinBSProcessor,
}


def register(interface_type: InterfaceType, processor_cls: Type, source: Optional[DataSource] = None) -> None:
    """注册/覆盖处理器。
    - 不指定 source：注册为接口默认处理器
    - 指定 source：注册为该来源的特化处理器
    """
    if source is None:
        _REGISTRY[interface_type] = processor_cls
    else:
        _REGISTRY[(interface_type, source)] = processor_cls


def get_processor(interface_type: InterfaceType, source: Optional[DataSource] = None):
    """根据接口类型（与可选来源）获取处理器实例。
    优先匹配 (interface_type, source)，否则回退到 interface_type 默认实现。
    """
    if source is not None:
        key = (interface_type, source)
        if key in _REGISTRY:
            return _REGISTRY[key]()
    if interface_type not in _REGISTRY:
        raise ValueError(f"No processor registered for {interface_type}")
    return _REGISTRY[interface_type]()

# extend registry mapping
_REGISTRY.update({
    InterfaceType.ANALYST: AnalystProcessor,
    InterfaceType.CORP_ACTIONS: CorpActionsProcessor,
    InterfaceType.UNIVERSE: UniverseProcessor,
    InterfaceType.CALENDAR: CalendarProcessor,
    InterfaceType.REF: RefProcessor,
})