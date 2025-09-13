"""
Decision Engine (Norm)

职责：
- 在主键维度进行去重与版本选择
- 策略：更新标志 > 报告类型 > 公告时间 > 质量评分

TODO:
- 决策规则的可配置化（按接口）
- 输入：df, primary_keys, policy_config, quality_scores
- 输出：df_deduped, decisions
"""