"""
Quality Validator (Norm)

职责：
- 数据质量检查：必填列、类型、重复、异常值、业务勾稽（财务）
- 生成质量评分，供决策器使用

TODO:
- 校验规则注册机制（接口/字段可插拔）
- 输出：report(dict)、score(float)、issues(list)
- 与 audit.AuditEngine 的调用顺序与结果合并策略
"""