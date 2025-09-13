"""
Audit Bridge (Norm)

职责：
- 将 Norm 阶段的校验与审计引擎打通
- 统一输出审计记录（JSON），并落地到 decisions/ 与 processing_logs/

TODO:
- 规范 audit payload：规则ID、级别、类别、上下文、统计
- 统一的save_audit_report接口（由Manager或Pipeline调用）
"""