"""
Parquet Reader（Norm）

TODO:
- 约定路径：data/norm/by_interface/<interface>/<year>/*.parquet
- 支持按年/按分区读取与合并（chunked）
- 提供增量读取（基于md5/time_range）
"""