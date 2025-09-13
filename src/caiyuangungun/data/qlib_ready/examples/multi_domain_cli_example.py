"""多域CLI使用示例

本脚本展示如何使用新的多域CLI管理器处理不同类型的数据。
"""

import subprocess
import sys
from pathlib import Path
from datetime import date, datetime

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def run_cli_command(command_args, description=""):
    """运行CLI命令并显示结果"""
    if description:
        print(f"\n=== {description} ===")
    
    print(f"执行命令: {' '.join(command_args)}")
    
    try:
        result = subprocess.run(
            command_args,
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.stdout:
            print("输出:")
            print(result.stdout)
        
        if result.stderr:
            print("错误:")
            print(result.stderr)
        
        return result.returncode == 0
    
    except Exception as e:
        print(f"命令执行失败: {e}")
        return False


def example_quotes_processing():
    """行情数据处理示例"""
    print("\n" + "="*50)
    print("行情数据处理示例")
    print("="*50)
    
    # 数据根目录
    data_root = "/path/to/your/data"  # 请替换为实际路径
    
    # CLI模块路径
    cli_module = "src.caiyuangungun.data.qlib_ready.cli.manager"
    
    # 1. 查看帮助信息
    run_cli_command([
        sys.executable, "-m", cli_module, 
        "quotes", "--help"
    ], "查看行情数据CLI帮助")
    
    # 2. 列出可用股票（前20个）
    run_cli_command([
        sys.executable, "-m", cli_module,
        "--data-root", data_root,
        "quotes", "list", "--limit", "20"
    ], "列出可用股票代码")
    
    # 3. 处理指定日期范围的数据
    run_cli_command([
        sys.executable, "-m", cli_module,
        "--data-root", data_root,
        "--verbose",
        "quotes", "process",
        "--start-date", "2024-01-01",
        "--end-date", "2024-01-31",
        "--symbols", "000001.SZ", "000002.SZ"
    ], "处理指定股票的一月份数据")
    
    # 4. 验证数据格式
    run_cli_command([
        sys.executable, "-m", cli_module,
        "--data-root", data_root,
        "quotes", "validate",
        "--symbols", "000001.SZ", "000002.SZ"
    ], "验证数据格式")
    
    # 5. 强制重新处理数据
    run_cli_command([
        sys.executable, "-m", cli_module,
        "--data-root", data_root,
        "quotes", "process",
        "--start-date", "2024-01-01",
        "--end-date", "2024-01-07",
        "--force"
    ], "强制重新处理一周数据")


def example_traditional_cli():
    """传统CLI使用示例（向后兼容）"""
    print("\n" + "="*50)
    print("传统CLI使用示例（向后兼容）")
    print("="*50)
    
    # 通过主项目CLI调用
    run_cli_command([
        sys.executable, "cli.py", 
        "qlib", "--help"
    ], "通过主项目CLI查看帮助")
    
    # 直接调用传统CLI
    run_cli_command([
        sys.executable, "-m", 
        "src.caiyuangungun.data.qlib_ready.cli.cli",
        "process", "--help"
    ], "直接调用传统CLI")


def example_api_usage():
    """API使用示例"""
    print("\n" + "="*50)
    print("Python API使用示例")
    print("="*50)
    
    try:
        from src.caiyuangungun.data.qlib_ready import (
            QlibReadyDataManager,
            QlibDataProcessor,
            QlibFormatValidator
        )
        from src.caiyuangungun.contracts import InterfaceType
        
        print("✓ 成功导入QLIB-READY模块")
        
        # 初始化管理器
        data_root = "/path/to/your/data"  # 请替换为实际路径
        manager = QlibReadyDataManager(data_root)
        print(f"✓ 初始化数据管理器: {data_root}")
        
        # 获取可用股票列表
        try:
            symbols = manager.list_symbols(InterfaceType.QUOTES_DAILY)
            print(f"✓ 找到 {len(symbols)} 个可用股票")
            if symbols:
                print(f"  前5个股票: {symbols[:5]}")
        except Exception as e:
            print(f"⚠ 获取股票列表失败: {e}")
        
        # 初始化处理器
        processor = QlibDataProcessor()
        print("✓ 初始化数据处理器")
        
        # 初始化验证器
        validator = QlibFormatValidator()
        print("✓ 初始化格式验证器")
        
        print("\n所有组件初始化成功！")
        
    except ImportError as e:
        print(f"✗ 导入失败: {e}")
        print("请确保项目路径正确")
    except Exception as e:
        print(f"✗ API使用失败: {e}")


def example_extension():
    """扩展示例：如何添加新的数据域"""
    print("\n" + "="*50)
    print("扩展示例：添加新数据域")
    print("="*50)
    
    print("""
要添加新的数据域（如财务数据），需要以下步骤：

1. 在 processors/ 目录下创建新的数据域目录：
   mkdir -p processors/financials
   
2. 创建处理器类：
   # processors/financials/processor.py
   from ...core.base_processor import BaseQlibProcessor
   
   class FinancialProcessor(BaseQlibProcessor):
       @property
       def feature_mapping(self):
           return {
               'revenue': '$revenue',
               'profit': '$profit'
           }
       
       def process_data(self, data):
           # 实现财务数据处理逻辑
           pass

3. 创建管理器类：
   # processors/financials/manager.py
   from ...core.base_manager import BaseQlibManager
   
   class FinancialManager(BaseQlibManager):
       @property
       def supported_interface_types(self):
           return [InterfaceType.FIN_IS, InterfaceType.FIN_BS]

4. 创建CLI处理器：
   # cli/financials_cli.py
   from .manager import BaseDomainCLI
   
   class FinancialsCLI(BaseDomainCLI):
       @property
       def domain_name(self):
           return "financials"

5. 注册到CLI管理器：
   # 在 cli/manager.py 的 _register_default_handlers 方法中添加
   from .financials_cli import FinancialsCLI
   self.register_domain_handler(FinancialsCLI())

6. 使用新的数据域：
   python -m src.caiyuangungun.data.qlib_ready.cli.manager financials process
""")


def main():
    """主函数"""
    print("QLIB-READY 多域CLI使用示例")
    print("="*60)
    
    # 检查Python版本
    print(f"Python版本: {sys.version}")
    print(f"工作目录: {Path.cwd()}")
    
    # 运行各种示例
    example_api_usage()
    example_quotes_processing()
    example_traditional_cli()
    example_extension()
    
    print("\n" + "="*60)
    print("示例运行完成！")
    print("\n注意事项:")
    print("1. 请将 '/path/to/your/data' 替换为实际的数据根目录")
    print("2. 确保NORM层数据已经准备就绪")
    print("3. 首次运行可能需要较长时间进行数据处理")
    print("4. 使用 --verbose 参数可以查看详细的处理过程")


if __name__ == "__main__":
    main()