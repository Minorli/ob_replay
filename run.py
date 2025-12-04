"""
启动入口：为 main.cli 加一层简单包装，便于直接运行。

使用方式：
  python3 run.py compat --sql "select * from dual"
  python3 run.py benchmark --sql "select count(*) from t" --iterations 5
  python3 run.py advise --sql "select * from orders where customer_id=1"

配置：
- 默认从当前目录的 config.ini 读取；也可以设置环境变量 OB_TOOL_CONFIG 指向其他路径。
- 不要在代码里硬编码凭据，每次运行前更新 config.ini 或用环境变量覆盖密码。
"""

import os
import sys

from main.cli import main as cli_main


def _inject_config(args):
    if "--config" in args:
        return args
    config_path = os.environ.get("OB_TOOL_CONFIG", "config.ini")
    return ["--config", config_path] + args


if __name__ == "__main__":
    sys.exit(cli_main(_inject_config(sys.argv[1:])))
