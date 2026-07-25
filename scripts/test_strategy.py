"""已废弃：Phase 4 检查的目标文件在目录重构后不存在。

历史上本脚本运行 ``core/strategy_brain/test_strategy.py``；该目录已重构为
``core/strategy/`` 且未保留对应测试。策略/信号逻辑现在通过以下方式验证：

- ``python -m pytest tests/``（含 analytics / dashboard metrics 单元测试）
- ``python main.py --test-mode``（快速模拟循环，跑完整 6 步决策链）

保留本文件是为了让旧文档/脚本调用得到明确提示而非 FileNotFoundError。
"""

from __future__ import annotations

import sys


def main() -> None:
    print(__doc__)
    sys.exit(0)


if __name__ == "__main__":
    main()
