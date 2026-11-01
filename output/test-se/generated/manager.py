"""模块 `manager`：开发一个个人任务管理小工具"""

from typing import Any, Dict

def process_manager(data):
    """开发一个个人任务管理小工具（对应需求 F1，优先级：中）"""
    if not validate_input(data):
        raise ValueError("输入数据不合法")
    return process(data)

def validate_manager(data):
    """校验 manager 模块的输入数据合法性。"""
    if not validate_input(data):
        raise ValueError("输入数据不合法")
    return process(data)

def validate_input(data: Any) -> bool:
    """校验输入是否为非空数据。"""
    return data is not None

def process(data: Any) -> Any:
    """默认处理逻辑：原样返回数据。"""
    return data
