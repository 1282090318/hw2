"""模块 `task_2`：可选：支持任务提醒"""

from typing import Any, Dict

def process_task(data):
    """可选：支持任务提醒（对应需求 F4，优先级：低）"""
    if not validate_input(data):
        raise ValueError("输入数据不合法")
    return process(data)

def validate_task(data):
    """校验 task 模块的输入数据合法性。"""
    if not validate_input(data):
        raise ValueError("输入数据不合法")
    return process(data)

def validate_input(data: Any) -> bool:
    """校验输入是否为非空数据。"""
    return data is not None

def process(data: Any) -> Any:
    """默认处理逻辑：原样返回数据。"""
    return data
