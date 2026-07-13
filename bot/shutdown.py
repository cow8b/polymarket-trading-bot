"""进程级停机协调：首次信号优雅退出，再次信号立即终止。"""
from __future__ import annotations

import os
import signal
import threading
import time
from typing import Any, Callable, Optional

from loguru import logger

_signal_count = 0
_signal_lock = threading.Lock()


def install_shutdown_signal_handlers() -> None:
    """在主线程安装 SIGINT/SIGTERM 两阶段停机处理器。"""
    if threading.current_thread() is not threading.main_thread():
        return

    def _handle(signum, _frame) -> None:
        global _signal_count
        with _signal_lock:
            _signal_count += 1
            count = _signal_count
        if count >= 2:
            os._exit(130)
        name = signal.Signals(signum).name
        logger.warning(f"收到 {name}，开始优雅停机；再次按 Ctrl+C 将立即退出")
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _handle)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle)


def _call_bounded(label: str, fn: Callable[[], Any], timeout: float) -> bool:
    """在守护线程中执行可能阻塞的停机调用。"""
    error: list[BaseException] = []

    def _run() -> None:
        try:
            fn()
        except BaseException as exc:  # 停机路径不能因第三方异常中断
            error.append(exc)

    thread = threading.Thread(target=_run, name=f"shutdown-{label}", daemon=True)
    thread.start()
    thread.join(timeout=max(0.0, timeout))
    if thread.is_alive():
        logger.warning(f"{label} 超过 {timeout:.1f} 秒仍未返回")
        return False
    if error:
        logger.warning(f"{label} 失败: {error[0]}")
        return False
    return True


def shutdown_node(
    node: Any,
    thread: Optional[threading.Thread] = None,
    *,
    timeout: float = 10.0,
) -> None:
    """在总预算内停止、等待并释放 Nautilus 节点；重复调用安全。"""
    if node is None or getattr(node, "_bonereaper_shutdown_started", False):
        return
    try:
        setattr(node, "_bonereaper_shutdown_started", True)
    except Exception:
        pass

    deadline = time.monotonic() + max(1.0, timeout)
    stop = getattr(node, "stop", None)
    running_state = getattr(node, "is_running", True)
    try:
        is_running = bool(running_state() if callable(running_state) else running_state)
    except Exception:
        is_running = True
    stop_completed = True
    if callable(stop) and is_running:
        stop_completed = _call_bounded(
            "Nautilus stop",
            stop,
            min(7.0, max(0.5, deadline - time.monotonic())),
        )

    if thread is not None and thread.is_alive():
        thread.join(timeout=min(4.0, max(0.0, deadline - time.monotonic())))
        if thread.is_alive():
            logger.warning("Nautilus 运行线程未在停机预算内结束")

    # stop() 尚在后台运行时调用 dispose() 会与 Nautilus 的任务取消流程竞争，
    # 容易出现 Future cancelled 回调错误。此时直接结束主流程，由进程回收资源。
    dispose = getattr(node, "dispose", None)
    if callable(dispose) and stop_completed:
        _call_bounded(
            "Nautilus dispose",
            dispose,
            min(2.0, max(0.25, deadline - time.monotonic())),
        )
