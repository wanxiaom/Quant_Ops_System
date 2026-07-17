#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import requests
import threading
import subprocess
import socket
import queue
import psutil # 💡 新增：引入 psutil 库用于获取系统真实负载
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

MANAGER_URL = os.environ.get("MANAGER_URL", "${MANAGER_URL}")
NODE_ID = os.environ.get("NODE_ID", "${NODE_ID}")
TOKEN = os.environ.get("ACCESS_TOKEN", "${ACCESS_TOKEN}")
MAX_CONCURRENT_TASKS = 4  # 最大并发执行任务数
POLL_INTERVAL_SECONDS = 5 # Agent 轮询 Manager 的间隔时间，单位秒
HEARTBEAT_INTERVAL_SECONDS = 30 # Agent 发送心跳的间隔时间，单位秒
LOG_BATCH_FLUSH_INTERVAL_SECONDS = 5 # Agent 强制刷新日志批次的间隔时间，单位秒

# 核心：所有请求默认带上 Token 鉴权头
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {TOKEN}"
}

# 使用 Session 复用 TCP 连接；Manager 是本机服务，禁用环境代理，避免代理拦截 。
session = requests.Session()
session.trust_env = False
session.proxies = {"http": None, "https": None}

# 基于当前 Agent 所在目录，定位脚本执行的根路径（由于现在放在 src/agent/ 中，所以需要向上两级）
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

def get_primary_ip():
    """
    获取本机主要的对外IP地址。
    1. 尝试连接外部地址来判断。
    2. 如果失败，则遍历网卡信息获取。
    """
    try:
        # 1. 尝试连接一个公共DNS服务器（不会真的发送数据）
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("127.0.0.1", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except OSError:
        # 2. 如果连接失败（如无外网），则遍历网卡
        for _, addrs in psutil.net_if_addrs().items():
            for addr in addrs:
                if addr.family == socket.AF_INET and not addr.address.startswith("127."):
                    return addr.address
    return "127.0.0.1" # 如果所有方法都失败，返回本地回环地址


def heartbeat_loop():
    """后台守护线程：每 15 秒向 Manager 发送一次心跳，防止被判定为离线"""
    while True:
        try:
            # 💡 增强：补充 version 和 tags 字段，与前端和 Manager 契约对齐
            tags = ["rqdata", "dolphindb"] if NODE_ID == "node_001" else ["research"]
            payload = {
                "node_id": NODE_ID,
                "ip": get_primary_ip(), # 💡 修复：使用新的函数智能获取IP
                "os_type": "linux",
                "last_heartbeat": time.time(),
                "version": "agent-0.3.1",
                "tags": tags, # 💡 修复：使用 psutil 获取真实的 CPU 和内存使用率
                "cpu_load": psutil.cpu_percent(interval=1),
                "mem_usage": psutil.virtual_memory().percent
            }
            session.post(f"{MANAGER_URL}/api/agents/heartbeat", json=payload, headers=HEADERS, timeout=5)
        except Exception:
            pass # 心跳失败默默忽略，等待下一次重试
        time.sleep(15)

def read_stream(stream, q, stream_type):
    """异步读取流并放入线程安全队列"""
    for line in iter(stream.readline, ''):
        q.put({"type": stream_type, "content": line.rstrip('\n')})
    stream.close()

def execute_task(task):
    """在独立线程中执行单一任务的逻辑"""
    exec_id = task["exec_id"]
    script_path = task["script_path"]
    full_script_path = os.path.join(BASE_DIR, script_path)
    timeout_sec = task.get("timeout_sec", 3600)
    params = task.get('params', {})
    # 核心修复：用于在回调时聚合所有日志
    full_stdout = []
    full_stderr = []
    exit_code = -1
    
    if os.path.exists(full_script_path):
        # 将 params 字典转换为命令行参数列表
        cmd_params = []
        for key, value in params.items():
            # 将字典的 key 转换为命令行参数形式，例如 "start_date" -> "--start-date"
            arg_key = f"--{key.replace('_', '-')}"
            if isinstance(value, bool):
                # 如果是布尔值且为 True，仅添加 flag（适配 argparse 的 action='store_true'）
                if value:
                    cmd_params.append(arg_key)
            elif isinstance(value, list):
                # 如果值是列表，为每个元素添加一个同名参数
                for item in value:
                    cmd_params.extend([arg_key, str(item)])
            else:
                # 否则，直接添加 key-value 对
                cmd_params.extend([arg_key, str(value)])
        
        command = ["python3", full_script_path] + cmd_params

        print(f"   🏃 [{exec_id}] Executing command: {' '.join(command)}")
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8', 
            errors='replace'
        )
        start_time = time.time()
        
        # 初始化日志队列与异步读取线程
        log_queue = queue.Queue()
        threading.Thread(target=read_stream, args=(process.stdout, log_queue, "stdout"), daemon=True).start()
        threading.Thread(target=read_stream, args=(process.stderr, log_queue, "stderr"), daemon=True).start()
        
        log_batch = []
        last_log_send_time = time.time()
        
        def flush_logs():
            nonlocal log_batch, last_log_send_time # 声明使用外部变量
            if not log_batch: return
            
            # [测试验证] 在终端直观展示攒批发送的过程
            print(f"   📦 [{exec_id}] Batching {len(log_batch)} lines of logs, preparing to send...")
            
            try:
                session.post(f"{MANAGER_URL}/api/tasks/log", json={"exec_id": exec_id, "logs": log_batch}, headers=HEADERS, timeout=5)
            except Exception:
                pass
            log_batch = []
            last_log_send_time = time.time()
        
        # 循环进行非阻塞的状态检查
        while True:
            # 进程已结束且所有输出已读取完毕，或强制刷新间隔已到，则退出循环
            if process.poll() is not None and log_queue.empty() and not (log_batch and time.time() - last_log_send_time >= LOG_BATCH_FLUSH_INTERVAL_SECONDS):
                flush_logs() # 发送最后一次日志
                exit_code = process.poll()
                print(f"   ✅ [{exec_id}] Finished with exit code: {exit_code}")
                break

            # 1. 抽取当前队列中暂存的日志
            while not log_queue.empty():
                try:
                    log_batch.append(log_queue.get_nowait())
                    # 核心修复：同时将日志聚合到完整输出变量中
                    log_item = log_batch[-1]
                    if log_item['type'] == 'stdout': full_stdout.append(log_item['content'])
                    else: full_stderr.append(log_item['content'])
                except queue.Empty:
                    break
            
            # 2. 满足攒批条件（满 50 行 或 间隔 1 秒有新日志）则打包发送
            if len(log_batch) >= 50 or (log_batch and time.time() - last_log_send_time >= LOG_BATCH_FLUSH_INTERVAL_SECONDS):
                flush_logs()
            
            exit_code = process.poll() # 检查子进程是否已退出
                
            if time.time() - start_time > timeout_sec:
                print(f"   ⏳ [{exec_id}] Task timed out after {timeout_sec}s! Killing process...")
                process.kill() # 发送 SIGKILL 强杀进程
                process.wait() # 等待进程完全退出，防止产生僵尸进程
                exit_code = -2 # 退出码置为 -2，代表 Timeout
                print(f"   💀 [{exec_id}] Process killed.")
                break
                
            time.sleep(1) # 短暂休眠，避免死循环打满 CPU
    else:
        print(f"   ❌ [{exec_id}] Script not found: {full_script_path}")

    # 结果回调
    if exit_code is None:
        if 'process' in locals():
            exit_code = process.poll()
        if exit_code is None:
            exit_code = -1

    # 核心修复：将聚合后的完整日志内容合并为字符串
    final_stdout = "\n".join(full_stdout)
    final_stderr = "\n".join(full_stderr)

    try:
        callback_resp = session.post(
            f"{MANAGER_URL}/api/tasks/callback",
            json={
                "exec_id": exec_id, 
                "exit_code": int(exit_code),
                "stdout": final_stdout,
                "stderr": final_stderr
            },
            headers=HEADERS,
            timeout=10,
        )
        if callback_resp.status_code == 200 and callback_resp.json().get("code") == 0:
            print(f"   📤 [{exec_id}] Callback sent successfully.")
        else:
            print(
                f"   ❌ [{exec_id}] Callback rejected: "
                f"status={callback_resp.status_code}, response={callback_resp.text}"
            )
    except Exception as e:
        print(f"   ❌ [{exec_id}] Callback failed: {e}")

def main():
    print(f"🚀 Starting Python Agent ({NODE_ID}) connecting to {MANAGER_URL} ...")
    
    # 启动后台心跳线程
    hb_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    hb_thread.start()

    # 初始化线程池和活跃任务追踪列表
    executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_TASKS)
    active_futures = []

    # 主线程：任务拉取与分发循环
    while True:
        try:
            # 清理已完成的 Future
            active_futures = [f for f in active_futures if not f.done()]
            
            # 若并发数已达上限，暂停拉取新任务
            if len(active_futures) >= MAX_CONCURRENT_TASKS:
                time.sleep(2)
                continue

            poll_res = session.post(f"{MANAGER_URL}/api/tasks/poll", json={"node_id": NODE_ID}, headers=HEADERS, timeout=65)
            
            if poll_res.status_code == 401:
                print("🚨 Unauthorized! Token is invalid or missing.")
                time.sleep(10)
                continue
                
            if poll_res.status_code == 200:
                data = poll_res.json().get("data", {})
                task = data.get("task")
                if task:
                    exec_id = task["exec_id"]
                    script_path = task["script_path"]
                    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 📥 Received task: {exec_id} -> {script_path}")
                    
                    # 将执行逻辑提交到线程池中异步运行，并将 Future 存入活跃列表
                    future = executor.submit(execute_task, task)
                    active_futures.append(future)
        except Exception as e:
            time.sleep(5)
        time.sleep(2)

if __name__ == "__main__":
    main()
