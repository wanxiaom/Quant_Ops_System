import subprocess
import requests
import time
import psutil
import socket
import os
import sys  # 💡 新增引入：用于获取当前 Python 解释器的绝对路径
from pathlib import Path

class WindowsAgent:
    def __init__(self, manager_url, node_id, token="quant_ops_secret_2026"):
        self.manager = manager_url.rstrip('/')
        self.node_id = node_id
        self.session = requests.Session()
        
        # 强制忽略 Windows 系统代理 (如 Clash/V2Ray/Fiddler 等)
        # 代理软件经常会拦截局域网请求或吞掉 Authorization 请求头，导致 401
        self.session.trust_env = False
        self.session.proxies = {"http": None, "https": None}

        # 1. 显式定义所有请求都需要使用的 Headers
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # 2. 动态获取当前项目的根目录 (对应 Linux 的 quant_ops_system)
        # 当前脚本在 src/agent/ 下，所以向上三级就是项目的根目录 (如 C:\Users\your_username\Desktop)
        self.base_dir = Path(__file__).resolve().parent.parent.parent
        
    def _get_running_subprocesses(self):
        # 此处可扩展：跟踪通过 Agent 拉起的子进程
        return []
        
    def _get_heartbeat_payload(self):
        # 💡 增强：补充 version 和 tags 字段，与前端和 Manager 契约对齐
        return {
            "node_id": self.node_id,
            "ip": socket.gethostbyname(socket.gethostname()),
            "os_type": "windows",
            "version": "agent-0.3.1",
            "tags": ["wind", "cj-connector"],
            "last_heartbeat": time.time(),
            "cpu_load": psutil.cpu_percent(interval=1),
            "mem_usage": psutil.virtual_memory().percent,
            "running_tasks": self._get_running_subprocesses()
        }
    
    def _heartbeat_loop(self, interval=15):
        while True:
            try:
                resp = self.session.post(f"{self.manager}/api/agents/heartbeat",
                                         json=self._get_heartbeat_payload(), headers=self.headers, timeout=10)
                if resp.status_code != 200:
                    print(f"[{time.strftime('%H:%M:%S')}] Heartbeat failed, status: {resp.status_code}, response: {resp.text}")
                else:
                    print(f"[{time.strftime('%H:%M:%S')}] Heartbeat successful.")
            except Exception as e:
                print(f"[{time.strftime('%H:%M:%S')}] Heartbeat error: {e}")
            time.sleep(interval)
    
    def _execute_task(self, task):
        # 兼容规范路径脚本或直接的 Shell 命令 (适配 Manager 的 seed_demo_task 测试)
        if "script_path" in task:
            raw_script_path = task["script_path"]
            # 路径转换：如果是 Manager 下发的 Linux 绝对路径，去掉它的前缀
            linux_prefix = "/home/wanxm/ops_maintenance/quant_ops_system/"
            if raw_script_path.startswith(linux_prefix):
                raw_script_path = raw_script_path[len(linux_prefix):]
            
            raw_script_path = raw_script_path.lstrip('/')
            # 将处理后的相对路径拼接上 Windows 的基准路径
            script_path = self.base_dir / Path(raw_script_path)
            
            if not script_path.exists():
                return {"exit_code": -1, "stderr": f"Script {script_path} not found"}
            
            # 🔥【核心修复 1】动态截获解释器，如果是 python 系列命令，一律强制替换为当前虚拟环境的绝对路径
            interpreter = task.get("interpreter", "python")
            if "python" in interpreter.lower():
                interpreter_path = sys.executable
            else:
                interpreter_path = interpreter.replace("python3", "python")
            
            cmd_params = []
            # 💡 核心修复：确保这里的 params 处理逻辑与 linux_agent.py 完全一致
            for key, value in task.get("params", {}).items():
                arg_key = f"--{key.replace('_', '-')}"
                if isinstance(value, bool):
                    if value:
                        cmd_params.append(arg_key)
                elif isinstance(value, list):
                    for item in value:
                        cmd_params.extend([arg_key, str(item)])
                else:
                    cmd_params.extend([arg_key, str(value)])
            
            cmd = [interpreter_path, str(script_path)] + cmd_params
            use_shell = False
            print(f"Executing: {' '.join(cmd)}")
        elif "command" in task or "script" in task:
            cmd = task.get("command") or task.get("script")
            # 如果是纯命令模式，执行粗暴的全局路径和命令替换
            cmd = cmd.replace("/home/wanxm/ops_maintenance/quant_ops_system", str(self.base_dir).replace('\\', '/'))
            
            # 🔥【核心修复 2】在纯 Shell 脚本替换时，精准将 python 关键字打上绝对路径的双引号，防止路径空格断裂
            cmd = cmd.replace("python3 ", f'"{sys.executable}" ')
            cmd = cmd.replace("python ", f'"{sys.executable}" ')
            use_shell = True
            print(f"Executing: {cmd}")
        else:
            return {"exit_code": -1, "stderr": "No script_path or command provided in task"}
            
        # 修复：弃用 text=True，改为手动以 UTF-8 解码，确保与 C++ 后端和脚本侧的编码一致
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=use_shell)
        try:
            stdout_bytes, stderr_bytes = proc.communicate(timeout=task.get("timeout_sec", 3600))
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout_bytes, stderr_bytes = proc.communicate()
            stdout = stdout_bytes.decode('utf-8', errors='replace')
            stderr = "Timeout expired after " + str(task.get("timeout_sec", 3600)) + " seconds.\n" + stderr_bytes.decode('utf-8', errors='replace')
            return {"exit_code": -2, "stdout": stdout, "stderr": stderr}
            
        # 修复：采用安全的“先解码再截断”策略，防止破坏多字节字符
        def safe_decode_and_truncate(byte_string, max_len=20000):
            if not byte_string:
                return ""
            try:
                decoded_str = byte_string.decode('utf-8')
            except UnicodeDecodeError:
                decoded_str = byte_string.decode('utf-8', errors='replace')
            return decoded_str[-max_len:]

        stdout = safe_decode_and_truncate(stdout_bytes)
        stderr = safe_decode_and_truncate(stderr_bytes)
        return {"exit_code": proc.returncode, "stdout": stdout, "stderr": stderr}
    
    def _task_pull_loop(self):
        while True:
            try:
                # 1. 发起长轮询拉取任务 
                resp = self.session.post(
                    f"{self.manager}/api/tasks/poll",
                    json={"node_id": self.node_id},
                    headers=self.headers,
                    timeout=65  
                )
                if resp.status_code == 200:
                    resp_json = resp.json()
                    data = resp_json.get("data")
                    if data and "task" in data and data["task"]:
                        task = data["task"]
                        print(f"\n[{time.strftime('%H:%M:%S')}] Received task: {task.get('exec_id')}")
                        
                        # 2. 执行任务
                        result = self._execute_task(task)

                        stdout = result.get("stdout", "")
                        stderr = result.get("stderr", "")
                        if stdout:
                            print(stdout)
                        if stderr:
                            print(stderr)

                        # 与 Linux Agent 保持一致：将 Windows 脚本输出上传到 Manager，
                        # 便于 ws_client 和任务日志直接看到成功/失败原因。
                        logs = []
                        logs.extend(
                            {"type": "stdout", "content": line}
                            for line in stdout.splitlines()
                        )
                        logs.extend(
                            {"type": "stderr", "content": line}
                            for line in stderr.splitlines()
                        )
                        if logs:
                            log_resp = self.session.post(
                                f"{self.manager}/api/tasks/log",
                                json={"exec_id": task["exec_id"], "logs": logs},
                                headers=self.headers,
                                timeout=10,
                            )
                            log_body = log_resp.json()
                            if log_resp.status_code != 200 or log_body.get("code") != 0:
                                print(
                                    f"[{time.strftime('%H:%M:%S')}] Failed to upload task logs: "
                                    f"status {log_resp.status_code}, response: {log_resp.text}"
                                )
                        
                        if result.get("exit_code") != 0:
                            print(f"[{time.strftime('%H:%M:%S')}] Task Failed with exit code {result.get('exit_code')}")
                            if result.get("stderr"):
                                print(f"Error details:\n{result.get('stderr')}")
                        
                        # 3. 上报执行结果 Callback
                        callback_payload = {
                            "exec_id": task["exec_id"],
                            "exit_code": result.get("exit_code", -999),
                            "stdout": result.get("stdout", ""),
                            "stderr": result.get("stderr", "")
                        }
                        cb_resp = self.session.post(
                            f"{self.manager}/api/tasks/callback",
                            json=callback_payload,
                            headers=self.headers,
                            timeout=10
                        )
                        cb_body = cb_resp.json()
                        if cb_resp.status_code == 200 and cb_body.get("code") == 0:
                            print(f"[{time.strftime('%H:%M:%S')}] Task {task['exec_id']} result reported successfully.")
                        else:
                            print(f"[{time.strftime('%H:%M:%S')}] Failed to report task result: status {cb_resp.status_code}, response: {cb_resp.text}")
            except requests.exceptions.RequestException:
                time.sleep(3)
            except Exception as e:
                print(f"[{time.strftime('%H:%M:%S')}] Task pull error: {e}")
                time.sleep(5)
    
    def run(self):
        import threading
        print(f"Starting Windows Agent [{self.node_id}] connected to {self.manager}")
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        self._task_pull_loop()

if __name__ == "__main__":
    # 替换为真实的 Linux 服务器 Manager IP
    MANAGER_URL = os.environ.get("MANAGER_URL", "${MANAGER_URL}")
    NODE_ID = os.environ.get("NODE_ID", "${NODE_ID}")
    TOKEN = os.environ.get("ACCESS_TOKEN", "${ACCESS_TOKEN}")
    agent = WindowsAgent(MANAGER_URL, NODE_ID, TOKEN)
    agent.run()
