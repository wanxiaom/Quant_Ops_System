@echo off
chcp 65001 >nul
echo ===================================================
echo   Quant Ops System - Windows Agent 一键部署工具
echo ===================================================

:: 获取当前路径和 Python 路径
set "AGENT_SCRIPT=%~dp0windows_agent.py"
set "PYTHON_EXE=python"

:: 检查 Python 是否在环境变量中
%PYTHON_EXE% --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 找不到 Python 解释器，请确保 Python 已添加到系统环境变量 PATH 中。
    pause
    exit /b
)

:: 检查 agent 脚本是否存在
if not exist "%AGENT_SCRIPT%" (
    echo [错误] 找不到 windows_agent.py 脚本: %AGENT_SCRIPT%
    pause
    exit /b
)

echo [1/3] 准备注册系统计划任务...
echo        - 任务名称: QuantOps_WindowsAgent
echo        - 触发条件: 当前用户登录时自动启动 (解决 WindPy Session 0 隔离问题)
echo        - 运行权限: 最高权限

:: 删除旧的同名任务（如果存在）
schtasks /delete /tn "QuantOps_WindowsAgent" /f >nul 2>&1

:: 创建新的计划任务
:: /sc onlogon 表示在用户登录时启动
:: /rl highest 表示以最高权限运行
:: /tr 执行运行 Python 脚本的命令
schtasks /create /tn "QuantOps_WindowsAgent" /tr "\"%PYTHON_EXE%\" \"%AGENT_SCRIPT%\"" /sc onlogon /rl highest /f

if %errorlevel% equ 0 (
    echo.
    echo [2/3] ✅ 守护进程任务部署成功！
    echo        Agent 现在会在您每次登录 Windows 时自动在后台静默启动。
    echo        并且它拥有完整的桌面交互权限，完美支持 WindPy API。
    echo.
    
    set /p start_now="[3/3] 是否立即启动该 Agent? (y/n): "
    if /i "%start_now%"=="y" (
        schtasks /run /tn "QuantOps_WindowsAgent"
        echo 🚀 Agent 已在后台启动！您可以在 Manager 管理端查看其心跳状态。
    )
) else (
    echo.
    echo [错误] 部署失败，请尝试右键以 "管理员身份运行" 此脚本。
)

echo.
pause
