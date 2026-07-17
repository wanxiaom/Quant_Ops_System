#!/bin/bash
# Quant Ops System - Linux Agent systemd 一键部署工具

set -e

echo "==================================================="
echo "  Quant Ops System - Linux Agent 一键部署工具"
echo "==================================================="

if [ "$EUID" -ne 0 ]; then
  echo "⚠️  请使用 root 权限 (sudo) 运行此脚本！"
  exit 1
fi

AGENT_DIR=$(dirname $(readlink -f "$0"))
AGENT_SCRIPT="$AGENT_DIR/linux_agent.py"
SERVICE_FILE="/etc/systemd/system/quant_agent.service"
RUN_USER=${SUDO_USER:-$(whoami)}

if [ ! -f "$AGENT_SCRIPT" ]; then
    echo "❌ 找不到 linux_agent.py 脚本: $AGENT_SCRIPT"
    exit 1
fi

echo "👉 [1/3] 正在生成 systemd 服务配置文件..."

cat <<EOF > 
[Unit]
Description=Quant Ops Linux Agent Service
After=network.target

[Service]
Type=simple
User=$RUN_USER
WorkingDirectory=$AGENT_DIR
ExecStart=/usr/bin/python3 $AGENT_SCRIPT
Restart=always
RestartSec=10
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=quant_agent

[Install]
WantedBy=multi-user.target
EOF

echo "👉 [2/3] 重新加载 systemd 守护进程并启用服务自启..."
systemctl daemon-reload
systemctl enable quant_agent.service

echo "👉 [3/3] 启动 Quant Ops Linux Agent..."
systemctl restart quant_agent.service

echo ""
echo "✅ 部署完成！"
echo "🔍 查看运行状态: sudo systemctl status quant_agent"
echo "📜 查看后台日志: sudo journalctl -u quant_agent -f"
echo "==================================================="