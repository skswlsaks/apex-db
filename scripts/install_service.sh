#!/bin/bash
# ============================================================================
# APEX-DB 프로덕션 서비스 설치 스크립트
# ============================================================================
# 용도: systemd 서비스, 사용자, 디렉토리, cron 설정
# 실행: sudo ./install_service.sh
# ============================================================================

set -euo pipefail

# 권한 체크
if [ "$EUID" -ne 0 ]; then
    echo "Error: This script must be run as root"
    exit 1
fi

# ============================================================================
# 설정
# ============================================================================
APEX_HOME="/opt/apex-db"
DATA_DIR="/data/apex-db"
LOG_DIR="/var/log/apex-db"
BACKUP_DIR="/backup/apex-db"

# ============================================================================
# 1. apex 사용자 생성
# ============================================================================
echo "[1/8] Creating apex user..."
if ! id -u apex > /dev/null 2>&1; then
    useradd --system --no-create-home --shell /bin/bash apex
    echo "✓ User 'apex' created"
else
    echo "✓ User 'apex' already exists"
fi

# ============================================================================
# 2. 디렉토리 생성
# ============================================================================
echo "[2/8] Creating directories..."
mkdir -p "$APEX_HOME"
mkdir -p "$DATA_DIR"/{hdb,wal,tmp}
mkdir -p "$LOG_DIR"
mkdir -p "$BACKUP_DIR"

chown -R apex:apex "$APEX_HOME"
chown -R apex:apex "$DATA_DIR"
chown -R apex:apex "$LOG_DIR"
chown -R apex:apex "$BACKUP_DIR"

echo "✓ Directories created"

# ============================================================================
# 3. 바이너리 복사
# ============================================================================
echo "[3/8] Installing binaries..."
if [ -f "./build/apex_server" ]; then
    mkdir -p "${APEX_HOME}/bin"
    cp ./build/apex_server "${APEX_HOME}/bin/"
    chmod +x "${APEX_HOME}/bin/apex_server"
    echo "✓ Binaries installed"
else
    echo "⚠ Warning: apex_server binary not found in ./build/"
fi

# ============================================================================
# 4. 설정 파일 복사
# ============================================================================
echo "[4/8] Installing config..."
if [ -f "./config.yaml" ]; then
    cp ./config.yaml "${APEX_HOME}/config.yaml"
    chown apex:apex "${APEX_HOME}/config.yaml"
    echo "✓ Config installed"
else
    echo "⚠ Warning: config.yaml not found"
fi

# ============================================================================
# 5. 스크립트 복사
# ============================================================================
echo "[5/8] Installing scripts..."
mkdir -p "${APEX_HOME}/scripts"
cp scripts/*.sh "${APEX_HOME}/scripts/"
chmod +x "${APEX_HOME}/scripts/"*.sh
chown -R apex:apex "${APEX_HOME}/scripts"
echo "✓ Scripts installed"

# ============================================================================
# 6. systemd 서비스 설치
# ============================================================================
echo "[6/8] Installing systemd service..."
cp scripts/apex-db.service /etc/systemd/system/apex-db.service
systemctl daemon-reload
systemctl enable apex-db.service
echo "✓ systemd service installed"

# ============================================================================
# 7. cron 작업 설치
# ============================================================================
echo "[7/8] Installing cron jobs..."

# EOD 프로세스 (평일 18:00)
(crontab -u apex -l 2>/dev/null || true; echo "0 18 * * 1-5 ${APEX_HOME}/scripts/eod_process.sh") | crontab -u apex -

# 백업 (매일 02:00)
(crontab -u apex -l 2>/dev/null || true; echo "0 2 * * * ${APEX_HOME}/scripts/backup.sh") | crontab -u apex -

echo "✓ Cron jobs installed"

# ============================================================================
# 8. 로그 로테이션 설정
# ============================================================================
echo "[8/8] Configuring log rotation..."
cat > /etc/logrotate.d/apex-db <<EOF
${LOG_DIR}/*.log {
    daily
    rotate 30
    missingok
    notifempty
    compress
    delaycompress
    copytruncate
    create 0644 apex apex
}
EOF
echo "✓ Log rotation configured"

# ============================================================================
# 완료
# ============================================================================
echo ""
echo "========================================="
echo "APEX-DB Installation Complete!"
echo "========================================="
echo ""
echo "Next steps:"
echo "  1. Review config: ${APEX_HOME}/config.yaml"
echo "  2. Start service: sudo systemctl start apex-db"
echo "  3. Check status:  sudo systemctl status apex-db"
echo "  4. View logs:     sudo journalctl -u apex-db -f"
echo ""
echo "Endpoints:"
echo "  HTTP API:  http://localhost:8123/"
echo "  Health:    http://localhost:8123/health"
echo "  Metrics:   http://localhost:8123/metrics"
echo ""
