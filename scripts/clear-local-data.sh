#!/usr/bin/env bash
# 清空本地持久化数据（项目、推演记录、报告等），使首页「推演记录」为空
# 使用方式：在项目根目录执行 ./scripts/clear-local-data.sh

set -e
UPLOAD_DIR="backend/uploads"
if [ ! -d "$UPLOAD_DIR" ]; then
  echo "目录 $UPLOAD_DIR 不存在，无需清空。"
  exit 0
fi

echo "将清空以下目录（推演记录、项目、报告等将丢失）："
echo "  - $UPLOAD_DIR/projects"
echo "  - $UPLOAD_DIR/simulations"
echo "  - $UPLOAD_DIR/reports"
read -p "确认继续？[y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[yY]$ ]]; then
  echo "已取消。"
  exit 0
fi

rm -rf "$UPLOAD_DIR/projects" "$UPLOAD_DIR/simulations" "$UPLOAD_DIR/reports"
echo "已清空。重启应用后首页推演记录将为空。"
