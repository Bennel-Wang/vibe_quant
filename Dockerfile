FROM python:3.11-slim

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 先复制依赖文件（利用 Docker 层缓存）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY . .

# 创建数据和日志目录
RUN mkdir -p data/history data/realtime data/news data/indicators data/features data/backtests logs

# 环境变量：配置文件路径（挂载 config.yaml 时使用）
ENV QUANT_CONFIG=/app/config.yaml
ENV PYTHONUNBUFFERED=1

EXPOSE 8080

CMD ["python", "main.py", "web", "--host", "0.0.0.0", "--port", "8080"]
