# 使用官方的 Python 镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    libxml2-dev \
    libxslt1-dev \
    libjpeg-dev \
    zlib1g-dev \
    && apt-get clean

# 复制 requirements.txt 文件
COPY requirements.txt .

# 安装 Python 依赖
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# 复制项目代码到容器
COPY . .

# 设置默认命令
CMD ["python", "-m", "shiny", "run", "--host=0.0.0.0", "--port=8000"]