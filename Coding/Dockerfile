# 使用Python作为基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 复制requirements.txt文件并安装依赖
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# 复制Shiny应用代码
COPY . /app

# 暴露应用端口
EXPOSE 8000

# 运行Shiny应用
CMD ["python", "app.py"]