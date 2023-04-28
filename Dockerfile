FROM python:3.10.11-slim
COPY requirements.txt .
RUN pip install -r requirements.txt
WORKDIR /usr/local/src
COPY app.py .
EXPOSE 5000
ENTRYPOINT ["python", "app.py", "-u"]