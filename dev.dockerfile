FROM python:3.8.7

RUN pip install --no-cache-dir -U pip

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

WORKDIR /app

EXPOSE 8888
CMD ["python3", "-m", "query_compiler"]
