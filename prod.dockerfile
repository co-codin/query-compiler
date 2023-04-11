FROM python:3.8.7-slim

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY query_compiler/ /app/query_compiler/

WORKDIR /app
CMD ["python3", "-m", "query_compiler"]
