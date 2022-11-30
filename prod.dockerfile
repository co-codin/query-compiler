FROM python:3.8.7
ARG SERVICE_PORT=8888
RUN pip install --no-cache-dir -U pip

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

COPY query_compiler/ /app/query_compiler/

EXPOSE $SERVICE_PORT

WORKDIR /app
CMD ["python3", "-m", "query_compiler"]
