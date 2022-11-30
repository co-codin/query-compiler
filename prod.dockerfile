FROM python:3.8.7
ARG SERVICE_PORT=8888
RUN pip install --no-cache-dir -U pip

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

RUN mkdir -p /usr/local/app/
WORKDIR /usr/local/app/
COPY query_compiler ./query_compiler/
RUN mkdir -p /var/logs/
RUN mkdir logs

EXPOSE $SERVICE_PORT
CMD ["python3", "-m", "query_compiler"]
