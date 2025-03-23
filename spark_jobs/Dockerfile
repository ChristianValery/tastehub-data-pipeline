FROM bitnami/spark:3.1.2

USER root

RUN apt-get update && apt-get install -y wget \
    && wget -P /opt/spark/jars https://jdbc.postgresql.org/download/postgresql-42.2.23.jar \
    && wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.2/spark-sql-kafka-0-10_2.12-3.1.2.jar \
    && apt-get remove -y wget && apt-get autoremove -y

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["spark-submit", "--jars", "/opt/spark/jars/postgresql-42.2.23.jar,/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.1.2.jar", "streaming_analysis.py"]