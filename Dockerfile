FROM python:3.12.5-slim

# Install dependencies
RUN apt-get update && apt-get install -y python3-pip python3-dev wget openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Download and install Apache Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.1-bin-hadoop3.tgz -C /opt/ && \
    rm spark-3.5.1-bin-hadoop3.tgz && \
    ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Install PySpark and other Python dependencies
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

# Download Hadoop and AWS JARs
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.2/hadoop-aws-3.2.2.jar -P /opt/spark/jars/
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar -P /opt/spark/jars/

# Copy the Python scripts
COPY pasta1_etl/src/main.py /app/pasta1_etl/src/main.py
COPY pasta1_etl/src/utils.py /app/pasta1_etl/src/utils.py
COPY pasta2_analytics/src/main.py /app/pasta2_analytics/src/main.py

# Set the working directory
WORKDIR /app

# Run the Python script - Uncomment the line for the script you want to run (Finding a better way is a future improvement)
# CMD python3 -u pasta1_etl/src/main.py 
# CMD python3 -u pasta2_analytics/src/main.py