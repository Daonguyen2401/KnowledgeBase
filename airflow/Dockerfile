FROM apache/airflow:3.0.2-python3.12

USER root



RUN apt-get update && \
    apt-get install -y \
      python3-pip \
      curl \
      iputils-ping \
      telnet \
      unzip \
      software-properties-common \
      ca-certificates \
      openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# ENV PATH=$PATH:$JAVA_HOME/bin


USER airflow

# First install setuptools to ensure pkg_resources is available
RUN pip install --upgrade pip
RUN pip install --no-cache-dir setuptools==69.0.3

# Then install Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt