# Start from Flink 1.17.2 base image
FROM flink:1.17.2

USER root

# Install Python 3.10 and pip
RUN apt-get update && \
    apt-get install -y python3.10 python3.10-venv python3.10-distutils curl && \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10 && \
    ln -sf /usr/bin/python3.10 /usr/bin/python && \
    ln -sf /usr/bin/pip3 /usr/bin/pip

# Install PyFlink compatible with Flink 1.17.2
RUN pip install apache-flink==1.17.0

# Set environment
ENV PYTHONPATH=/opt/flink/python

# Set working directory
WORKDIR /opt/flink

# Copy job files
COPY jobs/ /opt/flink/jobs/

EXPOSE 8081 6123 6124

CMD ["bash"]
