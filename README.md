# MSK Kafka Connection

This document provides instructions and details for connecting to an Amazon MSK (Managed Streaming for Apache Kafka) cluster.

## Prerequisites

Before proceeding, ensure the following:
- An active AWS account.
- An MSK cluster is set up and running.
- AWS CLI is installed and configured.
- Kafka client tools are installed.

## Steps to Connect

We are using multiple packages to test the connections

- aiokafka

- kafka_python:

- confluent_kafka

### Running Kafka Packages on an EC2 Instance

1. **Connect to your EC2 instance**:
    ```bash
    ssh -i your-key.pem ec2-user@your-ec2-instance-ip
    ```

2. **Install required dependencies**:
    ```bash
    sudo yum update -y
    sudo yum install python3 -y
    pip install -r requirements.txt
    ```

3. **Run your Kafka script**:
    ```bash
    python3 *_exmample.py
    ```

Ensure your script includes the necessary configurations to connect to the MSK cluster.