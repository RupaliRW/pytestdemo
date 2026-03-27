pipeline {
    agent any

    triggers {
        githubPush()
    }

    parameters {
        choice(name: 'JOB_TYPE', choices: ['transformation'], description: 'Spark job to run')
        string(name: 'NUM_EXECUTORS', defaultValue: '2', description: 'Number of YARN executors')
        string(name: 'EXECUTOR_MEMORY', defaultValue: '512m', description: 'Memory per executor')
    }

    environment {
        CLOUDERA_HOST = 'ec2-user@13.41.167.97'
        SSH_KEY       = '/var/lib/jenkins/.ssh/id_rsa'
    }

    stages {

        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Run Tests') {
            steps {
                sh '''
                    python3 -m venv venv
                    . venv/bin/activate
                    pip install -r requirements.txt
                    pytest tests/
                '''
            }
        }

        stage('Copy Files to Cloudera') {
            steps {
                sh '''
                    scp -i ${SSH_KEY} -o StrictHostKeyChecking=no \
                        src/transformation.py \
                        ${CLOUDERA_HOST}:/tmp/transformation_${BUILD_NUMBER}.py

                    scp -i ${SSH_KEY} -o StrictHostKeyChecking=no \
                        src/orders.csv \
                        ${CLOUDERA_HOST}:/tmp/orders_${BUILD_NUMBER}.csv
                '''
            }
        }

        stage('Submit Spark Job') {
            steps {
                sh '''
                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} "
                        export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
                        export HADOOP_CONF_DIR=/etc/hadoop/conf
                        export SPARK_CONF_DIR=/etc/spark/conf
                        export PYSPARK_PYTHON=/usr/bin/python3

                        spark-submit \
                          --master yarn \
                          --deploy-mode cluster \
                          --name transformation-build-${BUILD_NUMBER} \
                          --num-executors ${NUM_EXECUTORS} \
                          --executor-cores 1 \
                          --executor-memory ${EXECUTOR_MEMORY} \
                          --driver-memory 512m \
                          --conf spark.pyspark.python=/usr/bin/python3 \
                          /tmp/transformation_${BUILD_NUMBER}.py \
                          /tmp/orders_${BUILD_NUMBER}.csv \
                          /tmp/output_${BUILD_NUMBER}
                    "
                '''
            }
        }

        stage('Fetch Output') {
            steps {
                sh '''
                    scp -i ${SSH_KEY} -o StrictHostKeyChecking=no -r \
                        ${CLOUDERA_HOST}:/tmp/output_${BUILD_NUMBER} ./output/
                '''
            }
        }
    }

    post {
        success { echo 'Pipeline executed successfully' }
        failure { echo 'Pipeline failed' }
    }
}