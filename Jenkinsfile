pipeline {
    agent any

    triggers {
        githubPush()
    }

    parameters {
        choice(name: 'JOB_TYPE', choices: ['transformation','pi_estimation','word_count'], description: 'Spark job to run on Cloudera')
        string(name: 'NUM_EXECUTORS',   defaultValue: '2',    description: 'Number of YARN executors')
        string(name: 'EXECUTOR_MEMORY', defaultValue: '512m', description: 'Memory per executor')
    }

    environment {
        CLOUDERA_HOST = 'ec2-user@13.41.167.97'
        SSH_KEY       = '/var/lib/jenkins/.ssh/id_rsa'
        SPARK_SCRIPTS = '/tmp/rupalicicd/'
        PYTHON_BIN    = 'python3'
    }

    stages {

        

        stage('Run Tests (No venv)') {
            steps {
                sh '''
                    ${PYTHON_BIN} --version

                    # Fix your main issue (old pip)
                    ${PYTHON_BIN} -m pip install --upgrade pip setuptools wheel
                    export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
                    # Install pytest safely (avoid strict version failure)
                    ${PYTHON_BIN} -m pip install "pytest>=6.2,<8"

                    # Install project deps if present
                    if [ -f requirements.txt ]; then
                        ${PYTHON_BIN} -m pip install -r requirements.txt
                    fi

                    # Run tests
                    ${PYTHON_BIN} -m pytest tests/
                '''
            }
        }

        stage('Validate Cluster') {
            steps {
                sh '''
                    echo "=== Checking Cloudera Cluster Health ==="
                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} \
                        "HADOOP_CONF_DIR=/etc/hadoop/conf yarn node -list 2>&1 | grep -E 'Total|RUNNING'
                         hdfs dfsadmin -report 2>&1 | grep -E 'Configured|DFS Remaining'
                         spark-submit --version 2>&1 | grep version"
                '''
            }
        }

        stage('Copy Spark Scripts to Cluster') {
            steps {
                sh '''
                    echo "=== Copying Spark Scripts ==="

                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} \
                        "mkdir -p ${SPARK_SCRIPTS}"

                    scp -i ${SSH_KEY} -o StrictHostKeyChecking=no \
                        src/*.py ${CLOUDERA_HOST}:${SPARK_SCRIPTS}/
                '''
            }
        }

        stage('Submit Spark Job') {
            steps {
                sh '''
                    echo "=== Submitting ${JOB_TYPE} ==="

                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} "
                        export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
                        export HADOOP_CONF_DIR=/etc/hadoop/conf
                        export SPARK_CONF_DIR=/etc/spark/conf
                        export PYSPARK_PYTHON=/usr/bin/python3

                        spark-submit \
                          --master yarn \
                          --deploy-mode client \
                          --name ${JOB_TYPE}-jenkins-${BUILD_NUMBER} \
                          --num-executors ${NUM_EXECUTORS} \
                          --executor-cores 1 \
                          --executor-memory ${EXECUTOR_MEMORY} \
                          --driver-memory 512m \
                          --conf spark.pyspark.python=/usr/bin/python3 \
                          ${SPARK_SCRIPTS}/${JOB_TYPE}.py
                    "
                '''
            }
        }

        stage('Check YARN Result') {
            steps {
                sh '''
                    echo "=== Recent YARN Applications ==="
                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} \
                        "yarn application -list -appStates FINISHED | head -10"
                '''
            }
        }
    }

    post {
        success { echo 'Spark job completed successfully on Cloudera YARN.' }
        failure { echo 'Pipeline failed. Check logs.' }
    }
}