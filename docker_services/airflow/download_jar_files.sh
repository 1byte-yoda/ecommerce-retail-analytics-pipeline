cd docker_services/airflow/jars

echo "Download Path:" ${PWD}


function download_jar() {
  FILE=$1;
  URL=$2

  if [ -f "${FILE}" ]; then
    echo "[Skipping Download] ${FILE} File Already Exists! ..."

  else
    echo "[Downloading] ${URL}..."

    if [ "$(uname)" == "Darwin" ]; then
      curl -O --no-verbose ${URL}
    elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
      wget --no-verbose ${URL}
    else
      echo "Please download the file using the link below place it under the ./docker_services/aiflow/jars directory."
      echo ${URL}

    fi
  fi
}


DELTA_JAR_FILENAME=delta-core_2.12-2.4.0.jar
DELTA_JAR_URL=https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/${DELTA_JAR_FILENAME}

DELTA_STORAGE_JAR_FILENAME=delta-storage-2.4.0.jar
DELTA_STORAGE_JAR_URL=https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/${DELTA_STORAGE_JAR_FILENAME}

ANT_LR4_JAR_FILENAME=antlr4-runtime-4.9.3.jar
ANT_LR4_JAR_URL=https://repo1.maven.org/maven2/io/antlr4/antlr4-runtime/4.9.3/${ANT_LR4_JAR_FILENAME}

WILDFLY_OPENSSL_JAR_FILENAME=wildfly-openssl-1.0.7.Final.jar
WILDFLY_OPENSSL_JAR_URL=https://repo1.maven.org/maven2/io/openssl/wildfly-openssl/1.0.7.Final/${WILDFLY_OPENSSL_JAR_FILENAME}

JAVA_SDK_JAR_FILENAME=aws-java-sdk-bundle-1.12.392.jar
JAVA_SDK_JAR_URL=https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.392/${JAVA_SDK_JAR_FILENAME}

HADOOP_JAR_FILENAME=hadoop-aws-3.3.1.jar
HADOOP_JAR_URL=https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/${HADOOP_JAR_FILENAME}

download_jar ${DELTA_JAR_FILENAME} ${DELTA_JAR_URL}
download_jar ${JAVA_SDK_JAR_FILENAME} ${JAVA_SDK_JAR_URL}
download_jar ${HADOOP_JAR_FILENAME} ${HADOOP_JAR_URL}
download_jar ${DELTA_STORAGE_JAR_FILENAME} ${DELTA_STORAGE_JAR_URL}
download_jar ${ANT_LR4_JAR_FILENAME} ${ANT_LR4_JAR_URL}
download_jar ${WILDFLY_OPENSSL_JAR_FILENAME} ${WILDFLY_OPENSSL_JAR_URL}

cd ../../..