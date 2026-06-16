FROM apache/airflow:3.2.2

# observable marker — proves our image is the one running
ENV NSE_CUSTOM_IMAGE=1

# --- system layer: a JVM for Spark (root for apt, then back to airflow) ---
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# JAVA_HOME so PySpark can find the JVM
ENV JAVA_HOME=/usr/lib/jvm/default-java

# --- python layer: Spark + Delta, pinned to the project (runs as airflow) ---
RUN pip install --no-cache-dir \
    pyspark==4.1.2 \
    delta-spark==4.1.0

# --- bake Delta engine onto Spark's classpath (resolved at build, no runtime Ivy) ---
RUN python -c "from pyspark.sql import SparkSession; from delta import configure_spark_with_delta_pip; configure_spark_with_delta_pip(SparkSession.builder.config('spark.jars.ivy','/tmp/ivy')).getOrCreate().stop()" \
    && JARS_DIR="$(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))")" \
    && find /tmp/ivy -name '*.jar' -exec cp -n {} "$JARS_DIR/" \; \
    && rm -rf /tmp/ivy