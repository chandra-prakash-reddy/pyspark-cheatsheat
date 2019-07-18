export SPARK_HOME="/home/tools/spark-2.4.3-bin-hadoop2.7"
export PATH="$SPARK_HOME/bin:$PATH"
export JAVA_HOME="/home/tools/jdk1.8.0_211"
export PATH="$JAVA_HOME/bin:$PATH"
   
export PYSPARK_SUBMIT_ARGS="pyspark-shell"
export PYSPARK_DRIVER_PYTHON=ipython
export PYSPARK_DRIVER_PYTHON_OPTS='notebook' pyspark
