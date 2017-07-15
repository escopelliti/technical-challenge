DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/..

if [ "$#" -ne 1 ]; then

### Set them if you are behind proxy
PROXY_HOST=
PROXY_PORT=
export http_proxy=http://$PROXY_HOST:$PROXY_PORT/
export https_proxy=https://$PROXY_HOST:$PROXY_PORT/

curl -O https://d3kbcqa49mib13.cloudfront.net/spark-1.6.0-bin-hadoop2.6.tgz
tar -xvzf spark-1.6.0-bin-hadoop2.6.tgz

SPARK_HOME=spark-1.6.0-bin-hadoop2.6

else

SPARK_HOME=$1
fi

rm -r output/*

$SPARK_HOME/bin/spark-submit \
  --class it.escopelliti.TimeSeriesExtractionJob \
  --master local \
  --num-executors 2 \
  --executor-memory 2g \
  --executor-cores 5 \
  --driver-memory 512m \
  --conf "spark.driver.extraJavaOptions=-Dhttp.proxyHost=$PROXY_HOST -Dhttp.proxyPort=$PROXY_PORT -Dhttps.proxyHost=$PROXY_HOST -Dhttps.proxyPort=$PROXY_PORT -Dconfig.file=conf/application.conf" \
  --packages com.databricks:spark-csv_2.10:1.5.0 \
lib/timeseries-extraction-job.jar