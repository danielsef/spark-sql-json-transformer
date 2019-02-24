
docker login
docker tag hseeberger/scala-sbt  danielsef85/scala-sbt
docker push danielsef85/scala-sbt



# Run application locally on 2 cores
cd /usr/local/Cellar/apache-spark/2.4.0/bin
/usr/local/Cellar/apache-spark/2.4.0/bin/spark-submit \
--class org.apache.spark.examples.Runner   --master local[2] \
/Users/danielsef/IdeaProjects/slick-pg/examples/spark-test/target/scala-2.11/spark-test_2.11-0.1.jar \
/Users/danielsef/IdeaProjects/slick-pg/examples/spark-test/data/20190223



  
  
  
