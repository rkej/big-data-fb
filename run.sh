sbt compile
sbt package 
spark-submit target/scala-2.11/mylab_2.11-1.0.jar --master yarn 
