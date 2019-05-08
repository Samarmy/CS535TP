#!/bin/bash
#https://spark.apache.org/docs/latest/submitting-applications.html
$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar politics /tp/predictionsPolitics

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar funny /tp/predictionsFunny

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar win /tp/predictionsWin

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar happybirthday /tp/predictionsHappybirthday

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar metoo /tp/predictionsMetoo

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar photography /tp/predictionsPhotography

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar marvel /tp/predictionsMarvel

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar pets /tp/predictionsPets

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar friends /tp/predictionsFriends

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar science /tp/predictionsScience

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar birthday /tp/predictionsBirthday

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar tech /tp/predictionsTech

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar technology /tp/predictionsTechnology

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar fashion /tp/predictionsFashion

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar trump /tp/predictionsTrump

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar impeachdonaldtrump /tp/predictionsImpeachdonaldtrump

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar news /tp/predictionsNews

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar fakenews /tp/predictionsFakenews

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar family /tp/predictionsFamily

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar food /tp/predictionsFood

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar summer /tp/predictionsSummer

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar usa /tp/predictionsUsa

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar love /tp/predictionsLove

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar men /tp/predictionsMen

$SPARK_HOME/bin/spark-submit --class MLModel --master spark://kenai:30135 --deploy-mode cluster Assgn1/target/scala-2.11/tp_2.11-1.0.jar women /tp/predictionsWomen
