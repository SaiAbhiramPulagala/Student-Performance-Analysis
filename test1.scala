import org.apache.spark.sql.{DataFrame, Row, SQLImplicits, SparkSession}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator

object test1 {
  def read_from_mysql(_spark: SparkSession, tableName: String): DataFrame = {
    _spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost/Student")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", "root")
      .option("password", "Aswitha@2004")
      .load()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkExample").master("local[*]").getOrCreate()
    // Join info and grades
    val table1 = "student_info"
    val table2 = "grades"
    val df_info = read_from_mysql(spark, table1)
    val df_grades = read_from_mysql(spark, table2)
    val studentDF = df_info.join(df_grades, "student_id")
    val indexer = new StringIndexer()
      .setInputCols(Array("school", "sex", "address", "famsize", "Pstatus", "schoolsup", "famsup", "paid",
        "activities", "nursery", "higher", "internet", "romantic"))
      .setOutputCols(Array("schoolIdx", "sexIdx", "addressIdx", "famsizeIdx", "PstatusIdx", "schoolsupIdx",
        "famsupIdx", "paidIdx", "activitiesIdx", "nurseryIdx", "higherIdx", "internetIdx", "romanticIdx"))

    val assembler = new VectorAssembler()
      .setInputCols(indexer.getOutputCols ++ Array("age", "G1", "G2"))
      .setOutputCol("features")

    // Train random forest model
    val rf = new RandomForestRegressor()
      .setLabelCol("G3")
      .setFeaturesCol("features")

    // Pipeline of stages
    val pipeline = new Pipeline()
      .setStages(Array(indexer, assembler, rf))

    // Train model
    val model = pipeline.fit(studentDF)

    // Evaluate model
    val predictions = model.transform(studentDF)
    predictions.select("G3", "prediction").show(5)
    val evaluator = new RegressionEvaluator()
      .setLabelCol("G3")
      .setPredictionCol("prediction")
    val evaluator1 = new RegressionEvaluator()
      .setLabelCol("G3")
      .setPredictionCol("prediction").setMetricName("r2")
    val evaluator2 = new RegressionEvaluator()
      .setLabelCol("G3")
      .setPredictionCol("prediction").setMetricName("mae")
    val rmse = evaluator.evaluate(predictions)
    val r2 = evaluator1.evaluate(predictions)
    val mae = evaluator2.evaluate(predictions)

    println("RMSE: " + rmse)
    println("R2: " + r2)
    println("mae: "+mae)

    spark.stop()
  }

}