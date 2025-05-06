from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressor, LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.linalg import VectorUDT
from pyspark.sql.types import StructType, StructField, DoubleType

TEAM = "team26"
WAREHOUSE = "project/hive/warehouse"

spark = (
    SparkSession.builder.appName(f"{TEAM} - ML Model Training")
    .master("yarn")
    .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")
    .config("spark.sql.warehouse.dir", WAREHOUSE)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.inMemoryColumnarStorage.batchSize", 200)
    .config("spark.dynamicAllocation.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()
)
# spark = (
#     SparkSession.builder.appName(f"{TEAM} - ML Model Training")
#     .master("yarn")
#     .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")
#     .config("spark.hadoop.dfs.replication", "1")
#     .config("spark.sql.warehouse.dir", WAREHOUSE)
#     .config("spark.sql.adaptive.enabled", "true")
#     .config("spark.sql.shuffle.partitions", "400")
#     .config("spark.executor.instances", "5")
#     .config("spark.executor.cores", "4")
#     .config("spark.executor.memory", "4g")
#     .config("spark.executor.memoryOverhead", "1g")
#     .config("spark.dynamicAllocation.enabled", "false")
#     .enableHiveSupport()
#     .getOrCreate()
# )

train_schema = StructType(
    [
        StructField("features", VectorUDT()),
        StructField("delay_from_typical_traffic", DoubleType()),
    ]
)

test_schema = StructType(
    [
        StructField("features", VectorUDT()),
        StructField("delay_from_typical_traffic", DoubleType()),
    ]
)

train_data = spark.read.schema(train_schema).json("project/data/train")
test_data = spark.read.schema(test_schema).json("project/data/test")

LABEL = "delay_from_typical_traffic"
FEATURES = "features"

evaluator = RegressionEvaluator(
    labelCol=LABEL, predictionCol="prediction", metricName="rmse"
)

rf = RandomForestRegressor(labelCol=LABEL, featuresCol=FEATURES)

rf_paramGrid = (
    ParamGridBuilder()
    .addGrid(rf.maxDepth, [5, 10])
    .addGrid(rf.numTrees, [20, 50])
    .build()
)

rf_cv = CrossValidator(
    estimator=rf,
    estimatorParamMaps=rf_paramGrid,
    evaluator=evaluator,
    numFolds=5,
    parallelism=4,
)

rf_cvModel = rf_cv.fit(train_data)

best_rf_model = rf_cvModel.bestModel

best_rf_model.write().overwrite().save("project/models/model1")

rf_predictions = best_rf_model.transform(test_data)

rf_predictions.select(LABEL, "prediction").write.mode("overwrite").format("csv").option(
    "header", "true"
).save("project/output/model1_predictions")

rf_rmse = evaluator.evaluate(rf_predictions)
rf_r2 = evaluator.setMetricName("r2").evaluate(rf_predictions)
rf_mae = evaluator.setMetricName("mae").evaluate(rf_predictions)

lr = LinearRegression(labelCol=LABEL, featuresCol=FEATURES)

lr_paramGrid = (
    ParamGridBuilder()
    .addGrid(lr.regParam, [0.01, 0.1])
    .addGrid(lr.elasticNetParam, [0.0, 0.5])
    .build()
)

lr_cv = CrossValidator(
    estimator=lr,
    estimatorParamMaps=lr_paramGrid,
    evaluator=evaluator.setMetricName("rmse"),
    numFolds=5,
    parallelism=4,
)

lr_cvModel = lr_cv.fit(train_data)

best_lr_model = lr_cvModel.bestModel

best_lr_model.write().overwrite().save("project/models/model2")

lr_predictions = best_lr_model.transform(test_data)

lr_predictions.select(LABEL, "prediction").write.mode("overwrite").format("csv").option(
    "header", "true"
).save("project/output/model2_predictions")

evaluator.setMetricName("rmse")
lr_rmse = evaluator.evaluate(lr_predictions)
lr_r2 = evaluator.setMetricName("r2").evaluate(lr_predictions)
lr_mae = evaluator.setMetricName("mae").evaluate(lr_predictions)

data = [
    ("Random Forest", float(rf_rmse), float(rf_r2), float(rf_mae)),
    ("Linear Regression", float(lr_rmse), float(lr_r2), float(lr_mae)),
]
columns = ["Model", "RMSE", "R2", "MAE"]
evaluation_df = spark.createDataFrame(data, columns)

evaluation_df.write.mode("overwrite").format("csv").option("header", "true").save(
    "project/output/evaluation"
)

spark.stop()
