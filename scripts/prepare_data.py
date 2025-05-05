from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.feature import (VectorAssembler, Imputer, 
                               StringIndexer, OneHotEncoder, FeatureHasher,
                               Tokenizer, StopWordsRemover, Word2Vec)
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, HasInputCols, Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark import keyword_only
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DoubleType
import math

# Add here your team number teamx
team = "team26"

# location of your Hive database in HDFS
warehouse = "project/hive/warehouse"

spark = SparkSession.builder\
        .appName("{} - spark ML".format(team))\
        .master("yarn")\
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
        .config("spark.sql.warehouse.dir", warehouse)\
        .enableHiveSupport()\
        .getOrCreate()

df = spark.read.format("parquet").table("team26_projectdb.traffic_partitioned").where(F.col("state") == "CA").limit(1000)

# Custom transformer for cyclical encoding of temporal features
class CyclicalEncoder(Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable):
    periodicity = Param(Params._dummy(), "periodicity", "The periodicity of the feature", 
                       typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, periodicity=None):
        super(CyclicalEncoder, self).__init__()
        self._setDefault(periodicity=None)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, periodicity=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setPeriodicity(self, value):
        return self._set(periodicity=value)

    def getPeriodicity(self):
        return self.getOrDefault(self.periodicity)

    def _transform(self, dataset):
        inputCol = self.getInputCol()
        outputCol = self.getOutputCol()
        periodicity = self.getPeriodicity()

        sin_col = F.sin(2 * math.pi * F.col(inputCol) / periodicity)
        cos_col = F.cos(2 * math.pi * F.col(inputCol) / periodicity)

        return dataset.withColumn(f"{outputCol}_sin", sin_col).withColumn(f"{outputCol}_cos", cos_col)

# Custom transformer for geodetic to ECEF conversion
class GeoToECEF(Transformer, HasInputCols, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable):
    @keyword_only
    def __init__(self, inputCols=None, outputCol=None):
        super(GeoToECEF, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCols=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        inputCols = self.getInputCols()
        outputCol = self.getOutputCol()

        # Constants for WGS84 ellipsoid
        a = 6378137.0  # semi-major axis
        b = 6356752.3142  # semi-minor axis
        f = (a - b) / a  # flattening
        e_sq = f * (2 - f)  # eccentricity squared

        # UDF for geodetic to ECEF conversion
        def geodetic_to_ecef(lat, lon, alt=0):
            if lat is None or lon is None:
                return (None, None, None)

            # Default altitude to 0 if None
            if alt is None:
                alt = 0

            lat_rad = math.radians(lat)
            lon_rad = math.radians(lon)

            # Prime vertical radius of curvature
            N = a / math.sqrt(1 - e_sq * math.sin(lat_rad) ** 2)

            # Calculate ECEF coordinates
            x = (N + alt) * math.cos(lat_rad) * math.cos(lon_rad)
            y = (N + alt) * math.cos(lat_rad) * math.sin(lon_rad)
            z = (N * (1 - e_sq) + alt) * math.sin(lat_rad)

            return (x, y, z)

        # Register UDF
        geodetic_to_ecef_udf = F.udf(geodetic_to_ecef, StructType([
            StructField("x", DoubleType(), True),
            StructField("y", DoubleType(), True),
            StructField("z", DoubleType(), True)
        ]))

        # Apply transformation based on number of input columns
        if len(inputCols) == 2:  # lat, lon only
            lat_col, lon_col = inputCols
            ecef_col = geodetic_to_ecef_udf(F.col(lat_col), F.col(lon_col))
        elif len(inputCols) == 3:  # lat, lon, alt
            lat_col, lon_col, alt_col = inputCols
            ecef_col = geodetic_to_ecef_udf(F.col(lat_col), F.col(lon_col), F.col(alt_col))
        else:
            raise ValueError("inputCols should contain either 2 columns (lat, lon) or 3 columns (lat, lon, alt)")

        return dataset \
            .withColumn(f"{outputCol}_x", ecef_col.getItem("x")) \
            .withColumn(f"{outputCol}_y", ecef_col.getItem("y")) \
            .withColumn(f"{outputCol}_z", ecef_col.getItem("z"))


class TimeDeltaTransformer(Transformer, HasInputCols, HasOutputCol):
    def __init__(self, inputCols=None, outputCol=None):
        super(TimeDeltaTransformer, self).__init__()
        self._set(inputCols=inputCols, outputCol=outputCol)

    def _transform(self, dataset):
        inputCols = self.getInputCols()
        outputCol = self.getOutputCol()

        if len(inputCols) != 2:
            raise ValueError("inputCols should contain exactly 2 columns (start_time, end_time)")

        start_time_col, end_time_col = inputCols

        return dataset.withColumn(
            outputCol,
            (F.unix_timestamp(F.col(end_time_col)) - F.unix_timestamp(F.col(start_time_col))) / 3600
        )


class TimeFeatureExtractor(Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable):
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super().__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        input_col = self.getInputCol()
        outputCol = self.getOutputCol()

        return dataset \
            .withColumn(f"{outputCol}_year", F.year(input_col)) \
            .withColumn(f"{outputCol}_month", F.month(input_col)) \
            .withColumn(f"{outputCol}_day", F.dayofmonth(input_col)) \
            .withColumn(f"{outputCol}_hour", F.hour(input_col)) \
            .withColumn(f"{outputCol}_minute", F.minute(input_col))


class NAIndicator(Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable):
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super().__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        input_col = self.getInputCol()
        output_col = self.getOutputCol()

        return dataset.withColumn(
            output_col,
            F.when(F.col(input_col).isNull(), 1.0).otherwise(0.0)
        )

columns_to_drop = [
    "delay_from_free_flow_speed",  # Potential data leakage
    "end_time",                    # Potential data leakage
    "weather_event",               # High missing rate (94%) and redundant
    "local_time_zone",             # Already encoded in timestamps
    "zip_code",                    # Unique identifier
    "id",                          # Unique identifier
    "country"                      # Only contains US, so no variability
]

df_essential = df.drop(*columns_to_drop)
df_essential = df_essential.filter(F.col("delay_from_typical_traffic").isNotNull())
df_essential = df_essential.fillna({"description": ""})
final_features = []

df_essential.columns

# Missing indcators
wind_na_indicator = NAIndicator(
    inputCol="wind_chill",
    outputCol="wind_chill_na"
)

precipitation_na_indicator = NAIndicator(
    inputCol="precipitation",
    outputCol="precipitation_na"
)
final_features.extend([
    "wind_chill_na",
    "precipitation_na"
])

# Categorical
low_cardinality_cols = [
    "severity",
    "congestion_speed",
    "state",
    "wind_dir"
]
high_cardinality_cols = [
    "county",
    "city",
    "weather_station_airport_code",
    "weather_conditions"
]
ultra_high_cardinality_cols = ["street"]

indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
    for col in low_cardinality_cols
]
encoders = [
    OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_onehot", handleInvalid="keep")
    for col in low_cardinality_cols
]

moderate_hashers = []
for col in high_cardinality_cols:
    moderate_hashers.append(
        FeatureHasher(
            inputCols=[col],
            outputCol=f"{col}_hash",
            numFeatures=16,
            categoricalCols=[col]
        )
    )

ultra_hashers = []
for col in ultra_high_cardinality_cols:
    ultra_hashers.append(
        FeatureHasher(
            inputCols=[col],
            outputCol=f"{col}_hash",
            numFeatures=128,
            categoricalCols=[col]
        )
    )
for col in low_cardinality_cols:
    final_features.append(f"{col}_onehot")

for col in high_cardinality_cols + ultra_high_cardinality_cols:
    final_features.append(f"{col}_hash")

# Time transformation
weather_time_transformer = TimeDeltaTransformer(
    inputCols=["start_time", "weather_time_stamp"],
    outputCol="weather_time_delta"
)
start_time_transformer = TimeFeatureExtractor(
    inputCol="start_time",
    outputCol="start"
)

# Cycle and Geo
time_encoders = []
for col, period in [
    ("start_month", 12),
    ("start_day", 31),
    ("start_hour", 24),
    ("start_minute", 60)
]:
    time_encoders.append(
        CyclicalEncoder(inputCol=col, outputCol=col, periodicity=period)
    )

geo_transformer = GeoToECEF(
    inputCols=["start_lat", "start_lng"],
    outputCol="ecef_coords"
)

# Text
tokenizer = Tokenizer(
    inputCol="description",
    outputCol="words"
)
stop_words_remover = StopWordsRemover(
    inputCol="words",
    outputCol="filtered_words"
)
word2Vec = Word2Vec(
    vectorSize=64,
    seed=42,
    minCount=5,
    inputCol="filtered_words",
    outputCol="description_enc"
)
final_features.append("description_enc")


# Imput continuous data and year
regular_continuous = [
    "distance",
    "temperature",
    "humidity",
    "pressure",
    "visibility",
    "wind_speed",
    "weather_time_delta",
    "start_month_sin",
    "start_month_cos",
    "start_day_sin",
    "start_day_cos",
    "start_hour_sin",
    "start_hour_cos",
    "start_minute_sin",
    "start_minute_cos",
    "ecef_coords_x",
    "ecef_coords_y",
    "ecef_coords_z"
]
high_missing_continuous = [
    "wind_chill",
    "precipitation"
]

regular_imputer = Imputer(
    inputCols=regular_continuous,
    outputCols=regular_continuous,
    strategy="mean"
)
high_missing_imputer = Imputer(
    inputCols=high_missing_continuous,
    outputCols=high_missing_continuous,
    strategy="median"
)
year_imputer = Imputer(
    inputCols=["start_year"],
    outputCols=["start_year"],
    strategy="mode"
)
final_features += regular_continuous + high_missing_continuous + ["start_year"]

vector_assembler = VectorAssembler(
    inputCols=final_features,
    outputCol="features",
    handleInvalid="keep"
)

missing_indicators = [
    wind_na_indicator,
    precipitation_na_indicator
]

categorical_indexers = indexers
categorical_encoders = encoders

high_card_hashers = moderate_hashers
ultra_card_hashers = ultra_hashers

time_delta = weather_time_transformer

time_features = start_time_transformer

cyclical_encoders = time_encoders

geo_ecef = geo_transformer

text_processing = [
    tokenizer,
    stop_words_remover,
    word2Vec
]

imputers = [
    regular_imputer,
    high_missing_imputer,
    year_imputer
]

pipeline_stages = (
    missing_indicators +
    categorical_indexers +
    categorical_encoders +
    high_card_hashers +
    ultra_card_hashers +
    [time_delta, time_features] +
    cyclical_encoders +
    [geo_ecef] +
    text_processing +
    imputers +
    [vector_assembler]
)

final_pipeline = Pipeline(stages=pipeline_stages)

traffic_pipeline = final_pipeline.fit(df_essential)
transformed_df = traffic_pipeline.transform(df_essential)

(train_data, test_data) = transformed_df.randomSplit([0.8, 0.2], seed=42)

train_data.select("features", "delay_from_typical_traffic")\
    .coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("json")\
    .save("project/data/train")


test_data.select("features", "delay_from_typical_traffic")\
    .coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("json")\
    .save("project/data/test")
