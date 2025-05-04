from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.feature import (VectorAssembler, StandardScaler, Imputer, 
                               StringIndexer, OneHotEncoder, FeatureHasher,
                               Tokenizer, StopWordsRemover, HashingTF, IDF)
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, HasInputCols, Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark import keyword_only
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DoubleType
import math

"""
CREATE EXTERNAL TABLE traffic_partitioned (
    id STRING, - congestion id (removed from modeling data due to high cardinality)
    severity INT, - severity level (0-4)
    start_lat DOUBLE, - congestion start latitude
    start_lng DOUBLE, - congestion start longitude
    start_time TIMESTAMP, - congestion start time (has a few missing values)
    end_time TIMESTAMP, - congestion end time (removed from modeling data due to potential data leakage)
    distance DOUBLE, - distance of congestion in miles
    delay_from_typical_traffic DOUBLE, - delay from typical traffic speed in minutes
    delay_from_free_flow_speed DOUBLE, - delay from free flow speed in minutes (removed from modeling data due to potential data leakage)
    congestion_speed STRING, - traffic speed (categorical with 3 values)
    description STRING, - event description (preprocessed and vectorized)
    street STRING, - street name (has a few missing values)
    city STRING, - city name (has a few missing values)
    county STRING, - county name
    country STRING, - country name (only US)
    zip_code STRING, - zip code (removed from modeling data due to high cardinality)
    local_time_zone STRING, - local time zone (already included in timestamps)
    weather_station_airport_code STRING, - weather station or airport code (has a few missing values)
    weather_time_stamp TIMESTAMP, - weather observation time (has a few missing values, will be used to calculate time difference with congestion start time)
    temperature DOUBLE, - observed temperature in Fahrenheit (has a few missing values)
    wind_chill DOUBLE, - observed wind chill in Fahrenheit (has 38% missing rate; include as a feature with missing indicator)
    humidity DOUBLE, - observed humidity in percentage (has a few missing values)
    pressure DOUBLE, - observed air pressure in inches (has a few missing values)
    visibility DOUBLE, - observed visibility in miles (has a few missing values)
    wind_dir STRING, - observed wind direction (categorical with 24 values, has a few missing values)
    wind_speed DOUBLE, - observed wind speed in miles per hour (has a few missing values)
    precipitation DOUBLE, - observed precipitation in inches, if any (has a 41% missing rate; include as a feature with missing indicator)
    weather_event STRING, - observed weather event (has a 94% missing rate; removed from modeling data due to high missing rate and redundancy)
    weather_conditions STRING - observed weather conditions (categorical with 173 values, has a few missing values)
)
PARTITIONED BY (state STRING)
CLUSTERED BY (city) INTO 4 BUCKETS
STORED AS PARQUET
LOCATION 'project/hive/warehouse/traffic_partitioned';
"""

# Set up SparkSession
team = "team26"
warehouse = "project/hive/warehouse"

spark = SparkSession.builder\
    .appName("{} - spark ML".format(team))\
    .master("yarn")\
    .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
    .config("spark.sql.warehouse.dir", warehouse)\
    .enableHiveSupport()\
    .getOrCreate()

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
            return dataset.withColumn(outputCol, geodetic_to_ecef_udf(F.col(lat_col), F.col(lon_col)))
        elif len(inputCols) == 3:  # lat, lon, alt
            lat_col, lon_col, alt_col = inputCols
            return dataset.withColumn(outputCol, geodetic_to_ecef_udf(F.col(lat_col), F.col(lon_col), F.col(alt_col)))
        else:
            raise ValueError("inputCols should contain either 2 columns (lat, lon) or 3 columns (lat, lon, alt)")

# Load data from Hive
df = spark.sql("SELECT * FROM team26_projectdb.traffic_partitioned WHERE state='CA' LIMIT 10000")

# 1. Initial data cleaning: drop columns that are not relevant or have high missing rate
columns_to_drop = [
    "delay_from_free_flow_speed",  # Potential data leakage
    "end_time",                    # Potential data leakage
    "weather_event",               # High missing rate (94%) and redundant
    "local_time_zone",             # Already encoded in timestamps
    "zip_code",                    # Unique identifier
    "id",                          # Unique identifier
    "country"                      # Only contains US, so no variability
]

df = df.drop(*columns_to_drop)

# 2. Calculate Weather Time Difference
df = df.withColumn(
    "weather_time_diff_hours",
    (F.unix_timestamp("start_time") - F.unix_timestamp("weather_time_stamp")) / 3600
)

# 3. Extract time components
df = df.withColumn("start_year", F.year(F.col("start_time")))
df = df.withColumn("start_month", F.month(F.col("start_time")))
df = df.withColumn("start_day", F.dayofmonth(F.col("start_time")))
df = df.withColumn("start_hour", F.hour(F.col("start_time")))
df = df.withColumn("start_minute", F.minute(F.col("start_time")))

# 4. Handle Missing Values with Indicators for high missing rate columns
high_missing_cols = ["wind_chill", "precipitation"]
for col_name in high_missing_cols:
    df = df.withColumn(
        f"{col_name}_missing",
        F.when(F.col(col_name).isNull(), 1.0).otherwise(0.0)
    )

# 5. Define column groups based on cardinality and missing values
# Regular numerical columns with few missing values
numeric_cols = ["distance", "temperature", "humidity", "pressure", "visibility", "wind_speed"]

# Categorical columns with low to medium cardinality - suitable for one-hot encoding
low_cardinality_cols = ["severity", "congestion_speed", "state", "wind_dir"]

# Categorical columns with high cardinality - will use feature hashing
high_cardinality_cols = ["county", "city", "weather_station_airport_code", "weather_conditions"]

# Ultra-high cardinality column - will use feature hashing with more features
ultra_high_cardinality_cols = ["street"]

# 6. Create pipeline stages

# Cyclical encoders for time components
time_encoders = []
for col, period in [
    ("start_month", 12), ("start_day", 31), ("start_hour", 24), ("start_minute", 60),
    ("weather_month", 12), ("weather_day", 31), ("weather_hour", 24), ("weather_minute", 60)
]:
    time_encoders.append(
        CyclicalEncoder(inputCol=col, outputCol=f"{col}_enc", periodicity=period)
    )

# ECEF transformer for geospatial data
geo_transformer = GeoToECEF(inputCols=["start_lat", "start_lng"], outputCol="ecef_coords")

# Imputers for numerical features
regular_imputer = Imputer(
    inputCols=numeric_cols, 
    outputCols=[f"{col}_imputed" for col in numeric_cols], 
    strategy="median"  # Using median instead of mean for better robustness against outliers
)

# Separate imputer for high missing rate features
high_missing_imputer = Imputer(
    inputCols=high_missing_cols, 
    outputCols=[f"{col}_imputed" for col in high_missing_cols],
    strategy="median"  # Using median for better robustness
)

# Process low cardinality categorical features with one-hot encoding
indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep") 
    for col in low_cardinality_cols
]
encoders = [
    OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_onehot") 
    for col in low_cardinality_cols
]

# Feature hashers for high cardinality categorical features
hashers = []
for col in high_cardinality_cols:
    hashers.append(
        FeatureHasher(
            inputCols=[col],
            outputCol=f"{col}_hash",
            numFeatures=200,  # Adjust based on cardinality
            categoricalCols=[col]
        )
    )

# Feature hasher for ultra-high cardinality column (street)
ultra_hashers = []
for col in ultra_high_cardinality_cols:
    ultra_hashers.append(
        FeatureHasher(
            inputCols=[col],
            outputCol=f"{col}_hash",
            numFeatures=1000,  # More features for ultra high cardinality
            categoricalCols=[col]
        )
    )

# Process text description with TF-IDF
tokenizer = Tokenizer(inputCol="description", outputCol="words")
stop_words_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
hashing_tf = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=300)
idf = IDF(inputCol="rawFeatures", outputCol="description_vector", minDocFreq=5)

# 7. Assemble all preprocessed features
# List all generated feature columns
numeric_cols_imputed = [f"{col}_imputed" for col in numeric_cols]
high_missing_cols_imputed = [f"{col}_imputed" for col in high_missing_cols]
high_missing_indicators = [f"{col}_missing" for col in high_missing_cols]

cyclical_cols = []
for col in ["start_month", "start_day", "start_hour", "start_minute", 
           "weather_month", "weather_day", "weather_hour", "weather_minute"]:
    cyclical_cols.extend([f"{col}_enc_sin", f"{col}_enc_cos"])

ecef_cols = ["ecef_coords.x", "ecef_coords.y", "ecef_coords.z"]
onehot_cols = [f"{col}_onehot" for col in low_cardinality_cols]
hash_cols = [f"{col}_hash" for col in high_cardinality_cols + ultra_high_cardinality_cols]

# Non-transformed numerical columns
year_cols = ["start_year", "weather_year", "weather_time_diff_hours"]

# Combine all feature columns
feature_cols = (
    numeric_cols_imputed + 
    high_missing_cols_imputed + 
    high_missing_indicators + 
    cyclical_cols + 
    ecef_cols + 
    onehot_cols + 
    hash_cols + 
    year_cols + 
    ["description_vector"]
)

# Vector assembler
assembler = VectorAssembler(
    inputCols=feature_cols, 
    outputCol="features", 
    handleInvalid="keep"
)

# Standardize features
scaler = StandardScaler(inputCol="features", outputCol="scaled_features", 
                       withStd=True, withMean=True)

# 8. Build the complete pipeline
pipeline_stages = (
    time_encoders + 
    [geo_transformer] + 
    [regular_imputer, high_missing_imputer] + 
    indexers + 
    encoders + 
    hashers + 
    ultra_hashers + 
    [tokenizer, stop_words_remover, hashing_tf, idf] + 
    [assembler, scaler]
)

pipeline = Pipeline(stages=pipeline_stages)

# 9. Fit the pipeline and transform the data
traffic_pipeline = pipeline.fit(df)
transformed_df = traffic_pipeline.transform(df)
