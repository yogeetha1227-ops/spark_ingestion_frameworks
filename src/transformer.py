def json_to_dataframe(spark, data):

    df = spark.createDataFrame(data)

    return df