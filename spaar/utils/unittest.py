import pyspark.sql.functions as F

def get_value_count(df, row, value):
    return df.filter(F.col(row) == value).count()