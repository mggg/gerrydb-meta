from sqlalchemy import text
from gerrydb_meta import models

def create_column_value_partition_text(geo_id: int):
    table_name=models.ColumnValue.__table__.name
    sql=f"CREATE TABLE IF NOT EXISTS {models.SCHEMA}_{table_name}_{geo_id} PARTITION OF {models.SCHEMA}.{table_name} FOR VALUES IN ({geo_id})"
    return text(sql)