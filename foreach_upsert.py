# Helper example showing how to upsert micro-batches into a Delta table using foreachBatch
from delta.tables import DeltaTable
from pyspark.sql.functions import to_date


def upsert_to_delta(micro_batch_df, batch_id, target_path):
    if micro_batch_df.rdd.isEmpty():
        return
    df = micro_batch_df.withColumn("date", to_date("event_time"))
    # if target exists, perform merge; otherwise write
    try:
        delta_table = DeltaTable.forPath(df.sparkSession, target_path)
        delta_table.alias('t').merge(
            df.alias('s'),
            't.txn_id = s.txn_id'
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except Exception:
        df.write.format('delta').mode('append').save(target_path)