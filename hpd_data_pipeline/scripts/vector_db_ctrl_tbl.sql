-- query used for creating the control table 
-- used in /DB_VirRt/hpd_data_pipeline/utilities/data_vectorizations.py
CREATE OR REPLACE TABLE mycatalog.hp_prd_data.fp_vector_data_ctrl_table (
  last_processed_ts TIMESTAMP,
  last_pushed_ts TIMESTAMP,
  batch_id INT
  )
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')