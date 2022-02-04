from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import pandas as pd
import io


class GCSToPostgresTransfer(BaseOperator):
  """
    Read CSV file from GCS and upload it to Postgres DB
  """

  template_fields = ()

  template_ext = ()

  ui_color = '#ededed'

  @apply_defaults
  def __init__(
    self,
    schema,
    table,
    bucket,
    object,
    google_cloud_conn_id = 'google_cloud_default',
    google_cloud_storage_conn_id = '',
    postgres_conn_id = 'postgres_default',
    delegate_to = None,
    *args,
    **kwargs):
      super(GCSToPostgresTransfer, self).__init__(*args, **kwargs)
      self.schema = schema
      self.table = table
      self.bucket = bucket
      self.object = object
      self.google_cloud_conn_id = google_cloud_conn_id
      self.postgres_conn_id = postgres_conn_id
      self.delegate_to = delegate_to

  def execute(self, context):
    
    #Create an instance to connect to GCS and Postgres
    self.log.info('Creating connection with: %s', self.google_cloud_conn_id)

    self.pg_hook = PostgresHook(postgre_conn_id = self.postgres_conn_id)
    self.gcs = GCSHook(gcp_conn_id = self.google_cloud_conn_id, delegate_to = self.delegate_to)

    self.log.info('Executing download: %s > %s', self.bucket, self.object)

    csvFile = self.gcs.download(bucket_name = self.bucket, object_name = self.object).decode(encoding = "utf-8", errors = "ignore")

    dataSchema = {
                  'InvoiceNo': 'string',
                  'StockCode': 'string',
                  'Description': 'string',
                  'Quantity': 'int64',
                  'InvoiceDate': 'datetime64',
                  'UnitPrice': 'float64',
                  'CustomerID': 'int64',
                  'Country': 'string'
                 }
    
    date_cols = ['InvoiceDate']

    # read csv file
    df_purchases = pd.read_csv(io.StringIO(csvFile),
      header=0,
      delimiter=",",
      quotechar='"',
      low_memory=False,
      dtype=dataSchema
      )
    
    self.log.info(df_purchases)
    self.log.info(df_purchases.info())

    df_purchases = df_purchases.replace(r"[\"]", r"'")
    list_df_purchases = df_purchases.values.tolist()
    list_df_purchases = [tuple(x) for x in list_df_purchases]

    self.log.info(list_df_purchases)

    list_target_fields = ['invoice_number', 
                          'stock_code',
                          'detail', 
                          'quantity', 
                          'invoice_date',
                          'unit_price',
                          'customer_id',
                          'country'
                          ]
    
    self.current_table = self.schema + '.' + self.table

    self.pg_hook.insert_rows(self.current_table,
                            list_df_purchases,
                            target_fields = list_target_fields,
                            commit_every = 1000,
                            replace = False)

    self.request = 'SELECT * FROM ' + self.current_table
    self.log.info(self.request)

    self.conn = self.pg_hook.get_conn()
    self.cursor = self.conn.cursor()
    self.cursor.execute(self.request)
    self.sources = self.cursor.fetchall()
    self.log.info(self.sources)

    for source in self.sources:           
            self.log.info("InvoiceNo: {0} - \
                           StockCode: {1} - \
                           Description: {2} - \
                           Quantity: {3} - \
                           InvoiceDate: {4} - \
                           UnitPrice: {5} - \
                           CustomerID: {6} - \
                           Country: {7} ".format( source[0],
                                                  source[1],
                                                  source[2],
                                                  source[3],
                                                  source[4],
                                                  source[5], 
                                                  source[6],
                                                  source[7]))  
    




    