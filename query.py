from google.cloud import bigquery
from hurry.filesize import size
import pandas as pd

table_prefix = "bigquery-public-data.gnomAD.v2_1_1_exomes__chr"

_GET_MUTATION_COUNT_QUERY = (
    'SELECT count(1) AS num_mutations '
    'FROM {TABLE_NAME} AS T, T.alternate_bases AS ALT '
    'WHERE start_position >= {START_POSITION} AND start_position < {END_POSITION} AND ALT.AF <=0.01')

_GET_MAX_START_POSITION_QUERY = (
    'SELECT MAX(start_position) '
    'FROM {TABLE_NAME}'
)

class BigQueryCaller:
    """ Contains the BigQuery API Client and calling mechanism """

    def __init__(self, client=None, num_retries=5):

        if client is None:
            self._client = bigquery.Client()
        else:
            self._client = client

        self._num_retries = num_retries


    def run_query(self, table_name, query_template, start_position, end_position, interval):
        output = pd.DataFrame()
        
        for range_start in range(start_position, end_position-interval, interval):
            range_end = range_start + interval
            query = query_template.format(
                TABLE_NAME=table_name,
                START_POSITION=range_start,
                END_POSITION=range_end
            )
            cost = self._get_query_cost(query)
            result = int(self._get_query_result(query)[0])
            
            range_result = {
                'start_position': range_start, 
                'end_position': range_end,
                'mutation_count': result,
                'bytes_processed': cost }
    
            output = output.append(range_result, ignore_index=True)
            
        columns = ['start_position', 'end_position', 'mutation_count', 'bytes_processed']
        output = output.reindex(columns=columns)
            
        return output
            
        
    def _get_query_cost(self, query):
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = self._client.query(query, job_config=job_config)
        
        return query_job.total_bytes_processed


    # https://github.com/googlegenomics/gcp-variant-transforms/blob/master/gcp_variant_transforms/libs/partitioning.py
    def _get_query_result(self, query):
        query_job = self._client.query(query)

        num_retries = 0
        while True:
            try:
                iterator = query_job.result(timeout=300)
            except TimeoutError as e:
                print('Time out waiting for query: %s', query)
                if num_retries < self._num_retries:
                    num_retries += 1
                    time.sleep(90)
                else:
                    raise e
            else:
                break
        result = []
        for i in iterator:
            result.append(str(i.values()[0]))

        return result


caller = BigQueryCaller()
table_name = table_prefix + "1"

df = caller.run_query(table_name, _GET_MUTATION_COUNT_QUERY, 0, 500000, 100000)
df.to_csv("output.csv")

