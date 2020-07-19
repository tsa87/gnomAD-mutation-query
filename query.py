from google.cloud import bigquery
from hurry.filesize import size

table_prefix = "bigquery-public-data.gnomAD.v2_1_1_exomes__chr"

_GET_MUTATION_COUNT_QUERY = (
    'SELECT count(1) AS num_mutations '
    'FROM {TABLE_NAME} AS T, T.alternate_bases AS ALT '
    'WHERE start_position >= {START_POSITION} AND start_position <= {END_POSITION} AND ALT.AF <=0.01')

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

    def _est_query_cost(self, query):
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = self._client.query(query, job_config=job_config)
        
        return query_job.total_bytes_processed

    # https://github.com/googlegenomics/gcp-variant-transforms/blob/master/gcp_variant_transforms/libs/partitioning.py
    def _run_query(self, query):
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

        total_bytes_billed = query_job.total_bytes_billed
        print(total_bytes_billed)

        return result


caller = BigQueryCaller()
table_name = table_prefix + "1"

query = _GET_MAX_START_POSITION_QUERY.format(TABLE_NAME=table_name)
print(size(caller._est_query_cost(query)))
#max_start_position = caller._run_query(query)[0]

start_position = 0

query = _GET_MUTATION_COUNT_QUERY.format(
    TABLE_NAME=table_name,
    START_POSITION=start_position,
    END_POSITION=start_position+100000
)

#num_mutations = caller._run_query(query)[0]