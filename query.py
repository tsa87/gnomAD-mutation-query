from google.cloud import bigquery

table_prefix = "bigquery-public-data.gnomAD.v2_1_1_exomes__chr"

_GET_MUTATION_COUNT_QUERY = (
    'SELECT count(1) AS num_mutations'
    'FROM {TABLE_NAME} AS T, T.alternate_bases AS ALT'
    'WHERE start_position >= {START_POSITION} AND start_position <= {END_POSITION} AND ALT.AF <=0.01')

_GET_MAX_START_POSITION_QUERY = (
    'SELECT MAX(start_position)'
    'FROM {TABLE_NAME}'
)

class BigQueryCaller:
    """ Contains the BigQuery API Client and calling mechanism """

    def __init__(self, client=None, num_retries=5):
    """ Initialize `BigQueryCaller` object.
    Args:
        client: Bigquery API client object.
        num_retries: Re-attempt limit for TimeoutErrors in queries.
    """
        if client is None:
            self._client = bigquery.Client()
        else:
            self._client = client

        self._num_retries = num_retries


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
        return result


caller = BigQueryCaller()
table_name = table_prefix + "1"

query = _GET_MAX_START_POSITION_QUERY.format(TABLE_NAME=table_name)
max_start_position = caller._run_query(query)[0]

print(max_start_position)

start_position = 0

query = _GET_MUTATION_COUNT_QUERY.format(
    TABLE_NAME=table_name,
    START_POSITION=start_position,
    END_POSITION=start_position+100000
)

num_mutations = caller._run_query(query)[0]

print(num_mutations)
