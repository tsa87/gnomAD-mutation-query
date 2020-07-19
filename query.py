from google.cloud import bigquery

table_prefix = "bigquery-public-data.gnomAD.v2_1_1_exomes__chr"

_GET_MUTATION_COUNT_QUERY = (
    'SELECT count(1) AS num_mutations'
    'FROM {TABLE_NAME} AS T, T.alternate_bases AS ALT'
    'WHERE start_position >= {START_POSITION} AND start_position <= {END_POSITION} AND ALT.AF <=0.01')


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


# Running the query on only chromosome 1 for now
for chr_id in range(1, 2):

    table_name = table_prefix + str(chr_id)

    query = """
        SELECT MIN(start_position), MAX(start_position)
        FROM {0}
    """.format(table_name)

    results = client.query(query)

    for row in results:
        min_start_position, max_start_position = row.f0_, row.f1_


    bin = []

    for start_position in range(0, max_start_position, 100000):
        query = _GET_MUTATION_COUNT_QUERY.format(
            TABLE_NAME=table_name,
            START_POSITION=start_position,
            END_POSITION=start_position+100000
        )

        results = client.query(query)

        for row in results:
            num_mutations = row.num_mutations

        bin.append(num_mutations)

    with open(table_name+"_mutations", 'wb') as fp:
        pickle.dump(bin, fp)
