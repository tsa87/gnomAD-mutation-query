from google.cloud import bigquery
from hurry.filesize import size
import pandas as pd

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


    def run_queries(self, table_names, query_template, start_position=0, interval=100000, verbose=False):
        results = {}
        
        for table_name in table_names:
            
            end_position = self._run_single_result_query(table_name, _GET_MAX_START_POSITION_QUERY)
            
            if verbose:
                print("Starting processing " + table_name)
                print("Table max start position: " + str(end_position))
            
            result = self._run_partitioned_query(
                table_name,
                query_template,
                start_position,
                end_position,
                interval,
                verbose=verbose)
            
            self.df_to_csv(result, table_name + ".csv")
            results[table_name] = result
        
        return results
            
        
    def _run_single_result_query(self, table_name, query_template):
        query = query_template.format(
            TABLE_NAME=table_name
        )
        result = int(self._get_query_result(query)[0])
        
        return result
        

    # Returns a pandas data frame for queries that partitions a table.
    def _run_partitioned_query(self, table_name, query_template, start_position, end_position, interval, verbose=False):
        output = pd.DataFrame()
        
        for range_start in range(start_position, end_position-interval+1, interval):
            range_end = range_start + interval
            
            query = query_template.format(
                TABLE_NAME=table_name,
                START_POSITION=range_start,
                END_POSITION=range_end
            )
            
            cost = self._get_query_cost(query)
            result = int(self._get_query_result(query)[0])
            
            if verbose:
                print(str(range_start) + "~" + str(range_end) + ": " + str(result) + " " + size(cost))
            
            range_result = {
                'start_position': range_start, 
                'end_position': range_end,
                'measure': result,
                'bytes_processed': cost }
    
            output = output.append(range_result, ignore_index=True)
            
        columns = ['start_position', 'end_position', 'measure', 'bytes_processed']
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

    
    def df_to_csv(self, dataframe, name):
        dataframe.to_csv(name)


if __name__ == "__main__":
    caller = BigQueryCaller()
    
    table_prefix = "bigquery-public-data.gnomAD.v3_genomes__chr"
    table_postfixes = ["X", "Y", "1", "20", "17"]
    table_names = [table_prefix + table_postfix for table_postfix in table_postfixes]
    
    caller.run_queries(table_names, _GET_MUTATION_COUNT_QUERY, verbose=True)

