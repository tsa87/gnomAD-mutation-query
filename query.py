from google.cloud import bigquery
import pickle

client = bigquery.Client()

table_prefix = "bigquery-public-data.gnomAD.v2_1_1_exomes__chr"

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
        query = """
            SELECT count(1) AS num_mutations
            FROM {0} AS T, T.alternate_bases AS ALT
            WHERE start_position >= {1} AND start_position <= {2} AND ALT.AF <=0.01
        """.format(table_name, start_position, start_position+100000)

        results = client.query(query)

        for row in results:
            num_mutations = row.num_mutations

        bin.append(num_mutations)

    with open(table_name+"_mutations", 'wb') as fp:
        pickle.dump(bin, fp)




