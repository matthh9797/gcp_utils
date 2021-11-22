from google.cloud import bigquery
import pandas as pd


def bq_dataset_to_storage(bucket_name, dataset_id):

    BUCKET_URI = 'gs://{}'.format(bucket_name)
    
    # Construct a BigQuery client object.
    client = bigquery.Client()

    dataset_id = dataset_id
    tables = client.list_tables(dataset_id)  # Make an API request.
                          
    print("Tables contained in '{}':".format(dataset_id))
    for table in tables:
        print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        
        query = "select * from `{}.{}.{}`".format(table.project, table.dataset_id, table.table_id)
        dataframe = (
            client.query(query)
            .result()
            .to_dataframe(
                create_bqstorage_client=True,
            )
        )
            

        filename = "{}/{}-{}.csv".format(BUCKET_URI, table.dataset_id, table.table_id)
        dataframe.to_csv(filename)   