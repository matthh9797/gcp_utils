# Save BigQuery results to Cloud Storage

# Libraries ... 
from google.cloud import bigquery
import datetime
import sys


def bq_to_storage(SQL_FILENAME):
    
    # Hard coded for this project
    BUCKET_NAME = 'blockchain-exploration/queries'
    
    # Query Parameters
    SQL_PATH = 'sql/' + SQL_FILENAME + '.sql'
    todaysDate = datetime.date.today().strftime('%Y-%m-%d');
    BUCKET_URI = 'gs://{}'.format(BUCKET_NAME)
    
    bqclient = bigquery.Client()
    
    QUERY = open(SQL_PATH).read()
    
    dataframe = (
        bqclient.query(QUERY)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    
    filename = "{}/{}-{}.csv".format(BUCKET_URI, todaysDate, SQL_FILENAME)
    dataframe.to_csv(filename)


if __name__ == "__main__":
    args = sys.argv
    # args[0] = current file
    # args[1] = function name
    # args[2:] = function args : (*unpacked)
    globals()[args[1]](*args[2:])
