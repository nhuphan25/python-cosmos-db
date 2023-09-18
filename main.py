from azure.data.tables import TableServiceClient, UpdateMode
from azure.core.exceptions import HttpResponseError
import os
import campaign_reports_table

connection_string = os.environ["COSMOS_CONNECTION_STRING"]
partition_key = os.environ["PARTITION_KEY"]

def main():
    with TableServiceClient.from_connection_string(connection_string) as client:
        try:
            campaign_reports_table.updateSegmentReportHRCode(client, partition_key)

        except HttpResponseError:
            raise
        finally:
            print("\nRevert Campaigns Data Completed")


if __name__ == "__main__":
    try:
        main()
    except HttpResponseError as e:
        print('\nerror: {0}'.format(e.message))
