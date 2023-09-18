from azure.data.tables import UpdateMode
from azure.core.exceptions import HttpResponseError
import json

def getCampaignIdsList():
    filename = "./campaign-ids.txt"
    campaign_ids_list = []
    campaign_ids_file = open(filename, "r")
    while True:
        id = campaign_ids_file.readline()
        if not id:
            break
        campaign_ids_list.append(id)

    return campaign_ids_list

def changeHRCodeToNull(segmentReport):
    report_dict = json.loads(segmentReport)

    for key in report_dict:
        key['salesRepHrCode1'] = None
        key['salesRepHrCode2'] = None

    return report_dict

def updateSegmentReportHRCode(cosmosDBClient, partitionKey):
    with cosmosDBClient.get_table_client("CampaignReports") as table_cilent:
        campaign_ids = getCampaignIdsList()

        try:
            for campaign_id in campaign_ids:
                entities = table_cilent.query_entities(
                    query_filter="PartitionKey eq '%s' and CampaignId eq '%s'" % (partitionKey, campaign_id))

                for document in entities:
                    print(document['SegmentReports'])
                    updated_segment_report = changeHRCodeToNull(document['SegmentReports'])
                    stringified_report = json.dumps(updated_segment_report)
                    document['SegmentReports'] = stringified_report
                    table_cilent.update_entity(mode=UpdateMode.REPLACE, entity=document)

                print("\nCampaign ID: %s HR Code Been Updated" % campaign_id)
        except HttpResponseError:
            raise
        finally:
            print("\nChange HR Code To Null for Segment Reports in Campaign Report Table Completed")