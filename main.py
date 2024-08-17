import boto3
import datetime
import logging

# Initialize AWS clients
ec2_client = boto3.client('ec2')

def get_snapshots():
    snapshots = ec2_client.describe_snapshots(OwnerIds=['self'])['Snapshots']
    return snapshots

def get_volumes():
    volumes = ec2_client.describe_volumes()['Volumes']
    return volumes

def get_attached_volumes():
    volumes = get_volumes()
    attached_volumes = {vol['VolumeId'] for vol in volumes if vol['Attachments']}
    return attached_volumes

def delete_snapshot(snapshot_id):
    try:
        ec2_client.delete_snapshot(SnapshotId=snapshot_id)
        logging.info(f"Deleted snapshot {snapshot_id}")
    except Exception as e:
        logging.error(f"Failed to delete snapshot {snapshot_id}: {e}")

def lambda_handler(event, context):
    logging.basicConfig(level=logging.INFO)
    
    # Define the timezone offset for UTC
    utc_offset = datetime.timezone.utc
    
    # Make the current time timezone-aware
    current_time = datetime.datetime.now(utc_offset)

    # Define the cutoff time (1 year ago) and make it timezone-aware
    one_year_ago = current_time - datetime.timedelta(days=365)
    
    snapshots = get_snapshots()
    attached_volumes = get_attached_volumes()
    
    for snapshot in snapshots:
        snapshot_id = snapshot['SnapshotId']
        volume_id = snapshot.get('VolumeId')
        start_time = snapshot['StartTime']

        # Ensure start_time is timezone-aware
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=utc_offset)

        # Compare the dates
        if start_time < one_year_ago or (volume_id and volume_id not in attached_volumes):
            delete_snapshot(snapshot_id)
    
    return {
        'statusCode': 200,
        'body': 'Snapshot cleanup completed.'
    }
