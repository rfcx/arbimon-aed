import os
import json
import logging
import boto3
from botocore.exceptions import ClientError
from db import connect
import sqlalchemy as sqal
import datetime as dt
import time

session, engine, metadata = connect() # RDS connection

logger = logging.getLogger(__name__)
sqs = boto3.resource('sqs')

rfcx_assumed_role = boto3.client('sts').assume_role(
    RoleArn="arn:aws:iam::887044485231:role/pm-worker-sieve-invoker",
    RoleSessionName="tm_driver_AssumeRoleSession1"
)

rfcx_sqs = boto3.resource('sqs',
    region_name = 'eu-west-1',
    aws_access_key_id=rfcx_assumed_role['Credentials']['AccessKeyId'],
    aws_secret_access_key=rfcx_assumed_role['Credentials']['SecretAccessKey'],
    aws_session_token=rfcx_assumed_role['Credentials']['SessionToken']
)

# Function to break playlist into chunks
def divide_chunks(l, n): 
    for i in range(0, len(l), n):  
        yield l[i:i + n] 

def handler(event, context):
    
    # event stores:
    #   Playlist ID
    #   User ID
    #   AED job name
    #   Amplitude threshold
    #   Duration threshold
    #   Bandwidth threshold
    #   Area threshold
    #   Filter size

    t0 = time.time()

    print('AED job name: ' + event['name'])
    print('Playlist ID: ' + str(event['playlist_id']))
    
    print('DB connections...')
    recordings = sqal.Table('recordings', metadata, autoload=True, autoload_with=engine)
    plist_recs = sqal.Table('playlist_recordings', metadata, autoload=True, autoload_with=engine)
    plists = sqal.Table('playlists', metadata, autoload=True, autoload_with=engine)
    job_params = sqal.Table('job_params_audio_event_detection_clustering', metadata, autoload=True, autoload_with=engine)
    jobs = sqal.Table('jobs', metadata, autoload=True, autoload_with=engine)

    print('Querying...')
    # Get project_id using playlist_id
    query = sqal.select([plists.columns.project_id]).where(plists.columns.playlist_id==event['playlist_id'])
    proj_id = session.execute(query).fetchall()[0][0]
    print('Project ID: '+str(proj_id))
    
    print('Querying playlist info...')
    j = plist_recs.join(recordings, recordings.c.recording_id == plist_recs.c.recording_id)
    query = sqal.select([plist_recs.c.recording_id,
                         recordings.c.sample_rate,
                         recordings.c.uri]).select_from(j).where(plist_recs.c.playlist_id==event['playlist_id'])
    result = session.execute(query).fetchall()
    rec_ids = [i[0] for i in result]
    rec_srs = [i[1] for i in result] # sample rate
    rec_uris = [i[2] for i in result]
    rec_accts = [0 if i.startswith('project_') else 1 for i in rec_uris]

    # # Insert new job
    ins = jobs.insert().values(job_type_id=8,
                                date_created=dt.datetime.now(),
                                last_update=dt.datetime.now(),
                                project_id=proj_id,
                                user_id=event['user_id'],
                                state='waiting',
                                progress=0,
                                completed=0,
                                progress_steps=10,
                                hidden=0,
                                ncpu=10)
    result = session.execute(ins)
    job_id = result.inserted_primary_key[0]
    print('Job ID: '+str(job_id))

    # Insert to job_params_audio_event_detection_clustering
    param_string = '{"Amplitude Threshold": '+str(float(event['Amplitude Threshold'])) + \
                   ', "Duration Threshold": '+str(event['Duration Threshold']) + \
                   ', "Bandwidth Threshold": '+str(event['Bandwidth Threshold']) + \
                   ', "Area Threshold": '+str(event['Area Threshold']) + \
                   ', "Filter Size": '+str(event['Filter Size']) + \
                   ', "Min Frequency": '+str(event['Min Frequency']) + \
                   ', "Max Frequency": '+str(event['Max Frequency']) + \
                   '}'
                        
    ins = job_params.insert().values(name=event['name'],
                                  project_id=proj_id,
                                  job_id=job_id,
                                  date_created=dt.datetime.now(),
                                  parameters=param_string,
                                  playlist_id=event['playlist_id'],
                                  user_id=event['user_id'])
    result = session.execute(ins)

    # launch workers for each AWS account in the playlist
    worker_id = 0
    for account in sorted(list(set(rec_accts))):

        # get recording IDs and sample rates for the current account
        rec_ids_acct = [rec_ids[i] for i in range(len(rec_accts)) if rec_accts[i]==account]
        rec_srs_acct = [rec_srs[i] for i in range(len(rec_accts)) if rec_accts[i]==account]
        rec_uris_acct = [rec_uris[i] for i in range(len(rec_accts)) if rec_accts[i]==account]

        print('\tSorting list by sample rate...')
        rec_srs_acct, rec_ids_acct, rec_uris_acct = zip(*sorted(zip(rec_srs_acct, rec_ids_acct, rec_uris_acct)))
        print(time.time() - t0)

    
        # Assign 10% of playlist to each item, with a limit per item
        if max(rec_srs_acct)<200000:
            if account==0: # sieve
                limit = 125
            else: # rfcx
                limit = 100
        else:
            if account==0: 
                limit = 62
            else:
                limit = 50
        mean_sr = sum(rec_srs_acct)/len(rec_srs_acct)

        print('\tDividing chunks...')
        recs_per_item = min(max(int(len(rec_ids_acct)*0.1),1),limit)
        rec_ids_acct = list(divide_chunks(rec_ids_acct, recs_per_item))
        rec_srs_acct = list(divide_chunks(rec_srs_acct, recs_per_item))
        rec_uris_acct = list(divide_chunks(rec_uris_acct, recs_per_item))

        # determine queue
        if os.environ.get('AWS_SECRET')=='PROD':
            large_job_threshold = 250
        else:
            large_job_threshold = 10
        if account==0:
            if len(rec_ids_acct)<=large_job_threshold and mean_sr<=192000:
                print('using sieve normal queue...')
                queue = get_queue(sqs, os.environ.get('SQS_QUEUE').split(':')[-1])
            else:
                print('using sieve large queue...')
                queue = get_queue(sqs, os.environ.get('SQS_QUEUE_LARGE').split(':')[-1])
        elif account==1:
            if len(rec_ids_acct)<=large_job_threshold and mean_sr<=192000:
                print('using rfcx normal queue...')
                queue = get_queue(rfcx_sqs, os.environ.get('SQS_QUEUE').split(':')[-1]) # the queues have the same names in both accounts
            else:
                print('using rfcx large queue...')
                queue = get_queue(rfcx_sqs, os.environ.get('SQS_QUEUE_LARGE').split(':')[-1])

        print(time.time() - t0)

        print('\tQueueing '+str(len(rec_ids_acct))+' items...')
        for (i,j,k) in zip(rec_ids_acct, rec_srs_acct, rec_uris_acct):
            send_message(queue, json.dumps({"worker_id":worker_id,
                                            "rec_ids": i,
                                            "rec_srs": j,
                                            "rec_uris": k,
                                            "project_id":proj_id,
                                            "job_id":job_id,
                                            "playlist_id":event['playlist_id'],
                                            "Amplitude Threshold":float(event['Amplitude Threshold']),
                                            "Duration Threshold":float(event['Duration Threshold']),
                                            "Bandwidth Threshold":float(event['Bandwidth Threshold']),
                                            "Area Threshold":float(event['Area Threshold']),
                                            "Filter Size":int(event['Filter Size']),
                                            "Min Frequency":float(event['Min Frequency']),
                                            "Max Frequency":float(event['Max Frequency'])
                        }))
                        
            worker_id += 1

    upd = jobs.update(jobs.c.job_id==job_id).values(progress_steps=max(worker_id, 1),
                                                    ncpu=max(worker_id, 1))
    session.execute(upd)
    
    session.commit()
    session.close()  

    return {
        'status': 200,
    }


def get_queue(resource, name):
    """
    Gets an SQS queue by name.

    :param name: The name that was used to create the queue.
    :return: A Queue object.
    """
    try:
        queue = resource.get_queue_by_name(QueueName=name)
        logger.info("Got queue '%s' with URL=%s", name, queue.url)
    except ClientError as error:
        logger.exception("Couldn't get queue named %s.", name)
        raise error
    else:
        return queue


def send_message(queue, message_body, message_attributes=None):
    """
    Send a message to an Amazon SQS queue.

    Usage is shown in usage_demo at the end of this module.

    :param queue: The queue that receives the message.
    :param message_body: The body text of the message.
    :param message_attributes: Custom attributes of the message. These are key-value
                              pairs that can be whatever you want.
    :return: The response from SQS that contains the assigned message ID.
    """
    if not message_attributes:
        message_attributes = {}

    try:
        response = queue.send_message(
            MessageBody=message_body,
            MessageAttributes=message_attributes
        )
    except ClientError as error:
        logger.exception("Send message failed: %s", message_body)
        raise error
    else:
        return response
    
