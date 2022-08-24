import json
import boto3
from db import connect
import sqlalchemy as sqal
import datetime as dt
import time
# from statistics import mean # test

session, engine, metadata = connect() # RDS connection

client = boto3.client('lambda')

# Function to break playlist into chunks
def divide_chunks(l, n): 
    for i in range(0, len(l), n):  
        yield l[i:i + n] 

def driver(event, context):
    
    # event stores:
    #   Playlist ID
    #   Us ID
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
                   ', "Area Threshold": '+str(event['Filter Size']) + \
                   ', "Filter Size": '+str(event['Filter Size']) + \
                   '}'
                        
    ins = job_params.insert().values(name=event['name'],
                                  project_id=proj_id,
                                  job_id=job_id,
                                  date_created=dt.datetime.now(),
                                  parameters=param_string,
                                  playlist_id=event['playlist_id'],
                                  user_id=event['user_id'])
    result = session.execute(ins)


    for account in sorted(lits(set(rec_accts))):

        # get recording IDs and sample rates for the current account
        rec_ids_acct = [rec_ids[i] for i in range(len(rec_accts)) if rec_accts[i]==account]
        rec_srs_acct = [rec_srs[i] for i in range(len(rec_accts)) if rec_accts[i]==account]


        print('\tSorting list by sample rate...')
        rec_srs_acct, rec_ids_acct = zip(*sorted(zip(rec_srs_acct, rec_ids_acct)))
        print(time.time() - t0)

    
        # Assign 10% of playlist to each item, with a limit per item
        if max(rec_srs_acct)<200000:
            limit = 300
        else:
            limit = 150
        mean_sr = sum(rec_srs_acct)/len(rec_srs_acct)

        print('\tDividing chunks...')
        recs_per_item = min(max(int(len(rec_ids_acct)*0.1),1),limit)
        rec_ids_acct = list(divide_chunks(rec_ids_acct, recs_per_item))
        rec_srs_acct = list(divide_chunks(rec_srs_acct, recs_per_item))

        # determine queue
        if os.environ.get('AWS_SECRET')=='PROD':
            large_job_threshold = 250
        else:
            large_job_threshold = 10
        if account==0:
            if len(rec_ids_acct)<=large_job_threshold and mean_sr<=192000:
                print('using sieve normal queue...')
                queue = get_queue(sqs, os.environ.get('SQS_QUEUE'))
            else:
                print('using sieve large queue...')
                queue = get_queue(sqs, os.environ.get('SQS_QUEUE_LARGE'))
        elif account==1:
            if len(rec_ids_acct)<=large_job_threshold and mean_sr<=192000:
                print('using rfcx normal queue...')
                queue = get_queue(rfcx_sqs, os.environ.get('SQS_QUEUE')) # the queues have the same names in both accounts
            else:
                print('using rfcx large queue...')
                queue = get_queue(rfcx_sqs, os.environ.get('SQS_QUEUE_LARGE'))

        print(time.time() - t0)

        print('\tQueueing '+str(len(rec_ids_acct))+' items...')
        for (i,j) in zip(rec_ids_acct, rec_srs_acct):
            send_message(queue, json.dumps({"worker_id":worker_id,
                                            "recording_ids": i,
                                            "sample_rates": j,
                                            "project_id":proj_id,
                                            "job_id":job_id,
                                            "worker_id":c,
                                            "playlist_id":event['playlist_id'],
                                            "Amplitude Threshold":float(event['Amplitude Threshold']),
                                            "Duration Threshold":float(event['Duration Threshold']),
                                            "Bandwidth Threshold":float(event['Bandwidth Threshold']),
                                            "Area Threshold":float(event['Area Threshold']),
                                            "Filter Size":int(event['Filter Size']),
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
    
