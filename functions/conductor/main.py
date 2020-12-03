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
    #   Job ID
    #   AED job name
    #   Amplitude threshold
    #   Filter size
    #   Size threshold
    
    t0 = time.time()

    print('AED job name: ' + event['name'])
    print('Playlist ID: ' + str(event['playlist_id']))
    
    print('DB connections...')
    plist_recs = sqal.Table('playlist_recordings', metadata, autoload=True, autoload_with=engine)
    plists = sqal.Table('playlists', metadata, autoload=True, autoload_with=engine)
    job_params = sqal.Table('job_params_audio_event_detection_clustering', metadata, autoload=True, autoload_with=engine)
    jobs = sqal.Table('jobs', metadata, autoload=True, autoload_with=engine)

    print('Querying...')
    # Get project_id using playlist_id
    query = sqal.select([plists.columns.project_id]).where(plists.columns.playlist_id==event['playlist_id'])
    proj_id = session.execute(query).fetchall()[0][0]
    
    # Get list of recording_id's
    query = sqal.select([plist_recs.columns.recording_id]).where(plist_recs.columns.playlist_id==event['playlist_id'])
    rec_ids = [i[0] for i in session.execute(query).fetchall()]
    
    # Assign 10% of playlist to each item, with a max of 300 per item
    recs_per_item = min(max(int(len(rec_ids)*0.1),1),300)
    rec_ids = list(divide_chunks(rec_ids, recs_per_item))
    
    # # Insert new job
    ins = jobs.insert().values(job_type_id=8,
                                date_created=dt.datetime.now(),
                                last_update=dt.datetime.now(),
                                project_id=proj_id,
                                user_id=event['user_id'],
                                state='processing',
                                progress=0,
                                completed=0,
                                progress_steps=len(rec_ids),
                                hidden=0,
                                ncpu=len(rec_ids))
    result = session.execute(ins)
    job_id = result.inserted_primary_key[0]

    # Insert to job_params_audio_event_detection_clustering
    param_string = '{"Amplitude Threshold": '+str(float(event['Amplitude Threshold'])) + \
                   ', "Size Threshold": '+str(event['Size Threshold']) + \
                   ', "Filter Size": '+str(event['Filter Size']) + '}'
                        
    ins = job_params.insert().values(name=event['name'],
                                  project_id=proj_id,
                                  job_id=job_id,
                                  date_created=dt.datetime.now(),
                                  parameters=param_string,
                                  playlist_id=event['playlist_id'])
    result = session.execute(ins)
    aed_id = result.inserted_primary_key[0]
    
    # Check if there are lambdas available for the job
    concurrent_limit = 1000
    query = sqal.select([jobs.c.progress,
                         jobs.c.progress_steps]).where(sqal.sql.and_(jobs.c.job_type_id == 6,
                                                                     jobs.c.date_created > (dt.datetime.now() - dt.timedelta(hours=2))))
    lamchk_res = session.execute(query).fetchall()  
    lamchk_res = sum([i[1] - i[0] for i in lamchk_res]) # sums lambdas in use
    if concurrent_limit - lamchk_res < len(rec_ids):
        print('Error: Resources not available')
        upd = jobs.update(jobs.c.job_id==job_id).values(state='error',
                                                                 remarks='Resources not currently available',
                                                                 last_update=dt.datetime.now())
        session.execute(upd)
        session.commit()
        return {'status' : -1}   
    
    session.commit()
    session.close()
    
    # times = [] # debugging

    print('Invoking '+str(len(rec_ids))+' items...')
    for i in rec_ids:
        # t0 = time.time() # debugging
        client.invoke(FunctionName='aed',
                    InvocationType='Event',
                    Payload=json.dumps({"recording_id":i,
                                        "project_id":proj_id,
                                        "job_id":job_id,
                                        "Amplitude Threshold":float(event['Amplitude Threshold']),
                                        "Filter Size":float(event['Filter Size']),
                                        "Size Threshold":float(event['Size Threshold'])
                    }))
        # t1 = time.time() # debugging
        # times.append(t1-t0) # debugging
        if context.get_remaining_time_in_millis()<20000:
            print('Running out of time, quitting to avoid auto-retry')
            return {'status': -1}
        
    # print('Average invoke time: ' + str(mean(times))) # debugging
    
    return {
        'status': 200,
    }
