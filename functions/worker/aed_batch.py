from aed_lib import *
from db import connect
import sqlalchemy as sqal
import datetime as dt
import time # testing
import shutil
import ast

session, engine, metadata = connect() # RDS connection 

aeds = sqal.Table('audio_event_detections_clustering', metadata, autoload=True, autoload_with=engine)
playlist_aed = sqal.Table('playlist_aed', metadata, autoload=True, autoload_with=engine)
jobs = sqal.Table('jobs', metadata, autoload=True, autoload_with=engine)

FILT_PCTL = 0.95


def handler(event, context):

    # user inputs
        # worker_id
        # recording_ids
        # project_id
        # job_id
        # worker_id
        # amplitude threshold
        # filter size
        # duration threshold
        # bandwidth threshold
        # area threshold
        # playlist id

    event = ast.literal_eval(event['Records'][0]["body"])

    worker_id = event['worker_id']
    rec_ids = event['rec_ids']
    rec_srs = event['rec_srs']
    rec_uris = event['rec_uris']
    proj_id = event['project_id']
    job_id = event['job_id']
    plist_id = int(event['playlist_id'])
    print(str(len(rec_ids)), ' recs to process.')

    # define variables
    temp_dir = '/tmp/temp/'
    rec_dir = temp_dir+'/recordings/'
    image_dir = temp_dir+'/images'
    det_dir = temp_dir+'/detection_data/'
    feature_file_prefix = temp_dir+'/'+str(job_id)+'_'+str(event['worker_id'])

    # create temp directories
    if os.path.exists(temp_dir): 
        shutil.rmtree(temp_dir)
        os.mkdir(temp_dir)
        os.mkdir(rec_dir)
        os.mkdir(image_dir)
        os.mkdir(det_dir)
    else:
        os.mkdir(temp_dir)
        os.mkdir(rec_dir)
        os.mkdir(image_dir)
        os.mkdir(det_dir)     
    

    # process recordings
    unprocessed = 0
    for n, rec in enumerate(rec_uris):

        # try:
            
        image_uri = 'audio_events/'+os.environ['AWS_SECRET'].lower()+'/detection/'+str(job_id)+'/png/'+str(rec_ids[n])+'/'

        if not os.path.exists(image_dir+'/'+str(rec_ids[n])):
            os.mkdir(image_dir+'/'+str(rec_ids[n]))

        t0 = time.time()

        # download recording and compute spectrogram
        f, t, S = download_and_get_spec(rec, os.environ['RECBUCKET'], rec_dir, rec_srs[n]);

        # detect events
        objs = find_events(S, f, t,
                            event['Filter Size'], 
                            FILT_PCTL, 
                            event['Amplitude Threshold'], 
                            event['Bandwidth Threshold'], 
                            event['Duration Threshold'],
                            event['Area Threshold'],
                            [event['Min Frequency'], event['Max Frequency']]
        )
        print(len(objs))

        if len(objs)>0:
            
            # bulk insert audio events to db
            result = session.execute(
    
                aeds.insert(),
    
                [{'job_id': int(job_id),
                    'recording_id': int(rec_ids[n]),
                    'time_min': float(ob['t0']),
                    'time_max': float(ob['t1']),
                    'frequency_min': float(ob['f0']),
                    'frequency_max': float(ob['f1']),
                    'aed_number': int(c),
                    'uri_image': image_uri+str(c)+'.png'
                    }
    
                    for c, ob in enumerate(objs)]
    
            )
            session.commit()
    
            # compute audio event features
            compute_features(objs, rec_ids[n], S, f, t, feature_file_prefix, image_dir, image_uri)

                    
        # except Exception as e:
        #     print(e)
        #     print('recording number: ',str(n))
        #     print('recording: ',rec)
        #     unprocessed+=1

    # query for aed_ids
    print('mapping ids...')
    query = sqal.select([aeds.c.aed_id, \
                         aeds.c.recording_id, \
                         aeds.c.aed_number]).where(sqal.and_(aeds.c.job_id==job_id, \
                                                             aeds.c.recording_id.in_(rec_ids)))
    result = session.execute(query).fetchall()
    key_dict = dict({i[1:]:i[0] for i in result}) # dictionary mapping recording and aed_number to aed_key

    if len(key_dict)==0: # if no AEDs were found, finish
        print('updating job status')
        upd = jobs.update(jobs.c.job_id==event['job_id']).values(state='processing',
                                                                 progress=jobs.c.progress+1,
                                                                 last_update=dt.datetime.now())
        session.execute(upd)
        session.commit()   
        return {'status' : 200}

    aed_ids = np.load(feature_file_prefix+'_ids.npy') # file contains ae recording ids and ae number
    aed_ids = [int(key_dict[tuple(i)]) for i in aed_ids]
    np.save(feature_file_prefix+'_ids.npy', aed_ids) # file now contains list of aed_ids from database
    
    # write to playlist_aeds
    result = session.execute(

        playlist_aed.insert(),

        [{
            'playlist_id': plist_id,
            'aed_id': aed_ids[c],
         }
         for c in range(len(aed_ids))]
    )
    session.commit()

    # upload to S3
    s3.Bucket(os.environ['WRITEBUCKET']).upload_file(feature_file_prefix+'_features.npy', 
                                                     'audio_events/'+os.environ['AWS_SECRET'].lower()+'/detection/'+str(job_id)+(feature_file_prefix+'_features.npy').split(temp_dir)[-1],
                                                     ExtraArgs={'ACL':'bucket-owner-full-control'})
    s3.Bucket(os.environ['WRITEBUCKET']).upload_file(feature_file_prefix+'_ids.npy', 
                                                     'audio_events/'+os.environ['AWS_SECRET'].lower()+'/detection/'+str(job_id)+(feature_file_prefix+'_ids.npy').split(temp_dir)[-1],
                                                     ExtraArgs={'ACL':'bucket-owner-full-control'})
                                                     
    if unprocessed/len(rec_ids) < 0.5:
        print('updating job status')
        upd = jobs.update(jobs.c.job_id==event['job_id']).values(state='processing',
                                                                 progress=jobs.c.progress+1,
                                                                 last_update=dt.datetime.now())
        session.execute(upd)
        session.commit()
    else: # if >=50% of recordings in the batch could not be processed, change job state to an error
        print('error: '+str(unprocessed*100/len(rec_ids))+'% of recordings could not be processed')
        upd = jobs.update(jobs.c.job_id==event['job_id']).values(state='error',
                                                                 remarks='At least 1 lambda could not process '+str(round(unprocessed*100/len(rec_ids)))+'% of recordings',
                                                                 last_update=dt.datetime.now())
        session.execute(upd)
        session.commit()
        
    session.close()

    return {'status' : 200}



