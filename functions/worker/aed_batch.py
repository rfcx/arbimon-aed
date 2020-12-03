from aed_lib import *
import h5py
from db import connect
import sqlalchemy as sqal
import datetime as dt
import time # testing

session, engine, metadata = connect() # RDS connection

recordings = sqal.Table('recordings', metadata, autoload=True, autoload_with=engine)
aeds = sqal.Table('audio_event_detections_clustering', metadata, autoload=True, autoload_with=engine)

FILT_PCTL = 0.95

def handler(event):

    #--- user inputs
        # recording_id
        # project_id
        # job_id
        # worker_id
        # amplitude threshold
        # filter size
        # size threshold

    rec_ids = [int(i) for i in np.sort(np.array(event['recording_id']))]
    proj_id = event['project_id']
    job_id = event['job_id']

    # define variables
    temp_dir = '/tmp/'
    rec_dir = temp_dir+'/recordings/'
    image_dir = temp_dir+'/images'
    det_dir = temp_dir+'/detection_data/'
    feature_file_prefix = temp_dir+'/'+str(job_id)+'_'+str(event['worker_id'])

    #--- create temp directories
    if not os.path.exists(temp_dir): ########################################################### delete
        os.mkdir(temp_dir)
        os.mkdir(rec_dir)
        os.mkdir(image_dir)
        os.mkdir(det_dir)
    else:
        shutil.rmtree(temp_dir)
        os.mkdir(temp_dir)
        os.mkdir(rec_dir)
        os.mkdir(image_dir)
        os.mkdir(det_dir)

    #--- find the recording URIs for downloading
    query = sqal.select([recordings.c.uri,
                         recordings.c.datetime]).where(recordings.columns.recording_id.in_(rec_ids)) \
                        .order_by(recordings.c.recording_id)
    result = session.execute(query).fetchall()
    rec_uris = [i[0] for i in result]
    rec_dts = [i[1] for i in result]
    rec_dts = [(i.hour+i.minute/60)/24 for i in rec_dts] # convert datetime to fraction of day
    rec_dts = [to_unitcirc(i) for i in rec_dts]

    #--- process recordings
    for n, rec in enumerate(rec_uris[:1]):

        print(rec)

        image_uri = 'audio_events/'+os.environ['AWS_SECRET'].lower()+'/detection/'+str(job_id)+'/png/'+str(rec_ids[n])+'/'

        if not os.path.exists(image_dir+'/'+str(rec_ids[n])):
            os.mkdir(image_dir+'/'+str(rec_ids[n]))

        t0 = time.time()

        #--- download recording and compute spectrogram
        f, t, S = download_and_get_spec(rec, os.environ['RECBUCKET'], rec_dir);

        #--- detect events
        objs = find_events(S, f, t, event['Filter Size'][0], event['Filter Size'][1], FILT_PCTL, event['Amplitude Threshold'], event['Size Threshold'][0], event['Size Threshold'][1])
        print(len(objs))

        print(time.time() - t0)

        #--- bulk insert audio events to db
        result = session.execute(

            aeds.insert(),

            [{'job_id': int(job_id),
              'recording_id': int(rec_ids[n]),
              'time_min': float(t[ob[1].start]),
              'time_max': float(t[ob[1].stop-1]),
              'frequency_min': float(f[ob[0].start]),
              'frequency_max': float(f[ob[0].stop-1]),
              'aed_number': int(c),
              'uri_image': image_uri+str(c)+'.png'
             }

             for c, ob in enumerate(objs)]

        )
        session.commit()

        #--- compute audio event features
        compute_features(objs, rec_ids[n], rec_dts[n], S, f, t, feature_file_prefix)

        #--- store roi images
        store_roi_images(S, objs, rec_ids[n], image_dir, image_uri)

    #--- query for aed_ids
    print('mapping ids...')
    query = sqal.select([aeds.c.aed_id, \
                         aeds.c.recording_id, \
                         aeds.c.aed_number]).where(sqal.and_(aeds.c.job_id==job_id, \
                                                             aeds.c.recording_id.in_(rec_ids)))
    result = session.execute(query).fetchall()
    key_dict = dict({i[1:]:i[0] for i in result}) # dictionary mapping recording and aed_number to aed_key

    aed_ids = np.load(feature_file_prefix+'_ids.npy') # file contains ae recording ids and ae number
    aed_ids = [key_dict[tuple(i)] for i in aed_ids]
    np.save(feature_file_prefix+'_ids.npy', aed_ids) # file now contains list of aed_ids from database

    session.close()

    #--- upload to S3
    s3.Bucket(os.environ['WRITEBUCKET']).upload_file(feature_file_prefix+'_features.npy', 
                                                     'audio_events/'+os.environ['AWS_SECRET'].lower()+'/detection/'+str(job_id)+(feature_file_prefix+'_features.npy').split(temp_dir)[-1])
    s3.Bucket(os.environ['WRITEBUCKET']).upload_file(feature_file_prefix+'_ids.npy', 
                                                     'audio_events/'+os.environ['AWS_SECRET'].lower()+'/detection/'+str(job_id)+(feature_file_prefix+'_ids.npy').split(temp_dir)[-1])

    shutil.rmtree(temp_dir)

    return {'status' : 200}



