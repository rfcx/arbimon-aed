#!/usr/bin/env python3
"""
rfcx-local combined Audio Event Detection (clustering) worker.
Phase D-ANALYSIS, job_type_id 8 ("Audio Event Detection for Clustering").

Collapses the AWS arbimon-aed two-stage design (conductor Lambda creates
the job + fans out per-chunk to worker Lambdas) into a single in-cluster
k8s Job invoked as:

    python3 aed_run_job.py <job_id>

The arbimon-legacy patch (analogous to the PM one) inserts a `jobs`
(job_type_id=8, state='waiting') + `job_params_audio_event_detection_clustering`
row; the dispatcher claims it (waiting->initializing) and runs this. Here
we read the job params, query all playlist recordings, run the worker's
AED over them as a single batch (worker_id=0), write detections +
playlist_aed + feature npy files, then set the terminal job state.

Reuses the upstream worker lib (aed_lib.find_events / compute_features /
store_roi_images / download_and_get_spec / to_unitcirc) verbatim; only
the orchestration (no Lambda fan-out / context) is new.

S3: aed_lib is patched to honor S3_ENDPOINT (-> s3-proxy). DB: db.py
falls back to env and uses ARBIMON_DB_USER.
"""
import os
import sys
import json
import shutil
import datetime as dt

import numpy as np
import sqlalchemy as sqal

from db import connect
from aed_lib import (
    find_events, compute_features, store_roi_images, download_and_get_spec,
    to_unitcirc,
)

FILT_PCTL = 0.95
TEMP_DIR = "/tmp/temp/"

def _fresh():
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
    for d in (TEMP_DIR, TEMP_DIR + "/recordings/", TEMP_DIR + "/images",
              TEMP_DIR + "/detection_data/"):
        os.makedirs(d, exist_ok=True)

def main(job_id):
    import boto3
    s3 = boto3.resource("s3", endpoint_url=os.environ.get("S3_ENDPOINT") or None)

    session, engine, metadata = connect()
    recordings = sqal.Table('recordings', metadata, autoload=True, autoload_with=engine)
    aeds = sqal.Table('audio_event_detections_clustering', metadata, autoload=True, autoload_with=engine)
    playlist_aed = sqal.Table('playlist_aed', metadata, autoload=True, autoload_with=engine)
    jobs = sqal.Table('jobs', metadata, autoload=True, autoload_with=engine)
    jparams = sqal.Table('job_params_audio_event_detection_clustering', metadata, autoload=True, autoload_with=engine)

    jp = session.execute(
        sqal.select([jparams.c.playlist_id, jparams.c.project_id, jparams.c.parameters])
        .where(jparams.c.job_id == job_id)
    ).fetchall()
    if not jp:
        _fail(session, jobs, job_id, "No job_params_audio_event_detection_clustering row")
        return 1
    plist_id, proj_id, params_json = int(jp[0][0]), jp[0][1], jp[0][2]
    p = json.loads(params_json) if params_json else {}
    amp = float(p.get("Amplitude Threshold", 0))
    dur = float(p.get("Duration Threshold", 0))
    bw = float(p.get("Bandwidth Threshold", 0))
    area = float(p.get("Area Threshold", 0))
    filt = int(p.get("Filter Size", 1))
    print(f"AED job_id={job_id} playlist={plist_id} proj={proj_id} "
          f"amp={amp} dur={dur} bw={bw} area={area} filt={filt}")

    session.execute(jobs.update().where(jobs.c.job_id == job_id).values(
        state='processing', last_update=dt.datetime.now()))
    session.commit()

    rec_rows = session.execute(
        sqal.select([recordings.c.recording_id, recordings.c.uri, recordings.c.datetime])
        .select_from(recordings.join(
            sqal.Table('playlist_recordings', metadata, autoload=True, autoload_with=engine),
            recordings.c.recording_id == sqal.literal_column('playlist_recordings.recording_id')))
        .where(sqal.literal_column('playlist_recordings.playlist_id') == plist_id)
        .order_by(recordings.c.recording_id)
    ).fetchall()
    rec_ids = [int(r[0]) for r in rec_rows]
    rec_uris = [r[1] for r in rec_rows]
    rec_dts_raw = [r[2] for r in rec_rows]
    total = len(rec_ids)
    print(f"playlist has {total} recordings")
    session.execute(jobs.update().where(jobs.c.job_id == job_id).values(
        progress=0, progress_steps=max(total, 1)))
    session.commit()

    # datetime -> unit circle (None-safe; many recs have 0000-00-00 -> None)
    rec_dts = []
    for d in rec_dts_raw:
        frac = ((d.hour + d.minute / 60) / 24) if d else 0.0
        rec_dts.append(to_unitcirc(frac))

    _fresh()
    rec_dir = TEMP_DIR + "/recordings/"
    image_dir = TEMP_DIR + "/images"
    feature_prefix = TEMP_DIR + "/" + str(job_id) + "_0"
    recbucket = os.environ.get("RECBUCKET", "rfcx-streams-production")
    writebucket = os.environ.get("WRITEBUCKET", "arbimon2")
    env = os.environ.get("AWS_SECRET", "prod").lower()

    unprocessed = 0
    any_features = False
    for n, rec in enumerate(rec_uris):
        try:
            image_uri = f"audio_events/{env}/detection/{job_id}/png/{rec_ids[n]}/"
            os.makedirs(image_dir + "/" + str(rec_ids[n]), exist_ok=True)
            f, t, S = download_and_get_spec(rec, recbucket, rec_dir)
            objs = find_events(S, f, t, filt, FILT_PCTL, amp, bw, dur, area)
            if len(objs) > 0:
                session.execute(aeds.insert(), [{
                    'job_id': int(job_id), 'recording_id': int(rec_ids[n]),
                    'time_min': float(t[ob[1].start]), 'time_max': float(t[ob[1].stop - 1]),
                    'frequency_min': float(f[ob[0].start]), 'frequency_max': float(f[ob[0].stop - 1]),
                    'aed_number': int(c), 'uri_image': image_uri + str(c) + '.png',
                } for c, ob in enumerate(objs)])
                session.commit()
                compute_features(objs, rec_ids[n], rec_dts[n], S, f, t, feature_prefix)
                store_roi_images(S, objs, rec_ids[n], image_dir, image_uri)
                any_features = True
        except Exception as e:
            print("unprocessed:", rec, e)
            unprocessed += 1
        finally:
            session.execute(jobs.update().where(jobs.c.job_id == job_id).values(
                progress=n + 1, last_update=dt.datetime.now()))
            session.commit()

    # map aed_ids + write playlist_aed + upload feature files (only if any)
    if any_features and os.path.exists(feature_prefix + "_ids.npy"):
        rows = session.execute(
            sqal.select([aeds.c.aed_id, aeds.c.recording_id, aeds.c.aed_number])
            .where(sqal.and_(aeds.c.job_id == job_id, aeds.c.recording_id.in_(rec_ids)))
        ).fetchall()
        key = {tuple(r[1:]): r[0] for r in rows}
        aed_ids = np.load(feature_prefix + "_ids.npy")
        aed_ids = [int(key[tuple(i)]) for i in aed_ids]
        np.save(feature_prefix + "_ids.npy", aed_ids)
        if aed_ids:
            session.execute(playlist_aed.insert(),
                            [{'playlist_id': plist_id, 'aed_id': a} for a in aed_ids])
            session.commit()
        for suffix in ("_features.npy", "_ids.npy"):
            fp = feature_prefix + suffix
            if os.path.exists(fp):
                s3.Bucket(writebucket).upload_file(
                    fp, f"audio_events/{env}/detection/{job_id}/{job_id}_0{suffix}")

    state = 'completed'
    remark = None
    if total and unprocessed / total >= 0.5:
        state = 'error'
        remark = f"{round(unprocessed*100/total)}% of recordings could not be processed"
    session.execute(jobs.update().where(jobs.c.job_id == job_id).values(
        state=state, completed=(1 if state == 'completed' else -1),
        progress=max(total, 1), remarks=remark, last_update=dt.datetime.now()))
    session.commit()
    session.close()
    engine.dispose()
    print(f"AED job {job_id} {state}: total={total} unprocessed={unprocessed}")
    return 0 if state == 'completed' else 1

def _fail(session, jobs, job_id, msg):
    print("FAIL:", msg)
    session.execute(jobs.update().where(jobs.c.job_id == job_id).values(
        state='error', completed=-1, remarks=msg[:500], last_update=dt.datetime.now()))
    session.commit()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: aed_run_job.py <job_id>")
        sys.exit(2)
    sys.exit(main(int(sys.argv[1].strip("'"))))