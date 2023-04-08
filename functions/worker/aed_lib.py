import os
import sys
import shutil
import subprocess
import numpy as np
from npy_append_array import NpyAppendArray
import soundfile as sf # for reading audio files
from scipy.signal import spectrogram, hann
import scipy.ndimage
import skimage.filters.rank
import skimage.util
import skimage.morphology
from skimage.feature import hog
import boto3
import botocore
import boto3.s3.transfer as s3transfer
from PIL import Image
from math import sin, cos, pi
eps = sys.float_info.epsilon
# establish s3 connection
s3 = boto3.resource('s3')


def find_events(S, f, t, filt_size_factor, pctl, amp_thresh, bandwidth_thresh, duration_thresh, area_thresh, freq_range):
    ''' Detects audio events in a spectrogram. Returns a list of slices describing coordinates of events
    Args:
        S:                      spectrogram array
        f:                      frequency array
        t:                      time array
        filt_size_factor:       percentile filter neighborhood size in pixels
        pctl:                   percentile for percentile filter
        amp_thresh:             amplitude threshold scale factor
        bandwidth_thresh:       minimum AED bandwidth
        duration_thresh:        minimum AED duration
        area_thresh:            minimum AED area
        freq_range:             frequency range of interest
    Returns:
        objs:                   objects storing coordinates of detected events
        f:                      cropped frequency array
    '''  

    # flatten
    S = band_flatten(S)

    # [-1, 1] normalization
    S -= S.min()
    S /= (S.max() - S.min())
    S *= 2
    S -= 1

    # crop
    S = S[((freq_range[0]*1000)<=f) & (f<=(freq_range[1]*1000)), :]
    f = f[((freq_range[0]*1000)<=f) & (f<=(freq_range[1]*1000))]

    # filter
    # filt_size_hertz_base = 200
    # filt_size_secs_base = 0.1
    # filt_size_y = int( filt_size_hertz_base/(f[1]-f[0]) * filt_size_factor)
    # filt_size_x = int( filt_size_secs_base/(t[1]-t[0]) * filt_size_factor)
    filt_size_y = int(filt_size_factor/2)
    filt_size_x = int(filt_size_factor*5)
    Sfilt = skimage.filters.rank.percentile(skimage.util.img_as_ubyte(S), skimage.morphology.rectangle(filt_size_y, filt_size_x), p0=pctl)

    # mask
    th = Sfilt.mean() + amp_thresh * Sfilt.std()
    mask = Sfilt > th

    # find events
    labels, num_labels = scipy.ndimage.measurements.label(mask)
    objs = scipy.ndimage.measurements.find_objects(labels)
    
    # filter events
    keeps = [i for i in range(len(objs)) if (f[objs[i][0].stop-1]-f[objs[i][0].start])>=bandwidth_thresh*1000 and \
                                            (t[objs[i][1].stop-1]-t[objs[i][1].start])>=duration_thresh and \
                                            (f[objs[i][0].stop-1]-f[objs[i][0].start])/1000*(t[objs[i][1].stop-1]-t[objs[i][1].start])>=area_thresh]
    objs = [objs[i] for i in keeps]
    
    return objs, f


def im_norm(x, trim=0.4):
    ''' Normalizes an image, and adjusts contrast
    Args:
        s:                      image
        trim:                   contrast adjustment factor
    Returns:
        normalized image
    '''  
    # normalizes an image, trim determines contrast, image min/max will be (trim/2)/(1-trim/2)
    return (x-x.min())/(x.max()-x.min())*(1-trim)+(trim/2)


def store_roi_images(S, objs, rec_id, worker_id, image_dir, image_uri):
    ''' Crops detection ROIs and uploads PNGs to S3
    Args:
        S:                      full spectrogram array
        objs:                   objects storing coordinates of detected events
        rec_id:                 recording ID
        worker_id:              worker ID
        image_dir:              local directory for detection images
        image_uri:              URI path for uploading image files
    Returns:
        normalized image
    '''  
    shutil.rmtree(image_dir)
    os.makedirs(image_dir)
    for c, ob in enumerate(objs):
        im = np.uint8(
            im_norm(
                -S[
                    ob[0].start : (ob[0].stop+1), 
                    ob[1].start : (ob[1].stop+1)
                    ])*255
                    )
        im = np.flipud(im)
        im = Image.fromarray(im).convert('RGB')
        im.save(image_dir+'/'+str(c)+'.png')
    fast_upload(boto3.Session(), os.environ['WRITEBUCKET'], image_uri, [image_dir+'/'+i for i in os.listdir(image_dir)])


def fast_upload(session, bucketname, s3dir, filelist, workers=20):
    ''' Uploads files to S3
    '''
    botocore_config = botocore.config.Config(max_pool_connections=workers)
    s3client = session.client('s3', config=botocore_config)
    transfer_config = s3transfer.TransferConfig(
        use_threads=True,
        max_concurrency=workers,
    )
    s3t = s3transfer.create_transfer_manager(s3client, transfer_config)
    for src in filelist:
        dst = os.path.join(s3dir, os.path.basename(src))
        s3t.upload(src, bucketname, dst, subscribers=[], extra_args={'ACL':'bucket-owner-full-control'})
    s3t.shutdown()  # wait for all the upload tasks to finish
        

def download_and_get_spec(uri, bucket, rec_dir, sr):
    ''' Downloads and audio recording and returns spectrogram
    Args:
        uri:                    URI of audio file to download
        bucket:                 bucket of audio file to download
        rec_dir:                local folder for storing audio files
        sr:                     audio recording sample rate from Arbimon database
    Returns:
        f:                      frequency array
        t:                      time array
        S:                      log-scaled spectrogram array
    '''  
    # download
    s3.Bucket(bucket).download_file(uri, rec_dir + uri.replace('/','_'))

    # load 
    data, samplerate = read_audio(uri, rec_dir, sr)
    print(samplerate)
    
    # compute spectrogram
    f, t, S = _spectrogram(data, samplerate)
    S = np.log10((S+eps))
    
    return f, t, S


def _spectrogram(x, fs, time_bin_size=0.02, freq_bin_size=50):
    ''' Wraps around scipy.signal.spectrogram with arguments for time/freq bin size
    Args:
        x:                      audio time series array
        fs:                     audio sample rate
        time_bin_size:          seconds per time bin
        freq_bin_size:          hertz per frequency bin
    Returns:
        f:                      frequency array
        t:                      time array
        Sxx:                    raw spectrogram array
    '''  
    nfft = fs//freq_bin_size
    nperseg = nfft
    time_bin_samples = int(time_bin_size*fs)
    assert nperseg >= time_bin_samples, 'Error: time_bin_size too large for freq_bin_size'
    noverlap = nperseg - time_bin_samples
    
    f, t, Sxx = spectrogram(x, fs, nperseg=nperseg, window=hann(nperseg), noverlap=noverlap, nfft=nfft)
    
    return f, t, Sxx
    
    
def compute_features(objs, rec_id, S, f, t, out_file_prefix):
    ''' Computes features from audio events for a single recording and appends to two local files

        <out_file_prefix>_features.npy
        <out_file_prefix>_ids.npy

    Args:
        objs:                   objects storing AED coordinates
        rec_id:                 recording ID
        S:                      log-scaled spectrogram array
        f:                      frequency array
        t:                      time array
        out_file_prefix:        path and prefix for output files
    Returns:
    '''     

    block_features = np.zeros((len(objs), 581))
    block_ids = np.zeros((len(objs), 2))
    for c, ob in enumerate(objs):
        roi = S[ob[0].start:ob[0].stop-1, ob[1].start:ob[1].stop-1]
        roi = np.array(Image.fromarray(roi).resize((20, 20)))
        features = hog(roi, orientations=9, pixels_per_cell=(4, 4), cells_per_block=(2, 2))
        block_features[c,:] = np.hstack([
                                        np.array([f[ob[0].start],                  # low frequency
                                                  f[ob[0].stop-1],                 # high frequency
                                                  t[ob[1].start],                  # start time
                                                  t[ob[1].stop-1],                 # end time
                                                  rec_id,
                                                 ]),
                                        features
                                ])
        block_ids[c,:] = [rec_id, c] # recording_id and aed_number
        
    npaa = NpyAppendArray(out_file_prefix+'_features.npy')
    npaa.append(block_features)                    
    npab = NpyAppendArray(out_file_prefix+'_ids.npy')
    npab.append(block_ids)       


def band_flatten(X):
    ''' Removes the median from each frequency band
    Args:
        X:                      spectrogram
    Returns:
        flattened spectrogram
    '''  
    band_medians = np.percentile(X, 50, axis=1).tolist()
    fn = lambda r, m: r - m.pop(0)
    return np.apply_along_axis(fn, 1, X, band_medians)


def read_audio(uri, rec_dir, sample_rate):
    ''' Loads audio data into memory
    Args:
        uri:                URI of audio file
        rec_dir:            local folder for storing audio files
        sample_rate:        audio sample rate
    Returns:
        data:               audio time series array
        samplerate:         audio sample rate
    '''  
    # convert uri to local-file format
    i = uri.replace('/','_')
    
    # handle opus recordings
    if uri.split('.')[-1] == 'opus':
        process = subprocess.Popen(['/opt/exodus/bundles/e4572ada66bc59e727a83ddabea44280afc8a72dbbb36d9aa1e7f043c2104c63/usr/bin/opusdec', rec_dir+i, rec_dir+i.replace('.opus', '.wav'), '--rate', str(sample_rate)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        i = i.replace('.opus', '.wav')

    # load
    data, samplerate, read_err = _read_audio(rec_dir+i)
    if read_err:
        print('Warning: Ran into an unreadable block. File partially read')
        
    return data, samplerate


def _read_audio(path, mono=True, offset=0.0, duration=None, dtype=np.float32):
    ''' Loads audio data into memory
    '''  
    y = []
    e_status=0
    with sf.SoundFile(os.path.realpath(path)) as input_file:
        sr = input_file.samplerate
        n_channels = input_file.channels

        fileLen = len(input_file)
        
        while True:
            try:
                block = input_file.read(input_file.samplerate, 'float32', False, None, None)
                y.append(block)
                if len(y) >= len(input_file)/input_file.samplerate:
                    break
            except:
                e_status=1
                break

    if y:
        y = np.concatenate(y)

        if n_channels > 1:
            y = y.reshape((-1, n_channels)).T
            if mono:
                y = y[0,:]
            
    # Final cleanup for dtype and contiguity
    y = np.ascontiguousarray(y, dtype=dtype)

    return y, sr, e_status

