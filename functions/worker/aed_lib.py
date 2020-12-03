import os
import shutil
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
import time
from PIL import Image
from math import sin, cos, pi
# establish s3 connection
s3 = boto3.resource('s3')


def find_events(S, f, t, filt_height, filt_width, pctl, amp_thresh, freq_size_thresh, time_size_thresh):
    
    # Detects audio events in a spectrogram. Returns a list of slices describing coordinates of events
    
#     print('flatten')
    S = band_flatten(S)

#     print('filter')
    S += -S.min()
    S *= (1.0/S.max())
    Sfilt = skimage.filters.rank.percentile(skimage.util.img_as_ubyte(S), skimage.morphology.rectangle(filt_height, filt_width), p0=pctl)

    # th = np.median(Sfilt.flatten())+amp_thresh*mad(Sfilt.flatten())
    th = np.median(Sfilt.flatten())+amp_thresh*np.std(Sfilt.flatten())
    mask = Sfilt > th

#     print('find objs')
    labels, num_labels = scipy.ndimage.measurements.label(mask)
    objs = scipy.ndimage.measurements.find_objects(labels)

#     print('discard')
    keeps = [i for i in range(len(objs)) if (f[objs[i][0].stop-1]-f[objs[i][0].start])*(t[objs[i][1].stop-1]-t[objs[i][1].start]) >= (time_size_thresh*freq_size_thresh)]
    objs = [objs[i] for i in keeps]

    return objs


def im_norm(x, trim=0.4):
    # normalizes an image, trim determines contrast, image min/max will be (trim/2)/(1-trim/2)
    return (x-x.min())/(x.max()-x.min())*(1-trim)+(trim/2)


def store_roi_images(S, objs, rec_id, image_dir, image_uri):
    for c, ob in enumerate(objs):
        im = np.uint8(im_norm(-S[ob[0], ob[1]])*255)
        im = Image.fromarray(im).convert('RGB')
        im.save(image_dir+'/tmp.png')
        s3.Bucket(os.environ['WRITEBUCKET']).upload_file(image_dir+'/tmp.png',
                                                         image_uri+str(c)+'.png')

        
def download_and_get_spec(uri, bucket, rec_dir, winlen=256, nfft=256, noverlap=128):

    # Downloads a recording and computes spectrogram

    s3.Bucket(bucket).download_file(uri, rec_dir + uri.replace('/','_'))

    # Load recording
    data, samplerate, read_err = read_audio_dev(rec_dir + uri.replace('/','_'))
    if read_err:
        print('Warning: Ran into an unreadable block. File partially read')

    # Compute spectrogram
    f, t, S = spectrogram(data, samplerate, window=hann(winlen), nfft=nfft, noverlap=noverlap)
    S = 10*np.log10((S+1e-12))
    
    return f, t, S
    
    
def compute_features(objs, rec_id, rec_dt, S, f, t, out_file_prefix):
    
    # Computes features from audio events for a single recording and appends to two local files
    #   <out_file_prefix>_features.npy
    #   <out_file_prefix>_ids.npy

    print('saving features')
    
    block_features = np.zeros((len(objs), 581))
    block_ids = np.zeros((len(objs), 2))
    for c, ob in enumerate(objs):
        roi = S[ob[0].start:ob[0].stop-1, ob[1].start:ob[1].stop-1]
        roi = np.array(Image.fromarray(roi).resize((20, 20)))
        features = hog(roi, orientations=9, pixels_per_cell=(4, 4), cells_per_block=(2, 2))
        block_features[c,:] = np.hstack([
                                        np.array([rec_dt[0],                       # time of day unit circle coordinates
                                                  rec_dt[1],
                                                  f[ob[0].start],                  # low frequency
                                                  f[ob[0].stop-1],                 # high frequency
                                                  t[ob[1].stop-1]-t[ob[1].start]   # duration
                                                 ]),
                                        features
                                ])
        block_ids[c,:] = [rec_id, c] # recording_id and aed_number
        
    npaa = NpyAppendArray(out_file_prefix+'_features.npy')
    npaa.append(block_features)                    
    npab = NpyAppendArray(out_file_prefix+'_ids.npy')
    npab.append(block_ids)       
    
    
def to_unitcirc(n):
    theta = 2 * pi * n
    return (cos(theta), sin(theta))


def band_flatten(X, percentile=50, divide=False):
    band_medians = np.percentile(X, percentile, axis=1).tolist()
    if divide:
        fn = lambda r, m: r / m.pop(0)
    else:
        fn = lambda r, m: r - m.pop(0)
    return np.apply_along_axis(fn, 1, X, band_medians)


def read_audio_dev(path, mono=True, offset=0.0, duration=None,
         dtype=np.float32):

    y = []
    e_status=0
    with sf.SoundFile(os.path.realpath(path)) as input_file:
        sr_native = input_file.samplerate
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
                y = to_mono(y)

        else:
            sr = sr_native
            
    # Final cleanup for dtype and contiguity
    y = np.ascontiguousarray(y, dtype=dtype)

    return y, sr, e_status
