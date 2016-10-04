# db_bunker.py
import subprocess
import dropbox # download dropbox python sdk 'pip install dropbox'
import re
import shutil
import os
import time
import pickle
import smtplib

# my local unix location is /fs/ext1
# from there, create the following folders:
# /fs/ext1/db_bunker
# /fs/ext1/db_bunker/db_root
# /fs/ext1/db_bunker/log
# /fs/ext1/db_bunker/metadata

dropbox_local_path = '/fs/ext1/db_bunker/db_root' # local path for top level dropbox bunker
prefix = None  #None # set to None (without quotes) for all of dropbox account or set to '/x/y/z' folder structure (you will see folder x in main view on www.dropbox.com after login)
start_time = time.strftime("%Y%m%d_%H%M%S",time.localtime())
log_file = '/fs/ext1/db_bunker/log/db_bunker_' + start_time + '.log'
l_meta_file = '/fs/ext1/db_bunker/metadata/l_files.pickle'
r_meta_file = '/code/db_bunker/metadata/l_files.pickle'
meta_rev_file = '/fs/ext1/db_bunker/metadata/l_files_rev.txt'
incr_bytes = 10485760
# Gather account info and utilization details
client = dropbox.client.DropboxClient('xxxxxxx') # create an app on dropbx and copy you token here
account_info = client.account_info()
db_quota_info = client.account_info()['quota_info']
db_total_size = db_quota_info['quota']
db_used_size = db_quota_info['normal']

smtp_hub_name = 'smtp.gmail.com'
smtp_hub_port = 587
smtp_user = 'xxxxxxx@gmail.com'
smtp_pass = 'xxxxxxxx'
to_addr = 'xxxxxxxxx@hotmail.com'
from_addr = smtp_user

# logging function
def logger(arg):
    print(arg)
    datetime_stamp = '[' + time.strftime("%Y%m%d_%H%M%S",time.localtime()) + ']  '
    out = open(log_file,'a')
    out.write('\r\n' + datetime_stamp + arg)
    out.close()

# smtp function
def smtp_send(arg_subj,arg_body):
    logger('Attempting to send mail:\n  TO: ' + to_addr + '\n  SUBJ: ' + arg_subj + '\n  BODY:\n  ' + arg_body)
    smtp_header = 'To: ' + to_addr + '\n' + 'From: ' + from_addr + '\n' + 'Subject: ' + arg_subj
    s = smtplib.SMTP(smtp_hub_name,smtp_hub_port)
    s.ehlo()
    s.starttls()
    s.ehlo()
    s.login(smtp_user,smtp_pass)
    s.sendmail(from_addr,to_addr,smtp_header + '\n\n' + arg_body)
    s.quit()

# check for running db_bunker
def db_bunker_running():
    proc_running = 0
    for line in os.popen('ps -ef'):
        if re.search('/db_bunker.py',line):
            proc_running = proc_running + 1
            logger(line)
    if proc_running < 3:
        logger('db_bunker.py is not running, proceeding...')
    else:
        logger('db_bunker.py is already running, exiting...')
        exit(0)

# create directories
def dir_maker(arg_dirs):
    dir_path = dropbox_local_path
    dirs = arg_dirs.split('/')[1:]
    for d in dirs:
        dir_path = (dir_path + '/' + d).lower()
        # create directory if needed
        if os.path.exists(dir_path) == False:
            logger('Directory not found; creating: ' + dir_path)
            os.mkdir(dir_path)

# function which does all of the work; mostly based on an example from the dropbox python api example
def list_files(client, files=None, cursor=None):
    cnt_files = 0
    tot_bytes = 0
    if files is None:
        files = {}
    has_more = True
    while has_more:
        result = client.delta(cursor,path_prefix=prefix,include_media_info=True)
        cursor = result['cursor']
        has_more = result['has_more']
        for lowercase_path, metadata in result['entries']:
            downloaded = False
            if metadata is not None:
                files[lowercase_path] = metadata
            else:
                # no metadata indicates a deletion
                # remove if present
                files.pop(lowercase_path, None)
                # in case this was a directory, delete everything under it
                for other in files.keys():
                    if other.startswith(lowercase_path + '/'):
                        del files[other]
            r_path = (metadata['path']).lower()
            l_path = (dropbox_local_path + r_path).lower()
            if metadata['is_dir'] == True:
                # ensure folder exists
                dir_maker(r_path)
                continue
            # file name and location preparation
            r_size = metadata['bytes']
            r_rev = metadata['rev']
            file_local_valid = True
            logger('Processing file: ' + r_path + ',' + str(r_size) + ',' + r_rev)
            # Check local metadata for file existence
            if r_path in l_files:
                logger('  File found in local hash.  Checking revision')
                if l_files[r_path]['REV'] == r_rev:
                    logger('  Revision matches.  No action required')
                else:
                    logger('  Revision does not match.  Downloading...')
                    l_files[r_path] = {'REV':r_rev,'SIZE':r_size}
            else:
                logger('  File not found in local hash.  Downloading...')
                out = open(l_path, 'wb')
                # Check for large file
                if r_size > 20971520:
                    incr = r_size//incr_bytes
                    rem_bytes = r_size%incr_bytes
                    logger('  Large file; breaking into ' + str(incr) + ' increment(s) with a remainder of ' + str(rem_bytes) + ' bytes')
                    b_start = 0
                    for i in range(0,incr):
                        with client.get_file(r_path,start=b_start,length=incr_bytes) as f:
                            out.write(f.read())
                        b_start = b_start + incr_bytes
                    if rem_bytes > 0:
                        with client.get_file(r_path,start=b_start,length=rem_bytes) as f:
                            out.write(f.read())
                else:
                    with client.get_file(r_path) as f:
                        out.write(f.read())
                out.close()
                logger('  Download Complete')
                l_files.update({r_path:{'REV':r_rev,'SIZE':r_size}})
                cnt_files = cnt_files + 1
                tot_bytes = tot_bytes + r_size
                if (cnt_files%10) == 0:
                    logger('##### Files processed:  ' + str(cnt_files) + '; Running Total Bytes:  ' + format(tot_bytes,',d'))
                if (cnt_files%20) == 0 or r_size > 20971520:
                    # Write out hash and upload to dropbox
                    logger('Dumping pickle file')
                    pickle_dump(l_meta_file,l_files)
                    logger('Copying pickle file to dropbox')
                    d_meta_rev = (copy_to_dropbox(l_meta_file,r_meta_file))['rev']
                    set_meta_rev(d_meta_rev,meta_rev_file)
                    logger('New revision ' + d_meta_rev + ' added to revision file:  ' + meta_rev_file)
    return files,cursor,cnt_files,tot_bytes

def pickle_load(pf):
    # ensure pickle file is present
    if os.path.exists(pf) == False:
        logger('Pickle file missing:  ' + pf)
        data = {}
    # ensure pickel file has data present
    elif os.path.getsize(l_meta_file) < 1:
        logger('Pickle file size is 0 bytes')
        data = {}
    # open pickle file
    else:
        with open(pf,'rb') as f:
            data = pickle.load(f)
    return data

def pickle_dump(pf,data):
    with open(pf,'wb') as f:
        pickle.dump(data,f)

def copy_to_dropbox(l_file,r_file):
    with open(l_file,'rb') as f:
        response = client.put_file(r_file,f,overwrite=True)
    return response

def copy_from_dropbox(l_file,r_file,drev):
    # initial rev check
    if drev != 'None':
        rev = (client.metadata(r_file,rev=drev))['rev']
        if rev != drev:
            logger('  Local and remote pickle revisions do not match.  Exiting...')
            exit(1)
        with open(l_file,'wb') as f:
            d,response = client.get_file_and_metadata(r_file,rev=drev)
            with d:
                f.write(d.read())
    else:
        with open(l_file,'wb') as f:
            d,response = client.get_file_and_metadata(r_file)
            with d:
                f.write(d.read())

def set_meta_rev(rev,rev_file):
    with open(rev_file,'w') as f:
        f.write(rev)

def get_meta_rev(rev_file):
    if os.path.exists(rev_file) == False:
        logger('rev file missing:  ' + rev_file + ', setting revision to None')
        rev = 'None'
    else:
        with open(rev_file,'r') as f:
            rev = f.read()
    return rev

# Determine if db_bunker is running already
logger('Starting db_runner.py')
db_bunker_running()

# Connect to dropbox
info = client.account_info()
logger('Starting db_bunker.py')
logger('Dropbox Account: ' + info['display_name'])

# Downloading pickle file and loading into hash
d_meta_rev = get_meta_rev(meta_rev_file)
logger('Downloading pickle file; rev:  ' + r_meta_file + "; " + d_meta_rev)
copy_from_dropbox(l_meta_file,r_meta_file,d_meta_rev)
logger('Loading pickle file')
l_files = pickle_load(l_meta_file)
#l_files = {}

# Start bunkering
logger('Starting Bunkering...')
files,cursor,cnt_files,tot_bytes = list_files(client)
logger('Bunkering complete')

# Write out hash and upload to dropbox
logger('Dumping pickle file')
pickle_dump(l_meta_file,l_files)
logger('Copying pickle file to dropbox')
d_meta_rev = (copy_to_dropbox(l_meta_file,r_meta_file))['rev']
set_meta_rev(d_meta_rev,meta_rev_file)
logger('New revision ' + d_meta_rev + ' added to revision file:  ' + meta_rev_file)

# Send summary email
end_time = time.strftime("%Y%m%d_%H%M%S",time.localtime())
subj = 'db_bunker success'
body = 'db_bunker Summary:\n'
body += 'Start_Time: ' + start_time + '\n'
body += 'End_Time:   ' + end_time + '\n'
body += 'Files_Proc: ' + str(cnt_files) + '\n'
body += 'Bytes_Proc (KB): ' + str(tot_bytes/1024) + '\n'
body += 'Meta_Rev:   ' + d_meta_rev + '\n\n'
body += 'Dropbox_Total (GB): ' + str(db_total_size/(1024*1024*1024)) + '\n'
body += 'Dropbox_Used (GB): ' + str(db_used_size/(1024*1024*1024)) + '\n'
body += 'Dropbox_Free (GB): ' + str((db_total_size - db_used_size)/(1024*1024*1024))
smtp_send(subj,body)
logger('End of Script')
exit(0)
