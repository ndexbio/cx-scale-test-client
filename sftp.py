import paramiko
from ndex.networkn import NdexGraph
import networkx as nx
from datetime import datetime
from pytz import timezone
import pytz
import threading
import time
from time import ctime
import argparse
import os


# Open a transport
def connect(host, port, username, keyfile):
    transport = paramiko.Transport((host, port))
    pkey = paramiko.RSAKey.from_private_key_file(keyfile)
    transport.use_compression(True)
    transport.connect(username=username, pkey=pkey)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp

def put_graph(sftp, filename):
    sftp.put(filename, '/home/ec2-user/' + filename)

def get_graph(sftp, filename):
    sftp.get('/home/ec2-user/' + filename, filename)

def download_files(thread_name, network_files_filename, outdir, sftp_url, sftp_port, sftp_username, sftp_pem):
    print 'Launching thread', thread_name
    try:
        os.makedirs(outdir)
    except OSError:
        if not os.path.isdir(outdir):
            raise
    sftp = connect(sftp_url, sftp_port, sftp_username, sftp_pem)
    network_files_file = open(network_files_filename, 'r')
    downloadsTxt = open(outdir + '/' + thread_name + '-downloads.txt', 'w')
    downloadsTxt.write('network id\tsize (bytes)\ttime (seconds)\tthread' + os.linesep)
    for network_file in network_files_file:
        network_file = network_file.strip()
        start = time.time()
        get_graph(sftp, network_file)
        end = time.time()
        total_time = (end - start)
        filesize = os.path.getsize(network_file)
        downloadsTxt.write(network_file + '\t' +
                           str(filesize) + '\t' +
                           str(total_time) + '\t' +
                           thread_name + os.linesep)
    sftp.close()
    downloadsTxt.close()
    print 'Done with thread', thread_name

def upload_cx_dir(thread_name, cx_dir_name, outdir, sftp_url, sftp_port, sftp_username, sftp_pem):
    print 'Launching thread', thread_name
    try:
        os.makedirs(outdir)
    except OSError:
        if not os.path.isdir(outdir):
            raise
    sftp = connect(sftp_url, sftp_port, sftp_username, sftp_pem)
    cx_files = [f for f in os.listdir(cx_dir_name) if f.endswith('.cx')]
    filenamesTxt = open(outdir + '/' + thread_name + '-filenams.txt', 'w')
    uploadsTxt = open(outdir + '/' + thread_name + '-uploads.txt', 'w')
    uploadsTxt.write('filename\tsize (bytes)\ttime (seconds)\tthread' + os.linesep)
    for cx_filename in cx_files:
        full_cx_filename = cx_dir_name + '/' + cx_filename
        start = time.time()
        put_graph(sftp, full_cx_filename)
        end = time.time()
        total_time = (end - start)
        filenamesTxt.write(cx_filename + os.linesep)
        filesize = os.path.getsize(full_cx_filename)
        uploadsTxt.write(cx_filename + '\t' +
                         str(filesize) + '\t' +
                         str(total_time) + '\t' +
                         thread_name + os.linesep)
    sftp.close()
    filenamesTxt.close()
    uploadsTxt.close()
    print 'Done with thread', thread_name

def concurrent_upload(num_threads, cx_dir_name, outdir, sftp_url, sftp_port):
    start = time.time()
    threads = []

    for i in range(num_threads):
        t = threading.Thread(target=upload_cx_dir, args=(str(i+1), cx_dir_name, outdir, sftp_url, sftp_port))
        threads.append(t)

    for i in range(num_threads):
        threads[i].start()

    print 'Done launching upload threads.'

    for i in range(num_threads):
        threads[i].join()

    print 'Finished all uploads.'
    end = time.time()
    total_time = end - start
    print 'time_elapsed:', total_time, 'seconds || clock:', ctime()

def concurrent_download(num_threads, network_id_filename, outdir, sftp_url, sftp_port, sftp_username, sftp_pem):
    start = time.time()
    threads = []

    for i in range(num_threads):
        t = threading.Thread(target=download_files, args=(str(i + 1), network_id_filename, outdir, sftp_url, sftp_port, sftp_username, sftp_pem))
        threads.append(t)

    for i in range(num_threads):
        threads[i].start()

    print 'Done launching download threads.'

    for i in range(num_threads):
        threads[i].join()

    print 'Finished all downloads.'
    end = time.time()
    total_time = end - start
    print 'time_elapsed:', total_time, 'seconds || clock:', ctime()


if __name__ == '__main__':
    VERSION = '1.0'
    parser = argparse.ArgumentParser(description='Run tests on NDEx Server.')
    parser.add_argument('--version', action='version', version=VERSION)
    parser.add_argument('type', choices=['upload','download'], help='type of test')
    parser.add_argument('num_threads', type=int, help='number of threads to run')
    parser.add_argument('sftp_url', help='the url of the sftp server, e.g. 54.244.205.16 ')
    parser.add_argument('-d','--dir', default='cx', help='the directory of cx files to act on')
    parser.add_argument('-f','--file', default='1-uuid.txt', help='the file that contains network ids to be operated on')
    parser.add_argument('-o','--outdir', default='.', help='the directory to save output files')
    parser.add_argument('-p', '--port', type=int, default=22, help='the port of the sftp server')
    parser.add_argument('-u', '--username', default='ec2-user', help='the username of the sftp server')
    parser.add_argument('-c', '--credentials', default='aws_test_RH_7.pem', help='the name of the .pem file that stores the credentials')

    args = parser.parse_args()

    if args.type == 'upload':
        concurrent_upload(args.num_threads, args.dir, args.outdir, args.sftp_url, args.port, args.username, args.credentials)

    elif args.type == 'download':
        concurrent_download(args.num_threads, args.file, args.outdir, args.sftp_url, args.port, args.username, args.credentials)
