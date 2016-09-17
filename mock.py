import threading
from requests_toolbelt import MultipartEncoder
import requests
import os
import argparse
import time
from time import ctime

def post_multipart(url, fields):

    multipart_data = MultipartEncoder(fields=fields)

    headers = {'Content-Type': multipart_data.content_type,
               'Accept': 'application/json',
               'Cache-Control': 'no-cache',
               }
    s = requests.session()
    response = s.post(url, data=multipart_data, headers=headers)
    response.raise_for_status()
    if response.status_code == 204:
        return ""
    try:
        result = response.json()
    except ValueError:
        result = response.text
    return result


def save_cx_stream_as_new_network(mock_url, cx_stream):
    fields = {
        'CXNetworkStream': ('filename', cx_stream, 'application/octet-stream')
    }
    url = 'http://'+mock_url+'/mock-cx-server/rest/network/'

    return post_multipart(url, fields)


def get_stream(mock_url, network_id):
    url = 'http://'+mock_url+'/mock-cx-server/rest/network/' + network_id
    s = requests.session()
    response = s.get(url, params=None, stream=True)
    response.raise_for_status()
    if response.status_code == 204:
        return ""
    return response


def get_network_as_cx_stream(mock_url, network_id):
    return get_stream(mock_url, network_id)

def download_mock_graph(network_id):
    response = get_network_as_cx_stream(network_id)
    return response


def mock_upload_from_file(mockurl, cxfilename):
    cx_stream = open(cxfilename, 'rb')
    response = save_cx_stream_as_new_network(mockurl, cx_stream)
    cx_stream.close()
    return response

def download_network_ids(thread_name, network_id_filename, outdir, mock_url):
    print 'Launching thread', thread_name
    try:
        os.makedirs(outdir)
    except OSError:
        if not os.path.isdir(outdir):
            raise
    network_id_file = open(network_id_filename, 'r')
    downloadsTxt = open(outdir + '/' + thread_name + '-downloads.txt', 'w')
    downloadsTxt.write('network id\tsize (bytes)\ttime (seconds)\tthread' + os.linesep)
    for network_id in network_id_file:
        network_id = network_id.strip()
        start = time.time()
        response = get_network_as_cx_stream(mock_url, network_id)
        end = time.time()
        total_time = (end - start)
        filesize = len(response.content)
        downloadsTxt.write(network_id + '\t' +
                           str(filesize) + '\t' +
                           str(total_time) + '\t' +
                           thread_name + os.linesep)
    downloadsTxt.close()
    print 'Done with thread', thread_name

def upload_cx_dir(thread_name, cx_dir_name, outdir, mock_url):
    print 'Launching thread', thread_name
    try:
        os.makedirs(outdir)
    except OSError:
        if not os.path.isdir(outdir):
            raise
    cx_files = [f for f in os.listdir(cx_dir_name) if f.endswith('.cx')]
    uuidTxt = open(outdir + '/' + thread_name + '-uuid.txt', 'w')
    uploadsTxt = open(outdir + '/' + thread_name + '-uploads.txt', 'w')
    uploadsTxt.write('filename\tsize (bytes)\ttime (seconds)\tthread\tuuid' + os.linesep)
    for cx_filename in cx_files:
        full_cx_filename = cx_dir_name + '/' + cx_filename
        start = time.time()
        uuid = mock_upload_from_file(mock_url, full_cx_filename)
        end = time.time()
        total_time = (end - start)
        uuidTxt.write(uuid + os.linesep)
        filesize = os.path.getsize(full_cx_filename)
        uploadsTxt.write(cx_filename + '\t' +
                         str(filesize) + '\t' +
                         str(total_time) + '\t' +
                         thread_name + '\t' +
                         uuid + os.linesep)
    uuidTxt.close()
    uploadsTxt.close()
    print 'Done with thread', thread_name


def concurrent_upload(num_threads, cx_dir_name, outdir, mock_url):
    start = time.time()
    threads = []

    for i in range(num_threads):
        t = threading.Thread(target=upload_cx_dir, args=(str(i+1), cx_dir_name, outdir, mock_url))
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

def concurrent_download(num_threads, network_id_filename, outdir):
    start = time.time()
    threads = []

    for i in range(num_threads):
        t = threading.Thread(target=download_network_ids, args=(str(i + 1), network_id_filename, outdir))
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
    parser.add_argument('type', choices=['upload', 'download'], help='type of test')
    parser.add_argument('num_threads', type=int, help='number of threads to run')
    parser.add_argument('mock_url', help='the url of the mock server, e.g. 54.244.205.25:8080 ')
    parser.add_argument('-d', '--dir', default='cx', help='the directory of cx files to act on')
    parser.add_argument('-f', '--file', default='1-uuid.txt',
                        help='the file that contains network ids to be operated on')
    parser.add_argument('-o', '--outdir', default='.', help='the directory to save output files')
    args = parser.parse_args()

    if args.type == 'upload':
        concurrent_upload(args.num_threads, args.dir, args.outdir, args.mock_url)

    elif args.type == 'download':
        concurrent_download(args.num_threads, args.file, args.outdir)