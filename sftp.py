import paramiko
from ndex.networkn import NdexGraph
import networkx as nx
from datetime import datetime
from pytz import timezone
import pytz
import thread
import time


# Open a transport
def connect(host, port, username, keyfile):
    transport = paramiko.Transport((host, port))
    pkey = paramiko.RSAKey.from_private_key_file(keyfile)
    transport.use_compression(True)
    transport.connect(username=username, pkey=pkey)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp

def generate_graph(graph_size, prefix=''):
    prefix = str(prefix)
    G = NdexGraph(networkx_G=nx.complete_graph(graph_size))
    filename = prefix + '-' + str(graph_size) + '.cx'
    G.write_to(filename)
    return filename

def put_graph(sftp, filename):
    sftp.put(filename, '/home/ec2-user/' + filename)

def get_graph(sftp, filename):
    sftp.get('/home/ec2-user/' + filename, filename)

# def wrapper_putter(sftp, filename):
#     def wrapped():
#         return put_graph(sftp, filename)
#      return wrapped

def upload(thread_name, graph_size, times_to_upload=1):
    print 'start', thread_name
    sftp = connect('54.244.205.16', 22, 'ec2-user', 'aws_test_RH_7.pem')

    # graph_size = 3200
    graph_sizes = []
    times = []
    for i in range(times_to_upload):
        graph_sizes.append(graph_size)
        filename = generate_graph(graph_size, prefix=thread_name + '-' + str(i+1))

        # put_graph(sftp, filename)
        def wrapped():
            return put_graph(sftp, filename)

        import timeit
        total_time = timeit.timeit(stmt=wrapped, number=1)
        date_format = '%H:%M:%S %Z'
        date = datetime.now()
        my_timezone = timezone('US/Pacific')
        date = my_timezone.localize(date)
        date = date.astimezone(my_timezone)
        print i, thread_name, filename
        print 'graph_size:', graph_size, '|| time_elapsed:', total_time, 'seconds || clock:', date.strftime(date_format)
        times.append(total_time)
        # graph_size = ((graph_size * 2) / 10) * 10

def download(thread_name, graph_size, times_to_upload=1, filename=None):
    print 'start download', thread_name
    sftp = connect('54.244.205.16', 22, 'ec2-user', 'aws_test_RH_7.pem')

    # graph_size = 3200
    graph_sizes = []
    times = []
    for i in range(times_to_upload):

        # put_graph(sftp, filename)
        def wrapped():
            return get_graph(sftp, filename)

        import timeit
        total_time = timeit.timeit(stmt=wrapped, number=1)
        date_format = '%H:%M:%S %Z'
        date = datetime.now()
        my_timezone = timezone('US/Pacific')
        date = my_timezone.localize(date)
        date = date.astimezone(my_timezone)
        print i, thread_name, filename
        print 'graph_size:', graph_size, '|| time_elapsed:', total_time, 'seconds || clock:', date.strftime(date_format)
        times.append(total_time)
        # graph_size = ((graph_size * 2) / 10) * 10


if __name__ == '__main__':
    thread.start_new_thread(download, ('thread-1', 1600, 1, 'thread-1-1-1600.cx') )
    thread.start_new_thread(download, ('thread-2', 1600, 1, 'thread-2-1-1600.cx') )
    thread.start_new_thread(download, ('thread-3', 1600, 1, 'thread-3-1-1600.cx'))
    thread.start_new_thread(download, ('thread-4', 1600, 1, 'thread-4-1-1600.cx'))
    thread.start_new_thread(download, ('thread-5', 1600, 1, 'thread-5-1-1600.cx'))
    thread.start_new_thread(download, ('thread-6', 1600, 1, 'thread-6-1-1600.cx'))
    # thread.start_new_thread(upload, ('thread-7', 3200, 1))
    # thread.start_new_thread(upload, ('thread-8', 3200, 1))
    # upload('main-thread', 3200, 1)
    print 'Done launching threads.'

    while 1:
        time.sleep(1000)
        pass
    # graph_size = ((graph_size * 2) / 10) * 10

    # sftp = connect('54.187.83.22', 22, 'ec2-user', 'aws_test_RH_7.pem')

    # import pandas as pd
    # df = pd.DataFrame (data=[graph_sizes,times])
    # print df
    # for file in sftp.listdir():
    #     print file
