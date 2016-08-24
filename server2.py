from ndex.networkn import NdexGraph
import networkx as nx
from datetime import datetime
from pytz import timezone
import pytz
import thread
import time
import ndex.client as nc
from requests_toolbelt import MultipartEncoder
import requests

def generate_graph(graph_size, prefix=''):
    prefix = str(prefix)
    G = NdexGraph(networkx_G=nx.complete_graph(graph_size))
    G.set_name(prefix + '-' + str(graph_size))
    return G
    # filename = prefix + '-' + str(graph_size) + '.cx'
    # G.write_to(filename)
    # return filename

def put_graph(sftp, filename):
    sftp.put(filename, '/home/ec2-user/' + filename)

def server2_upload(G):
    return G.upload_to('http://dev.ndexbio.org', 'scratch', 'scratch')

def server2_upload_from_file(cxfilename):
    cx_stream = open(cxfilename, 'rb')
    ndex = nc.Ndex('http://dev.ndexbio.org', 'scratch', 'scratch')
    ndex.save_cx_stream_as_new_network(cx_stream)


# def wrapper_putter(sftp, filename):
#     def wrapped():
#         return put_graph(sftp, filename)
#      return wrapped

def upload(thread_name, graph_size, times_to_upload=1):
    print 'start', thread_name
    sftp = connect('54.149.222.150', 22, 'ec2-user', 'aws_test_RH_7.pem')



    # graph_size = 3200
    graph_sizes = []
    times = []
    for i in range(times_to_upload):
        graph_sizes.append(graph_size)
        G = generate_graph(graph_size, prefix=thread_name + '-' + str(i+1))
        filename = G.get_name()

        # put_graph(sftp, filename)
        def wrapped():
            return server2_upload(G)

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
    thread.start_new_thread(upload, ('thread-1', 1600, 1) )
    thread.start_new_thread(upload, ('thread-2', 1600, 1) )
    thread.start_new_thread(upload, ('thread-3', 1600, 1))
    thread.start_new_thread(upload, ('thread-4', 1600, 1))
    thread.start_new_thread(upload, ('thread-5', 1600, 1))
    thread.start_new_thread(upload, ('thread-6', 1600, 1))
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