from ndex.networkn import NdexGraph
import networkx as nx
from datetime import datetime
from pytz import timezone

import thread
import time
import ndex.client as nc
import os


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

def server2_download(network_id):
    ndex = nc.Ndex('http://dev.ndexbio.org', 'scratch', 'scratch')
    result = ndex.get_network_as_cx_stream(network_id)
    return result.json()


# def wrapper_putter(sftp, filename):
#     def wrapped():
#         return put_graph(sftp, filename)
#      return wrapped

def upload(thread_name, graph_size, times_to_upload=1):
    print 'start upload', thread_name

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

def download(thread_name, graph_size, times_to_upload=1, network_id=None):
    print 'start download', thread_name

    graph_sizes = []
    times = []
    for i in range(times_to_upload):
        graph_sizes.append(graph_size)


        # put_graph(sftp, filename)
        def wrapped():
            return server2_download(network_id)

        import timeit
        total_time = timeit.timeit(stmt=wrapped, number=1)
        date_format = '%H:%M:%S %Z'
        date = datetime.now()
        my_timezone = timezone('US/Pacific')
        date = my_timezone.localize(date)
        date = date.astimezone(my_timezone)
        print i, thread_name, network_id
        print 'graph_size:', graph_size, '|| time_elapsed:', total_time, 'seconds || clock:', date.strftime(date_format)
        times.append(total_time)
        # graph_size = ((graph_size * 2) / 10) * 10

def upload_cx_dir(cx_dir_name='cx'):
    cx_files = [f for f in os.listdir(cx_dir_name) if f.endswith('.cx')]
    count = 1
    for cx_file in cx_files:
        G = NdexGraph(filename=cx_file)
        uuid = G.upload_to('http://dev.ndexbio.org', 'scratch', 'scratch')
        print 'Uploaded', count, 'cx file'
        count += 1
        del G

def timed_upload_cx_dir(thread_name, cx_dir_name='cx'):
    print 'start dir upload', thread_name

    def wrapped():
        return upload_cx_dir(cx_dir_name)

    import timeit
    total_time = timeit.timeit(stmt=wrapped, number=1)
    date_format = '%H:%M:%S %Z'
    date = datetime.now()
    my_timezone = timezone('US/Pacific')
    date = my_timezone.localize(date)
    date = date.astimezone(my_timezone)
    print thread_name
    print 'time_elapsed:', total_time, 'seconds || clock:', date.strftime(date_format)

def concurrent_timed_upload_cx_dir():
    thread.start_new_thread(timed_upload_cx_dir, ('thread-1', 'cx1'))
    thread.start_new_thread(timed_upload_cx_dir, ('thread-2', 'cx2'))
    thread.start_new_thread(timed_upload_cx_dir, ('thread-3', 'cx3'))
    thread.start_new_thread(timed_upload_cx_dir, ('thread-4', 'cx4'))
    thread.start_new_thread(timed_upload_cx_dir, ('thread-5', 'cx5'))
    thread.start_new_thread(timed_upload_cx_dir, ('thread-6', 'cx6'))

def concurrent_download():
    thread.start_new_thread(download, ('thread-1', 1600, 1, '207d5967-6a5c-11e6-b0fb-06832d634f41'))
    thread.start_new_thread(download, ('thread-2', 1600, 1, '20ba8978-6a5c-11e6-b0fb-06832d634f41'))
    thread.start_new_thread(download, ('thread-3', 1600, 1, '21f74f39-6a5c-11e6-b0fb-06832d634f41'))
    thread.start_new_thread(download, ('thread-4', 1600, 1, '23271caa-6a5c-11e6-b0fb-06832d634f41'))
    thread.start_new_thread(download, ('thread-5', 1600, 1, '2403c10b-6a5c-11e6-b0fb-06832d634f41'))
    thread.start_new_thread(download, ('thread-6', 1600, 1, '242838fc-6a5c-11e6-b0fb-06832d634f41'))

    print 'Done launching threads.'

    while 1:
        time.sleep(1000)
        pass



if __name__ == '__main__':
    # concurrent_download()
    concurrent_timed_upload_cx_dir()


