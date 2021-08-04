from mpi4py import MPI
import numpy as np
from time import sleep


"""

    @author: Huy Nguyen
    Source: https://www.coursera.org/learn/cloud-computing/lecture/XmDtl/2-2-implementing-multicast-ordering-1

"""


# Communication params.
TAG = 0  # Tagging for point to point communications.
SEQ_NUM = 's'  # Sequence number key.
MSG = 'm'  # Message key.
ROOT = 0

# MPI Communications.
COMM_WORLD = MPI.COMM_WORLD
rank = COMM_WORLD.Get_rank()
size = COMM_WORLD.Get_size()
group = [0,1,2,3]  # Multicasting group

P = np.zeros(4, dtype=np.int16)  # Sequence numbers.. 
buffer = dict()  # Storing messages on wait.


def FIFO_multicast_send(group=None, msg=None, delay=0.1):  # Similar to broadcast when using size.
    assert group is not None
    P[rank] += 1  # Set P_j[j] = P_j[j]+1
    reqs = []
    if msg is None:
        for i in group:
            if i != rank:
                msg = {SEQ_NUM: P[rank], MSG: P[rank]}  # Sending data (dummy message).
                req = COMM_WORLD.isend(msg, dest=i, tag=TAG)  # Non-blocking op.
                reqs.append(reqs)
                sleep(delay)
    else:
        for i in group:
            if i != rank:
                req = COMM_WORLD.isend(msg, dest=i, tag=TAG)  # Non-blocking op.
                reqs.append(reqs)
                sleep(delay)
    return reqs


def FIFO_recv():  # Receive + Buffer msg if Sequence number is not valid.
    stt = MPI.Status()
    data = COMM_WORLD.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=stt)  # Blocking op.
    #print('[%d] %s from [%d]' % (rank, data, stt.Get_source()))

    if data[SEQ_NUM] == P[stt.Get_source()]+1:  # Valid seq num.
        tmp_seq_num = data[SEQ_NUM]
        buffer[tmp_seq_num] = data[MSG]
        while tmp_seq_num in buffer:
            #print('[Process %d] doing task %d %s.' % (rank, buffer[tmp_seq_num], P))
            tmp_seq_num += 1
        P[stt.Get_source()] = data[SEQ_NUM]
    else:  # Buffer msg.
        buffer[data[SEQ_NUM]] = data[MSG]


def example():
    if size != len(group):  # Fixed size of comm world.
        if rank == ROOT:
            print('This application is designed to run with %s MPI Processes.' % len(group))
        return

    if rank == 0:
        FIFO_multicast_send(group, delay=0.5)
        FIFO_multicast_send(group, delay=0.03)
        FIFO_recv()
    elif rank == 1:
        FIFO_recv()
        FIFO_recv()
        FIFO_recv()
    elif rank == 2:
        FIFO_recv()
        FIFO_recv()
        FIFO_multicast_send(group)
    elif rank == 3:
        FIFO_recv()
        FIFO_recv()
        FIFO_recv()

    print('[Process %d] %s' % (rank, P))


if __name__ == "__main__":
    # Running example.
    example()
