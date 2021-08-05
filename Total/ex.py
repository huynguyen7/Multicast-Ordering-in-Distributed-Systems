from mpi4py import MPI
import numpy as np
from time import sleep


"""

    @author: Huy Nguyen
    Source:
    +https://www.coursera.org/learn/cloud-computing/lecture/XmDtl/2-2-implementing-multicast-ordering-1
    +https://cse.buffalo.edu/~stevko/courses/cse486/spring13/lectures/12-multicast2.pdf

"""


# Communication params.
ROOT = 0  # Root's rank.
SEQUENCER = 0  # Sequencer's rank.
TAG = 0  # Tagging for point to point communications.
SEQ_NUM = 's'  # Sequence number key.
MSG = 'm'  # Message key.

# MPI Communications.
COMM_WORLD = MPI.COMM_WORLD
rank = COMM_WORLD.Get_rank()
size = COMM_WORLD.Get_size()
group = [1,2,3,4]  # Multicasting group

S = 0  # Sequence number.
buffer = dict()  # Storing messages on wait.


def total_multicast_send(group=None, msg=None, delay=0.1):  # Similar to broadcast when using size.
    assert group is not None
    global S
    if msg is None:
        for i in group:
            if i != rank:
                data = {SEQ_NUM: S, MSG: S}  # Generating dummy data, for testing purpose.
                COMM_WORLD.isend(data, dest=i, tag=TAG)  # Non-blocking op.
                sleep(delay)  # Delay simulation.
    else:
        for i in group:
            if i != rank:
                data = {SEQ_NUM: S, MSG: msg}
                COMM_WORLD.isend(data, dest=i, tag=TAG)  # Non-blocking op.
                sleep(delay)  # Delay simulation

    # Sending message to sequencer
    if rank != SEQUENCER:
        data = {SEQ_NUM: S, MSG: None}
        COMM_WORLD.isend(data, dest=SEQUENCER, tag=TAG)  # Non-blocking op.


def total_recv():  #TODO
    stt1 = MPI.Status()
    stt2 = MPI.Status()
    # Receive msg.
    data1 = COMM_WORLD.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=stt1)  # Blocking op.
    data2 = COMM_WORLD.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=stt2)  # Blocking op.

    # Filter which msg from source P_j or from sequencer.
    seq_msg = data1 if stt1.Get_source() == SEQUENCER else data2
    multicast_msg = data1 if stt1.Get_source() != SEQUENCER else data2
    S_M = seq_msg[SEQ_NUM]  # Sequencer's S.
    #print('[%d] %s from [%d]' % (rank, data, stt.Get_source()))
    #print('[%d] %s %s' % (rank, seq_msg, multicast_msg))

    global S
    #print('[%d] %d %d' % (rank, S, S_M))
    if S+1 == S_M:  # Valid seq num.
        tmp_seq_num = S_M
        buffer[tmp_seq_num] = multicast_msg[MSG]
        while tmp_seq_num in buffer:
            print('[Process %d] doing task %d.' % (rank, buffer[tmp_seq_num]))
            #buffer.pop(tmp_seq_num)
            tmp_seq_num += 1
        S = tmp_seq_num if tmp_seq_num in buffer else tmp_seq_num-1
        #S = tmp_seq_num
        #print('[%d] %d %s' % (rank, tmp_seq_num, buffer))
    else:  # Buffer msg.
        buffer[S_M] = multicast_msg[MSG]


def total_seq_send_recv(delay=0.1):  # Sequencer receive and send msg.
    stt = MPI.Status()
    data = COMM_WORLD.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=stt)  # Blocking op.
    global S
    S += 1
    msg = S
    total_multicast_send(group, msg)


def example():
    if size != 5:  # Fixed size of comm world.
        if rank == ROOT:
            print('This application is designed to run with 5 MPI Processes.')
        return

    global S
    if rank == 1:
        total_multicast_send(group,delay=0.3)
        total_recv()
        total_recv()
        total_recv()
    elif rank == 2:
        total_recv()
        total_multicast_send(group,delay=0.1)
        total_recv()
        total_recv()
    elif rank == 3:
        total_recv()
        total_recv()
        total_multicast_send(group,delay=0.1)  # Switch this up to see effect.
        total_multicast_send(group,delay=0.1)
    elif rank == 4:
        total_recv()
        total_recv()
        total_recv()
        total_recv()
    elif rank == SEQUENCER:  # Sequencer
        total_seq_send_recv()
        total_seq_send_recv()
        total_seq_send_recv()
        total_seq_send_recv()

    #print('[Process %d] %s' % (rank, S))
    print('[Process %d] %s' % (rank, buffer))


if __name__ == "__main__":
    # Running example.
    example()
