from mpi4py import MPI
from time import sleep


"""

    @author: Huy Nguyen
    Source: https://www.coursera.org/learn/cloud-computing/lecture/0vA4p/2-3-implementing-multicast-ordering-2

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

P = [0]*len(group)  # Sequence numbers.. 
buffer = dict()  # Storing messages on wait.


def causality_check(M, P, j):
    for k in range(len(M)):
        if k != j and M[k] > P[k]:
            return False
    return True


def causal_multicast_send(group=None, delay=0.1, msg=None):  # Similar to broadcast when using size.
    assert group is not None
    P[rank] += 1  # Set P_j[j] = P_j[j]+1
    reqs = []
    if msg is None:
        for i in group:
            if i != rank:
                data = {SEQ_NUM: P, MSG: P[rank]}  # Generating dummy data., just for testing purpose.
                req = COMM_WORLD.isend(data, dest=i, tag=TAG)  # Non-blocking op.
                reqs.append(reqs)
                sleep(delay)
    else:
        for i in group:
            if i != rank:
                data = {SEQ_NUM: P, MSG: msg}
                req = COMM_WORLD.isend(data, dest=i, tag=TAG)  # Non-blocking op.
                reqs.append(reqs)
                sleep(delay)
    return reqs


def causal_recv():  # Receive + Buffer msg if Sequence number is not valid.
    stt = MPI.Status()
    data = COMM_WORLD.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=stt)  # Blocking op.
    j = stt.Get_source()
    M = data[SEQ_NUM]

    flag1 = M[j] == P[j]+1
    flag2 = causality_check(M,P,j)

    if flag1 and flag2:  # Valid seq num.
        tmp_M = M.copy()
        if buffer.get(j) is None:
            buffer[j] = list()
        buffer[j].append((tmp_M, data[MSG]))
        discard_list = list() # For removing used items in buffer.
        for (tmp_M, msg) in buffer[j]:
            flag1 = tmp_M[j] == P[j]+1 
            flag2 = causality_check(tmp_M, P, j)

            if flag1 and flag2:
                P[j] = tmp_M[j]
                discard_list.append((tmp_M, msg))

        # Discard used items.
        for discard_item in discard_list:
            buffer[j].remove(discard_item)
    else:  # Buffer msg.
        if buffer.get(j) is None:
            buffer[j] = list()
        buffer[j].append((M, data[MSG]))


def example():
    if size != len(group):  # Fixed size of comm world.
        if rank == ROOT:
            print('This application is designed to run with %s MPI Processes.' % len(group))
        return

    if rank == 0:
        causal_multicast_send(group)
        causal_recv()
        causal_recv()
    elif rank == 1:
        causal_recv()
        causal_multicast_send(group)
        causal_recv()
    elif rank == 2:
        causal_recv()
        causal_recv()
        causal_recv()
    elif rank == 3:
        causal_recv()
        causal_multicast_send(group)
        causal_recv()

    print('[Process %d] %s' % (rank, P))

if __name__ == "__main__":
    # Running example.
    example()
