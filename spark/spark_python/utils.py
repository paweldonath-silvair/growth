import socket
from pyspark import TaskContext


def task_info(*_):
    ctx = TaskContext()
    return ["Stage: {0}, Partition: {1}, Host: {2}".format(
        ctx.stageId(), ctx.partitionId(), socket.gethostname())]
