import time
from delay_queue.base import BaseTask


@BaseTask
def cal(x, y):
    return x * y


@BaseTask
def wait_cal(x, y):
    for i in range(30):
        print('running: ', i)
        time.sleep(1)

    return x * y

