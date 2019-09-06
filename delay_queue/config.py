import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379

JOB_POOL_KEY = 'delay_queue:job_pool'
DELAY_POOL_KEY = 'delay_queue:delay_pool'
READY_POOL_KEY = 'delay_queue:ready_pool'

WORKER_STOP_KEY = 'delay_queue:worker:stop'
TIMER_LOCK_KEY = 'delay_queue:timer:lock'
TIMER_LOCK_EXP_TIME = 20  # 秒

WORKER_NUM = 3  # 设置worker 线程数量,也就是并发获取任务的数量

# 所有异步执行的任务
IMPORT_TASKS = (
    'test.tasks',
)


