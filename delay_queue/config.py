
# 内部自定义参数
SELF_KWARGS = ['delay']

REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379

JOB_POOL_KEY = 'delay_queue:job_pool'
DELAY_POOL_KEY = 'delay_queue:delay_pool'
READY_POOL_KEY = 'delay_queue:ready_pool'

JOB_LOCK_KEY_PREFIX = 'delay_queue:job:lock:'
JOB_LOCK_EXP_TIME = 60 * 15  # 任务执行超时15分钟

WORKER_STOP_KEY = 'delay_queue:worker:stop'
TIMER_LOCK_KEY = 'delay_queue:timer:lock'
TIMER_LOCK_EXP_TIME = 10  # 秒

WORKER_NUM = 3  # 设置worker 线程数量,也就是并发获取任务的数量

# 所有异步执行的任务
IMPORT_TASKS = (
    'task_task.tasks',
)


