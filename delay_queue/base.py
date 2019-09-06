import uuid
import time
import json
from delay_queue.config import JOB_POOL_KEY, DELAY_POOL_KEY, READY_POOL_KEY, \
    JOB_LOCK_EXP_TIME, JOB_LOCK_KEY_PREFIX
from delay_queue.utils import redis_client, release_redis_lock


class BaseTask(object):

    def __init__(self, func):
        self.func = func

    def delay(self, *args, **kwargs):
        # 获取函数所在文件的绝对路径
        func_abs_path = self.func.__globals__['__file__']
        task_dict = {
            'task': {'abs_path': func_abs_path, 'name': self.func.__name__},
            'id': str(uuid.uuid4()),
            'delay': kwargs.get('delay'),
            'body': {'args': args, 'kwargs': kwargs},
            'post_time': int(time.time() * 1000),
            'start_time': None,
            'delete_time': None,
            'update_time': None,
            'finish_time': None,
            'status': 'delay' if kwargs.get('delay') else 'ready',
            'result': None,
            'error': None
        }
        redis_client.hset(JOB_POOL_KEY, task_dict['id'], json.dumps(task_dict))
        # 是否延迟
        if task_dict['status'] == 'delay':
            redis_client.zadd(DELAY_POOL_KEY, {task_dict['id']: task_dict['delay']})
        else:
            redis_client.lpush(READY_POOL_KEY, task_dict['id'])
        return task_dict

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


class TaskManager(object):
    def __init__(self, job_pool_key, delay_pool_key, ready_pool_key):
        self.job_pool_key = job_pool_key
        self.delay_pool_key = delay_pool_key
        self.ready_pool_key = ready_pool_key

    def list_task(self, task_id=None):
        if task_id:
            task_dict_raw = redis_client.hget(self.job_pool_key, task_id)
            data = json.loads(task_dict_raw.decode('utf-8'))
        else:
            task_dict_raw = redis_client.hgetall(self.job_pool_key)
            if task_dict_raw:
                data = [json.loads(i.decode('utf-8')) for i in task_dict_raw.values()]
            else:
                data = []
        return data

    def edit_task(self, task_id, **kwargs):
        task_dict_raw = redis_client.hget(self.job_pool_key, task_id)
        task_dict = json.loads(task_dict_raw.decode('utf-8'))
        # 设置锁，防止多重修改
        job_lock_key = JOB_LOCK_KEY_PREFIX + task_dict['id']
        job_lock_value = uuid.uuid4().hex[:20]
        if redis_client.set(job_lock_key, job_lock_value, ex=JOB_LOCK_EXP_TIME, nx=True):
            for k, v in kwargs.items():
                task_dict[k] = v
            task_dict['update_time'] = int(time.time() * 1000)
            redis_client.hset(self.job_pool_key, task_id, json.dumps(task_dict))
            # 解锁
            if not release_redis_lock(job_lock_key, job_lock_value):
                # alert
                print('release job lock fail')

    def delete_task(self, task_id):
        task_dict_raw = redis_client.hget(self.job_pool_key, task_id)
        task_dict = json.loads(task_dict_raw.decode('utf-8'))
        # 设置锁，防止多重修改
        job_lock_key = JOB_LOCK_KEY_PREFIX + task_dict['id']
        job_lock_value = uuid.uuid4().hex[:20]
        if redis_client.set(job_lock_key, job_lock_value, ex=JOB_LOCK_EXP_TIME, nx=True):
            task_dict['status'] = 'delete'
            task_dict['delete_time'] = int(time.time() * 1000)
            redis_client.hset(self.job_pool_key, task_id, json.dumps(task_dict))
            # 解锁
            if not release_redis_lock(job_lock_key, job_lock_value):
                # alert
                print('release job lock fail')


task_manager = TaskManager(JOB_POOL_KEY, DELAY_POOL_KEY, READY_POOL_KEY)
