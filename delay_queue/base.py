import uuid
import json
import os
from config import JOB_POOL_KEY, DELAY_POOL_KEY, READY_POOL_KEY, BASE_DIR
from utils import redis_client


class BaseTask(object):

    def __init__(self, func):
        self.func = func

    def delay(self, *args, **kwargs):
        # 获取函数所在文件的绝对路径，并修改为相对路径
        func_abs_path = self.func.__globals__['__file__']
        func_import_path = func_abs_path.replace(BASE_DIR + os.sep, '')
        func_import_path = os.path.splitext(func_import_path)[0].replace(os.sep, '.')

        job_info = {
            'task': {'import_path': func_import_path, 'name': self.func.__name__},
            'id': str(uuid.uuid4()),
            'delay': kwargs.get('delay'),
            'body': {'args': args, 'kwargs': kwargs},
            'status': 'delay'
        }
        redis_client.hset(JOB_POOL_KEY, job_info['id'], json.dumps(job_info))
        # 是否延迟，不延迟直接发送到队列中
        if job_info['delay']:
            redis_client.zadd(DELAY_POOL_KEY, {job_info['id']: job_info['delay']})
        else:
            redis_client.lpush(READY_POOL_KEY, job_info['id'])
        return job_info

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)
