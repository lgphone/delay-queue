import time
import json
import uuid
from delay_queue.utils import redis_client, release_redis_lock
from delay_queue.config import JOB_POOL_KEY, DELAY_POOL_KEY, \
    READY_POOL_KEY, TIMER_LOCK_KEY, TIMER_LOCK_EXP_TIME, JOB_LOCK_KEY_PREFIX, JOB_LOCK_EXP_TIME


def timer_func():
    while True:
        # 锁，防止并发获取delay task
        lock_value = uuid.uuid4().hex[:20]
        if redis_client.set(TIMER_LOCK_KEY, lock_value, ex=TIMER_LOCK_EXP_TIME, nx=True):
            # 当前时间戳
            st = int(time.time() * 1000)
            # 获取小于当前时间戳的所有task，加入ready 队列
            task_id_list = redis_client.zrangebyscore(DELAY_POOL_KEY, '-inf', st)
            task_id_list = [i.decode('utf-8') for i in task_id_list]
            for task_id in task_id_list:
                print(f'delay task: {task_id}')
                task_dict_raw = redis_client.hget(JOB_POOL_KEY, task_id)
                if task_dict_raw:
                    task_dict = json.loads(task_dict_raw.decode('utf-8'))
                    if task_dict['status'] == 'delay':
                        # 设置锁，任务执行期间无法修改
                        job_lock_key = JOB_LOCK_KEY_PREFIX + task_dict['id']
                        job_lock_value = uuid.uuid4().hex[:20]
                        if redis_client.set(job_lock_key, job_lock_value, ex=JOB_LOCK_EXP_TIME, nx=True):
                            redis_client.zrem(DELAY_POOL_KEY, task_dict['id'])
                            redis_client.lpush(READY_POOL_KEY, task_dict['id'])
                            # 设置为ready状态
                            task_dict['status'] = 'ready'
                            redis_client.hset(JOB_POOL_KEY, task_dict['id'], json.dumps(task_dict))
                            # 解锁
                            if not release_redis_lock(job_lock_key, job_lock_value):
                                # alert
                                print('release job lock fail')
            if not release_redis_lock(TIMER_LOCK_KEY, lock_value):
                # alert
                print('release lock fail')
        # sleep
        time.sleep(0.1)


if __name__ == "__main__":
    # TODO 可设置多个线程扫描多个delay pool， 类似于celery 的 queue router
    print('start timer')
    try:
        timer_func()
    except KeyboardInterrupt:
        print('exit timer')
        exit(0)
