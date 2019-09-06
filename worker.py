import importlib
import json
import uuid
import time
import os
from threading import Thread
from delay_queue.utils import redis_client, release_redis_lock
from delay_queue.config import READY_POOL_KEY, JOB_POOL_KEY, WORKER_NUM, \
    WORKER_STOP_KEY, IMPORT_TASKS, SELF_KWARGS, JOB_LOCK_KEY_PREFIX, JOB_LOCK_EXP_TIME


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

task_import_dict = {
    import_task: importlib.import_module(import_task)
    for import_task in IMPORT_TASKS
}


def run_worker_func(worker_id):
    print(f'start worker-{worker_id}')
    while True:
        stop_flag = redis_client.get(WORKER_STOP_KEY)
        if stop_flag:
            stop_flag = stop_flag.decode('utf-8')
            if stop_flag == '1':
                print(f'exit worker-{worker_id}')
                break
        task_tuple = redis_client.brpop(READY_POOL_KEY, timeout=3)
        if task_tuple:
            task_id = task_tuple[1].decode('utf-8')
            print(f'worker-{worker_id}: get task: {task_id}')
            task_dict = redis_client.hget(JOB_POOL_KEY, task_id)
            if task_dict:
                task_dict = json.loads(task_dict.decode('utf-8'))
                if task_dict['status'] == 'ready':
                    # 设置锁，任务执行期间无法修改
                    job_lock_key = JOB_LOCK_KEY_PREFIX + task_dict['id']
                    job_lock_value = uuid.uuid4().hex[:20]
                    if redis_client.set(job_lock_key, job_lock_value, ex=JOB_LOCK_EXP_TIME, nx=True):
                        task_dict['status'] = 'reserved'
                        task_dict['start_time'] = int(time.time() * 1000)
                        # 更新任务状态
                        redis_client.hset(JOB_POOL_KEY, task_id, json.dumps(task_dict))
                        # 删除task定义的参数， 目前只有一个delay参数 用于确定任务运行时间
                        for kwarg in SELF_KWARGS:
                            if task_dict['body']['kwargs'].get(kwarg):
                                task_dict['body']['kwargs'].pop(kwarg)
                        # 获取task 函数导入路径及名称
                        task_fun_name = task_dict['task']['name']
                        task_func_bas_path = task_dict['task']['abs_path']
                        _func_import_path = task_func_bas_path.replace(BASE_DIR + os.sep, '')
                        func_import_path = os.path.splitext(_func_import_path)[0].replace(os.sep, '.')
                        # 执行函数
                        if not task_import_dict.get(func_import_path):
                            print('cant run not import task')
                            task_dict['status'] = 'error'
                        else:
                            try:
                                result = getattr(
                                    task_import_dict[func_import_path],
                                    task_fun_name
                                )(*task_dict['body']['args'], **task_dict['body']['kwargs'])
                                task_dict['status'] = 'finish'
                                task_dict['finish_time'] = int(time.time() * 1000)
                                task_dict['result'] = result
                            except Exception as e:
                                task_dict['status'] = 'error'
                                task_dict['error'] = str(e)
                            # 更新任务结果
                            redis_client.hset(JOB_POOL_KEY, task_id, json.dumps(task_dict))
                        # 解锁
                        if not release_redis_lock(job_lock_key, job_lock_value):
                            # alert
                            print('release job lock fail')
                        print(f'worker-{worker_id}: task {task_id} done!')
                    else:
                        # 没有设置成功，重新加入队列
                        redis_client.rpush(READY_POOL_KEY, task_id)


if __name__ == "__main__":
    print('start main worker..')
    # 设置停止flag
    redis_client.set(WORKER_STOP_KEY, 0)

    worker_thread_list = []
    for num in range(0, WORKER_NUM):
        t = Thread(target=run_worker_func, args=(num + 1,))
        worker_thread_list.append(t)
        t.start()

    for t in worker_thread_list:
        t.join()
