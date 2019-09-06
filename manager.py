import uuid
import json
import time
from flask import Flask, request, jsonify
from utils import redis_client
from config import job_pool_key, delay_pool_key, stop_key_name
from contr.tasks2.a_task import wait_cal2

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello, World!'


@app.route('/job2', methods=['POST'])
def job2():
    if request.method == 'POST':
        result = wait_cal2.delay(1, 2, delay=1567666120313)

        return jsonify({'status': 100, 'data': result})


@app.route('/job', methods=['POST', 'DELETE', 'GET'])
def job():
    if request.method == 'POST':
        # print(request.json)
        if request.json.get('delay'):
            delay = int(request.json.get('delay'))
        else:
            delay = int(time.time() * 1000)
        job_info = {
            'task': request.json.get('task'),
            'id': str(uuid.uuid4()),
            'delay': delay,
            'body': request.json.get('body'),
            'status': 'delay'
        }
        print(job_info)
        # 任务元数据
        redis_client.hset(job_pool_key, job_info['id'], json.dumps(job_info))
        redis_client.zadd(delay_pool_key, {job_info['id']: job_info['delay']})

        return jsonify({'status': 100})

    if request.method == 'DELETE':
        task_id = request.values.get('task_id')
        print(task_id)
        if task_id:
            task_info = redis_client.hget(job_pool_key, task_id)
            if task_info:
                task_info = json.loads(task_info.decode('utf-8'))
                task_info['status'] = 'delete'
                redis_client.hset(job_pool_key, task_id, json.dumps(task_info))

        return jsonify({'status': 100})

    if request.method == 'GET':
        task_id = request.values.get('task_id')
        if task_id:
            task_info = redis_client.hget(job_pool_key, task_id)
            task_info = json.loads(task_info.decode('utf-8'))
        else:
            task_info = redis_client.hgetall(job_pool_key)
            print(task_info)
            if task_info:
                task_info = [json.loads(i.decode('utf-8')) for i in task_info.values()]
            else:
                task_info = []
        return jsonify({'status': 100, 'data': task_info})


@app.route('/worker', methods=['POST'])
def worker_mange():
    if request.method == 'POST':
        redis_client.set(stop_key_name, 1)

    return jsonify({'status': 100})


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=5000)
