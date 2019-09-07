import time
from flask import Flask, request, jsonify
from delay_queue.base import task_manager
from delay_queue.config import IMPORT_TASKS
from task_task.tasks import wait_cal

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello, World!'

# 列出导入的任务
@app.route('/job/list')
def job_list():
    if request.method == 'GET':
        return jsonify({'status': 100, 'data': list(IMPORT_TASKS)})

# task 管理
@app.route('/task', methods=['GET', 'POST', 'DELETE'])
def task():
    if request.method == 'GET':
        task_id = request.values.get('task_id')
        if task_id:
            data = task_manager.list_task(task_id)
        else:
            data = task_manager.list_task()

        return jsonify({'status': 100, 'data': data})

    if request.method == 'POST':
        # 有delay 参数设置为延迟10秒执行
        if request.json and request.json.get('delay'):
            result = wait_cal.delay(692, 28, delay=int(time.time() * 1000) + 10000)
        else:
            result = wait_cal.delay(692, 28)
        return jsonify({'status': 100, 'data': result})

    if request.method == 'DELETE':
        task_id = request.values.get('task_id')
        if task_id:
            task_manager.delete_task(task_id)
        return jsonify({'status': 100, 'data': None})


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=5000)
