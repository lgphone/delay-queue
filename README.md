# delay-queue
参考有赞及celery实现的延迟任务队列，基于redis 的sorted set，做了一些扩展

#### 功能
1. 灵活的任务控制
2. 简单的http api 管理
3. 延迟执行，立刻执行
4. 可集成到Flask等Web框架中


#### 待完成
1. http api 队列中ready状态的排队管理
2. 重复定时任务，实现逻辑： 给普通任务添加一个tag标记为重复任务，第一次启动即投递，
  worker完成后重新计算延迟，再次投递
3. 多路由，多队列，启动多个timer 去轮询对应队列，启动多个worker去执行对应队列的任务


#### 项目说明
* manager.py 基于Flask的http api，可修改任务的一些参数，删除任务，查看任务
* timer.py  延迟任务检查器 时刻轮询获取到期的任务，并投递到执行任务队列
* worker.py 任务执行器 从队列中获取任务并执行，返回结果，基于多线程，可配置线程数量
* delay_queue.base 基于类装饰器实现了任务函数添加delay功能
* delay_queue.config 配置文件
* delay_queue.utils 一些工具

#### 如何启动
1. 配置好redis等参数后
2. pip install -r requeriments.txt
3. python manager.py  # 启动http api管理服务
4. python worker.py   # 启动任务执行器
5. python timer.py    # 启动延迟检查器


---
#### 引用
[有赞延迟队列设计](https://tech.youzan.com/queuing_delay/)
