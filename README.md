## 参考有赞设计的延迟任务队列，做了一些扩展
* 灵活的任务控制
* 简单的http api 管理
* 延迟执行，立刻执行
* 可集成到Flask等Web框架中


### 待完成
* http api 队列中ready状态的排队管理
* 重复定时任务，实现逻辑： 给普通任务添加一个tag标记为重复任务，第一次启动即投递，
  worker完成后重新计算延迟，再次投递

---
[有赞延迟队列设计](https://tech.youzan.com/queuing_delay/)
