# -*- coding: utf-8 -*-
import queue
import threading
import time

import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['SimHei']  # 显示汉字

queueLock = threading.Lock()
reqQueue = queue.Queue(1000)

workers = []
over_flag = False


class client(threading.Thread):
    def __init__(self, req_rate, req_num):
        threading.Thread.__init__(self)
        self.req_rate = req_rate
        self.time = 1 / req_rate
        self.req = 0
        self.req_name = "req "
        self.req_num = req_num

    def run(self):
        while self.req_num != self.req:
            thread = reqThread(self.req, self.req_name + str(self.req))
            thread.start()
            reqQueue.put(thread)
            time.sleep(self.time)
            self.req += 1


class reqThread(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.time_stamp = time.time()

    def run(self):
        print("req到达：%s" % self.name)
        # 查看时间戳
        # print("req到达：%s , time: %f  " % (self.name, self.time_stamp))


class master(threading.Thread):
    def __init__(self, req_num):
        threading.Thread.__init__(self)
        self.req_num = req_num

    def run(self):
        while self.req_num != 0:
            for each_worker in workers:
                if detection():
                    queueLock.acquire()
                    each_worker.collect()
                    self.req_num -= 1

        pic()


class worker(threading.Thread):
    def __init__(self, workerID, batch_size, duration):
        threading.Thread.__init__(self)
        self.workerID = workerID
        self.batch_size = batch_size
        self.duration = duration
        self.task_queue = queue.Queue(batch_size)
        self.wcl_list = []

    def collect(self):
        self.task_queue.put(self.collect_req())
        if self.task_queue.full():
            self.work()

    def collect_req(self):
        if not reqQueue.empty():
            data = reqQueue.get()
            queueLock.release()
            print("put req %s in %s worker!" % (data.threadID, self.workerID))
            return data
        else:
            print("req queue is empty!")
            return None

    def work(self):
        first_req_time = self.task_queue.get().time_stamp
        self.task_queue.queue.clear()

        print("worker %s work..." % self.workerID)
        time.sleep(self.duration)

        done_time = time.time()
        wcl_time = done_time - first_req_time
        self.wcl_list.append(wcl_time)
        print("worker %s done...and WCL is %s " % (self.workerID, wcl_time))

    def run(self):
        print("worker %s 启动！" % self.workerID)


def create_workers(work_num, batch_size, duration):
    for i in range(work_num):
        new_worker = worker(i, batch_size, duration)
        workers.append(new_worker)
        new_worker.start()


def detection():
    while reqQueue.empty():
        continue
    return True


def pic():
    pic_worker = workers[0]
    pic_x = range(1, len(pic_worker.wcl_list) + 1)

    color_list = ['r', 'salmon', 'tomato', 'lightsalmon', 'firebrick', 'tan']
    marker_list = ['o', 'v', 's', 'p', 'h', 'd']
    index = 0

    for done_worker in workers:
        plt.plot(pic_x,
                 done_worker.wcl_list,
                 color=color_list[index],
                 marker=marker_list[index],
                 linestyle='-',
                 label=done_worker.workerID
                 )
        index += 1

    plt.legend()  # 显示图例
    plt.xticks(pic_x, pic_x, rotation=45)
    plt.xlabel("运行次数")  # X轴标签
    plt.ylabel("Worst Case Latency")  # Y轴标签
    plt.show()


if __name__ == '__main__':
    create_workers(6, 10, 1)

    Client = client(10, 1200)
    Client.start()

    Master = master(1200)
    Master.start()
