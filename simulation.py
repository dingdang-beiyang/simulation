import queue
import threading
import time

queueLock = threading.Lock()
reqQueue = queue.Queue(100)

workers = []


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

    def run(self):
        print("req到达：" + self.name + "  ")


def create_workers(work_num, batch_size, duration):
    for i in range(work_num):
        new_worker = worker(i, batch_size, duration)
        workers.append(new_worker)
        new_worker.start()


class master(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            for worker in workers:
                worker.collect()


def collect_req(workerID):
    queueLock.acquire()
    if not reqQueue.empty():
        data = reqQueue.get()
        queueLock.release()
        print("put req %s in %s worker!" % (data.threadID, workerID))
        return True
    else:
        queueLock.release()
        return False


class worker(threading.Thread):
    def __init__(self, workerID, batch_size, duration):
        threading.Thread.__init__(self)
        self.workerID = workerID
        self.batch_size = batch_size
        self.duration = duration
        self.task = 0

    def collect(self):
        if collect_req(self.workerID):
            self.task += 1
        if self.task == self.batch_size:
            self.task = 0
            time.sleep(self.duration)

    def run(self):
        print("work" + str(self.workerID) + "启动！")


if __name__ == '__main__':
    create_workers(5, 4, 1)

    Client = client(2, 50)
    Client.start()

    Master = master()
    Master.start()
