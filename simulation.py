import queue
import threading
import time

queueLock = threading.Lock()
reqQueue = queue.Queue(100)

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
            for each_worker in workers:
                if self.detection():
                    queueLock.acquire()
                    each_worker.collect()

    def detection(self):
        while reqQueue.empty():
            continue
        return True


class worker(threading.Thread):
    def __init__(self, workerID, batch_size, duration):
        threading.Thread.__init__(self)
        self.workerID = workerID
        self.batch_size = batch_size
        self.duration = duration
        self.task_queue = queue.Queue(batch_size)

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

        print("worker " + str(self.workerID) + " work...")
        time.sleep(self.duration)

        done_time = time.time()
        print("worker " + str(self.workerID) + " done...and WCL is " + str(done_time - first_req_time))

    def run(self):
        print("worker" + str(self.workerID) + "启动！")


if __name__ == '__main__':
    create_workers(2, 2, 2)

    Client = client(2, 50)
    Client.start()

    Master = master()
    Master.start()
