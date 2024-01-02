from job import JobLog
from topology import Topology

# from worker import run_worker
from unused_worker import run_worker
from scheduler import run_fetcher, run_dispatcher
from monitor import run_monitor
import threading
import time

if __name__ == "__main__":
    ######################
    # args 파싱 방식으로 수정하기
    ######################
    fn_cluster = "cluster"
    fn_log = "./log/2nd_log_ms"
    fn_res = "./result/LATEST_fcfs_2nd_log_ms"

    # topo = []
    delegate_queue = []
    submitted_job_queue = []
    waiting_job_queues = []
    executing_job_queues = []
    finished_job_queues = []

    # tplg : cluster 파일의 host들을 배열에 순서대로 저장
    tplg = Topology(fn_cluster)

    # topo : 각 원소는 queue이고 각 queue 내부 원소는 해당 queue에 속하는 host들
    topo = [[] for i in range(tplg.num_queue_types)]

    for host in tplg.hosts:
        topo[host.queue_type].append(host)

    job_log = JobLog(fn_log)
    total_job_num = len(job_log.jobs)

    for i in range(tplg.num_queue_types):
        waiting_job_queue = []
        executing_job_queue = []
        finished_job_queue = []

        waiting_job_queues.append(waiting_job_queue)
        executing_job_queues.append(executing_job_queue)
        finished_job_queues.append(finished_job_queue)

    fin_job_num = [0, 0, 0, 0, 0, 0]  # delegate / fetcher / dispatcher / execute / release

    stop_event = threading.Event()
    monitor_thread = threading.Thread(
        target=run_monitor, args=(tplg, topo, fn_res, stop_event, fin_job_num)
    )
    monitor_thread.start()

    while len(job_log.jobs) > 0 or (fin_job_num[4] < total_job_num):
        run_worker(job_log, topo, delegate_queue, submitted_job_queue, fin_job_num)
        run_fetcher(submitted_job_queue, waiting_job_queues, fin_job_num)

        for i in range(tplg.num_queue_types):
            run_dispatcher(
                topo[i],
                waiting_job_queues[i],
                executing_job_queues[i],
                finished_job_queues[i],
                fin_job_num,
            )

    time.sleep(3)
    stop_event.set()
    monitor_thread.join()

    print("simulation finished")
