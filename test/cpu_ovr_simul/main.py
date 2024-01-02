import heapq
import threading

from job import JobLog
from functions import simplified_two_stage_process, simplified_dispatcher, run_monitor

from topology import Topology

"""
    object : job log에 대한 cpu overall occupancy의 upper bound 구하기
    detail : 실제 기타 overhead를 고려하지 않은 이상적인 상황 상정
    기대 효과 : 1) 해당 job log를 test에서 제외할 때 근거가 된다. ex) 2차 로그는 90% 찍는 지 테스트에서 제외할 근거 확립
               2) 실제 log scale로 테스트 가능 (n배속 효과)
"""

with open('result', 'w') as file:
    pass

fn_cluster = "../../cluster"
fn_log = "../../log/2nd_log_10000_scale_sec"
fn_res = "../../result/sdc_original_2nd_log"

tplg = Topology(fn_cluster)

topo = [[] for i in range(tplg.num_queue_types)]

for host in tplg.hosts:
    topo[host.queue_type].append(host)

job_log = JobLog(fn_log)

heap = []
topo_lock = threading.Lock()

job_log_len = len(job_log.jobs)
fin_job_num = [0]

t = 0

while True:
    if len(job_log.jobs) == 0 and (fin_job_num[0] == job_log_len):
        print(f'{t} | FIN')
        break
    # job 제출 시점 여부 확인
    elif len(job_log.jobs) > 0 and job_log.jobs[0].submitted_time == t:
        job = job_log.jobs.pop(0)

        #print(f'start {t} | job_{job.job_id}')

        # 1순위 : 종료 시간 | 2순위 : job id
        heapq.heappush(heap, (job.submitted_time + job.runtime, job.job_id, job))
        # 이 simul에선 바로 실행된다는 전제이므로 바로 CPU 할당
        simplified_two_stage_process(job, topo, t)  # job의 target queue 유지 / 변경 여부 정하고 cpu 할당
        # simplified_dispatcher(topo[job.queue_type], job)  # target host 정하고 cpu 할당
        continue

    # job 실행 완료 시점 여부 확인
    if len(heap) > 0 and heap[0][0] + heap[0][1] == t:
        job = heapq.heappop(heap)[2]
        # CPU 반납
        #print(f'end {t} | job_{job.job_id}')
        topo[job.queue_type][job.alloc_host].avail_num_cores += job.required_num_cores
        continue

    # 3초마다 monitoring 수행
    if t % 3 == 0:
        iter = int(t / 3)
        run_monitor(tplg, topo, iter)

    t += 1

print('+++++ Simulate Finished. +++++')
