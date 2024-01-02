from timer import cur_time_ms

with open('result/waiting_time/LATEST_sdc_original_2nd_log_ms_waiting_time', 'w') as waiting_time_file:
    pass


def run_fetcher(submitted_job_queue, waiting_job_queues, fin_job_num):
    for job in submitted_job_queue:
        waiting_job_queues[job.queue_type].append(job)
        fin_job_num[1] += 1

    submitted_job_queue.clear()


def run_dispatcher(topo, waiting_job_queue, executing_job_queue, finished_job_queue, fin_job_num):
    temp_waiting_job_queue = []

    for job in waiting_job_queue:
        req_core = job.required_num_cores
        for i in range(len(topo)):
            if topo[i].avail_num_cores >= req_core:
                topo[i].avail_num_cores -= req_core
                job.alloc_host = i
                job.execute_start_time = cur_time_ms()
                # waiting time 계산
                job.waiting_time = job.execute_start_time - job.submitted_time

                executing_job_queue.append(job)

                fin_job_num[2] += 1
                fin_job_num[5] += 1

                break

            if i == (len(topo) - 1):
                temp_waiting_job_queue.append(job)

    waiting_job_queue.clear()
    for job in temp_waiting_job_queue:
        waiting_job_queue.append(job)
    temp_waiting_job_queue.clear()

    execute_job(executing_job_queue, finished_job_queue, fin_job_num)
    release_resource(topo, finished_job_queue, fin_job_num)


def execute_job(executing_job_queue, finished_job_queue, fin_job_num):
    temp_executing_job_queue = []

    for job in executing_job_queue:
        cur_time = cur_time_ms()
        cur_runtime = cur_time - job.execute_start_time
        is_execution_finished = True if (cur_runtime >= job.runtime) else False

        if is_execution_finished:
            finished_job_queue.append(job)
            fin_job_num[3] += 1
            fin_job_num[5] -= 1
        else:
            temp_executing_job_queue.append(job)

    executing_job_queue.clear()
    for job in temp_executing_job_queue:
        executing_job_queue.append(job)
    # executing_job_queue = temp_executing_job_queue[:]
    temp_executing_job_queue.clear()


def release_resource(topo, finished_job_queue, fin_job_num):
    fin_job_list = []

    for job in finished_job_queue:
        topo[job.alloc_host].avail_num_cores += job.required_num_cores
        fin_job_num[4] += 1

        fin_job_list.append(job)

    printer = ''

    for job in fin_job_list:
        printer += f'{job.job_id} {job.waiting_time} {job.queue_type}\n'

        # waiting time 관련 로그 저장
    with open('result/waiting_time/LATEST_sdc_original_2nd_log_ms_waiting_time', 'a') as waiting_time_file:
        waiting_time_file.write(printer)

    finished_job_queue.clear()
