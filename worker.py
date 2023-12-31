import time
from topology import Host

time_unit = 's'
t_st = time.time()
t_prev = time.time()
t_curr = time.time()


def two_stage(topo, delegate_queue, t_flush, submitted_job_queue, fin_job_num):
    virt_topo = []

    for i in range(len(topo)):
        virt_topo.append([])
        for j in range(len(topo[i])):
            host = topo[i][j]
            virt_host = Host(host.host_type, host.queue_type, host.num_cores, host.avail_num_cores)
            virt_topo[i].append(virt_host)

    delegate_queue_size = len(delegate_queue)

    for i in range(delegate_queue_size):
        job = delegate_queue[i]
        job.score = (job.required_num_cores * (t_curr - job.submitted_time)) / t_flush

    delegate_queue.sort(key=lambda x: x.score, reverse=True)

    vec_a, temp_vec_b, vec_b, vec_c = [], [], [], []

    for i in range(len(delegate_queue)):
        min_val = 4096
        min_idx = -1
        target_queue_type = delegate_queue[i].queue_type

        for j in range(len(virt_topo[target_queue_type])):
            if (virt_topo[target_queue_type][j].avail_num_cores >= delegate_queue[i].required_num_cores) \
                    and (virt_topo[target_queue_type][j].avail_num_cores <= min_val):
                min_val = virt_topo[target_queue_type][j].avail_num_cores
                min_idx = j

        if min_idx != -1:
            virt_topo[target_queue_type][min_idx].avail_num_cores -= delegate_queue[i].required_num_cores
            vec_a.append(delegate_queue[i])
        else:
            temp_vec_b.append(delegate_queue[i])

    for i in range(len(temp_vec_b)):
        min_val = 4096
        min_idx_queue = -1
        min_idx_host = -1
        target_queue_type = temp_vec_b[i].queue_type

        for j in range(len(virt_topo)):
            if j == target_queue_type:
                continue

            for k in range(len(virt_topo[j])):
                if (virt_topo[j][k].avail_num_cores >= temp_vec_b[i].required_num_cores) and (
                        virt_topo[j][k].avail_num_cores <= min_val):
                    min_val = virt_topo[j][k].avail_num_cores
                    min_idx_queue = j
                    min_idx_host = k

        if min_idx_queue != -1:
            virt_topo[min_idx_queue][min_idx_host].avail_num_cores -= temp_vec_b[i].required_num_cores
            temp_vec_b[i].queue_type = min_idx_queue
            vec_b.append(temp_vec_b[i])
        else:
            vec_c.append(temp_vec_b[i])

    for i in range(len(vec_a)):
        # 65ms 동안 sleep
        time.sleep(0.0065)
        submitted_job_queue.append(vec_a[i])
        fin_job_num[0] += 1

    for i in range(len(vec_b)):
        # 65ms 동안 sleep
        time.sleep(0.0065)
        submitted_job_queue.append(vec_b[i])
        fin_job_num[0] += 1

    delegate_queue.clear()
    vec_a.clear()
    temp_vec_b.clear()
    vec_b.clear()

    for job in vec_c:
        delegate_queue.append(job)
    vec_c.clear()


def run_worker(job_log, topo, delegate_queue, submitted_job_queue, fin_job_num):
    if len(job_log.jobs) == 0 and len(delegate_queue) == 0:
        return

    global t_prev, t_curr
    t_curr = time.time()

    # submit time이 된 job들을 delegate queue에 submit
    while len(job_log.jobs) > 0:
        job = job_log.jobs[0]
        submit_time = job.submitted_time
        is_time_to_submit = True if (t_curr - t_st > submit_time) else False

        if is_time_to_submit:
            job = job_log.jobs.pop(0)
            job.submitted_time = time.time()
            delegate_queue.append(job)
        else:
            break

    delegate_queue_size = len(delegate_queue)
    t_flush = t_curr - t_prev

    # print(t_flush)

    if (t_flush > 2) and (delegate_queue_size > 0):
        two_stage(topo, delegate_queue, t_flush, submitted_job_queue, fin_job_num)

        # FCFS
        """
        for job in delegate_queue:
            time.sleep(0.0065)
            submitted_job_queue.append(job)

        delegate_queue.clear()
        """

        t_prev = t_curr
