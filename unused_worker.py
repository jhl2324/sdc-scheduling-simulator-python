import timer
import time
from topology import Host

time_unit = 's'  # main으로 옮기기
scheduling_policy = 'sdc_advanced'  # main으로 옮기기
flush_period = 2  # main으로 옮기기
is_mid_checked = False
flush_count = 0
ou_queue = []
period_queue = [flush_period, flush_period]

t_st = time.time()
t_prev = time.time()
t_curr = time.time()

with open('./result/latest_log_lambda', 'w') as lambda_file:
    pass
with open('./result/latest_log_flush_period', 'w') as period_file:
    pass


def run_worker(job_log, topo, delegate_queue, submitted_job_queue, fin_job_num):
    global time_unit, scheduling_policy, flush_period, is_mid_checked, flush_count, ou_queue, period_queue, t_prev, t_curr

    if len(job_log.jobs) == 0 and len(delegate_queue) == 0:
        return

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

    t_flush = t_curr - t_prev

    is_mid_time_to_flush_sdc_advanced = (t_flush > (flush_period / 2)) and (
            is_mid_checked == False) and scheduling_policy == 'sdc_advanced'
    is_time_to_flush_sdc_advanced = (t_flush > flush_period) and (
            scheduling_policy == 'sdc_advanced')
    is_time_to_flush_others = (t_flush > flush_period) and (scheduling_policy != 'sdc_advanced')

    if is_mid_time_to_flush_sdc_advanced:
        is_mid_checked = True

        cur_core_sum = 0
        total_core_sum = 5756

        for queue in topo:
            for host in queue:
                cur_core_sum += host.avail_num_cores

        prev_core_sum = None

        if flush_count == 0:
            prev_core_sum = total_core_sum
        elif flush_count == 1:
            prev_core_sum = total_core_sum - ou_queue[0]
        else:
            prev_core_sum = total_core_sum - ou_queue[1]

        if prev_core_sum < cur_core_sum:
            execute_scheduling_policy(topo, delegate_queue, submitted_job_queue, scheduling_policy, fin_job_num)

    elif is_time_to_flush_sdc_advanced:
        execute_scheduling_policy(topo, delegate_queue, submitted_job_queue, scheduling_policy, fin_job_num)
    elif is_time_to_flush_others:
        execute_scheduling_policy(topo, delegate_queue, submitted_job_queue, scheduling_policy, fin_job_num)


def execute_scheduling_policy(topo, delegate_queue, submitted_job_queue, scheduling_policy, fin_job_num):
    if scheduling_policy == 'fcfs':
        for i in range(len(delegate_queue)):
            job = delegate_queue[i]
            submitted_job_queue.append(job)
        delegate_queue.clear()

    elif scheduling_policy == 'sdc_vanilla':
        virt_topo = init_virt_topo(topo)
        cur_time = time.time()

        for i in range(len(delegate_queue)):
            delegate_queue[i].score = delegate_queue[i].required_num_cores * (
                    cur_time - delegate_queue[i].submitted_time)

        two_stage_process(virt_topo, delegate_queue, submitted_job_queue)

    elif scheduling_policy == 'sdc_advanced':
        global flush_period, is_mid_checked, period_queue, lambda_val
        virt_topo = init_virt_topo(topo)

        if len(delegate_queue) > 1:
            req_num_cores_list = list(map(lambda x: x.required_num_cores, delegate_queue))
            min_core = min(req_num_cores_list)
            max_core = max(req_num_cores_list)

            normalized_req_num_cores_list = []

            if min_core < max_core:
                normalized_req_num_cores_list = list(
                    map(lambda x: (x - min_core) / (max_core - min_core), req_num_cores_list))
            else:
                normalized_req_num_cores_list = list(map(lambda x: 0.5, req_num_cores_list))

            pending_time_list = list(
                map(lambda x: (time.time() - x.submitted_time) / flush_period, delegate_queue))
            min_pending_time = min(pending_time_list)
            max_pending_time = max(pending_time_list)

            normalized_pending_time_list = []

            if min_pending_time < max_pending_time:
                normalized_pending_time_list = list(
                    map(lambda x: (x - min_pending_time) / (max_pending_time - min_pending_time), pending_time_list))
            else:
                normalized_pending_time_list = list(map(lambda x: 0.5, pending_time_list))

            normalized_score_list = []

            for i in range(len(delegate_queue)):
                score = (normalized_req_num_cores_list[i] * normalized_pending_time_list[i])

            #global period_queue
            lambda_val = 1 / ((period_queue[1] / period_queue[0]) + 1)
            #print(f'+++++++++++++ lambda : {lambda_val}+++++++++++')

            for i in range(len(delegate_queue)):
                delegate_queue[i].score = lambda_val * normalized_req_num_cores_list[i] + (1 - lambda_val) * \
                                          normalized_pending_time_list[i]

            delegate_queue.sort(key=lambda x: x.score, reverse=True)

        two_stage_process(virt_topo, delegate_queue, submitted_job_queue, fin_job_num)

        avail_core_sum = 0
        total_core_sum = 0

        for queue in topo:
            for host in queue:
                avail_core_sum += host.avail_num_cores
                total_core_sum += host.num_cores

        ou = total_core_sum - avail_core_sum

        if len(ou_queue) == 2:
            ou_queue.pop(0)

        ou_queue.append(ou)

        if len(ou_queue) == 2:
            flush_period = flush_period * 0.5 if (not ou_queue[0] or not ou_queue[1]) else ou_queue[1] / ou_queue[0]

        period_queue.pop(0)
        period_queue.append(flush_period)
        is_mid_checked = False

        #print(f'+++++++++++++ period : {flush_period}+++++++++++')
        with open('./result/latest_log_flush_period', 'a') as file:
            file.write(str(flush_period) + '\n')
        with open('./result/latest_log_lambda', 'a') as lambda_file:
            lambda_file.write(str(lambda_val) + '\n')

    global t_prev
    # t_curr 했을 때와 차이 심한가?
    t_prev = time.time()


def init_virt_topo(topo):
    virt_topo = []

    for i in range(len(topo)):
        virt_topo.append([])

        for j in range(len(topo[i])):
            host = topo[i][j]
            virt_host = Host(host.host_type, host.queue_type, host.num_cores, host.avail_num_cores)
            virt_topo[i].append(virt_host)

    return virt_topo


def two_stage_process(virt_topo, delegate_queue, submitted_job_queue, fin_job_num):
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
                        virt_topo[j][k].avail_num_cores < min_val):
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
        #time.sleep(0.0065)
        submitted_job_queue.append(vec_a[i])
        fin_job_num[0] += 1

    for i in range(len(vec_b)):
        # 65ms 동안 sleep
        #time.sleep(0.0065)
        submitted_job_queue.append(vec_b[i])
        fin_job_num[0] += 1

    delegate_queue.clear()
    vec_a.clear()
    temp_vec_b.clear()
    vec_b.clear()

    for job in vec_c:
        delegate_queue.append(job)
    vec_c.clear()
