import time

def simplified_two_stage_process(job, topo, iter):
    min_val = 4096
    min_idx = -1
    target_queue_type = job.queue_type

    for j in range(len(topo[target_queue_type])):
        if (topo[target_queue_type][j].avail_num_cores >= job.required_num_cores) \
                and (topo[target_queue_type][j].avail_num_cores <= min_val):
            min_val = topo[target_queue_type][j].avail_num_cores
            min_idx = j

    # 원래 지정된 target queue에 할당 가능
    if min_idx != -1:
        # 해당 queue의 host로부터 CPU 할당
        topo[target_queue_type][min_idx].avail_num_cores -= job.required_num_cores

        job.queue_type = target_queue_type
        job.alloc_host = min_idx
    else:
        min_val = 4096
        min_idx_queue = -1
        min_idx_host = -1
        target_queue_type = job.queue_type

        for j in range(len(topo)):
            if j == target_queue_type:
                continue

            for k in range(len(topo[j])):
                if (topo[j][k].avail_num_cores >= job.required_num_cores) and (
                        topo[j][k].avail_num_cores < min_val):
                    min_val = topo[j][k].avail_num_cores
                    min_idx_queue = j
                    min_idx_host = k

        # 다른 queue에는 할당 가능
        if min_idx_queue != -1:
            topo[min_idx_queue][min_idx_host].avail_num_cores -= job.required_num_cores
            job.queue_type = min_idx_queue
            job.alloc_host = min_idx_host
        # 할당 가능한 queue가 없는 상태 (원래 대로면 vec_c에 넣는 상황)
        else:
            print('=========== 2-stage => need additional logic ==========')
            exit(-1)


def simplified_dispatcher(topo, job):
    req_core = job.required_num_cores

    for i in range(len(topo)):
        if topo[i].avail_num_cores >= req_core:
            topo[i].avail_num_cores -= req_core
            job.alloc_host = i
            job.execute_start_time = job.submitted_time
            # waiting time 계산
            job.waiting_time = job.execute_start_time - job.submitted_time

            break

        # target queue를 2-stage서 지정을 했는데 dispatcher 단계에서는 해당 queue가 꽉 찬 경우 (둘 간 delay 때문에 발생)
        if i == (len(topo) - 1):
            print('======== dispatcher => need additional logic =========')
            exit(-1)


def run_monitor(tplg, topo, iteration):
    num_host = tplg.num_host_types
    num_queue = tplg.num_queue_types
    num_core = tplg.num_total_cores
    tot_core = [0 for i in range(num_host)]
    #util_core = [0 for i in range(num_host)]
    tot_queue = [0 for i in range(num_queue)]
    #util_queue = [0 for i in range(num_queue)]

    for i in range(len(tplg.hosts)):
        tot_core[tplg.hosts[i].host_type] += tplg.hosts[i].num_cores
        tot_queue[tplg.hosts[i].queue_type] += tplg.hosts[i].num_cores

    ovr_core = 0
    printer = ''

    util_core = [0 for i in range(num_host)]
    util_queue = [0 for i in range(num_queue)]

    for i in range(len(topo)):
        for j in range(len(topo[i])):
            curr_core = topo[i][j].num_cores - topo[i][j].avail_num_cores
            util_core[topo[i][j].host_type] += curr_core
            util_queue[topo[i][j].queue_type] += curr_core

    printer += '===========[ Iteration '
    if iteration < 10:
        printer += "    "
    elif iteration < 100:
        printer += "   "
    elif iteration < 1000:
        printer += "  "
    elif iteration < 10000:
        printer += " "
    printer += str(iteration)
    printer += " ]=================================================================================================\n"

    printer += '\nutil_core : '
    for i in range(num_host):
        printer += f"{util_core[i]}({tot_core[i] - util_core[i]}) "

    printer += '\nutil_queue : '
    for i in range(num_queue):
        printer += f"{util_queue[i]}({tot_queue[i] - util_queue[i]}) "

    printer += "\nHost:   "
    for i in range(num_host):
        printer += f"    {i}        "

    printer += "\nH.Util:  "
    ovr_core = 0
    for i in range(num_host):
        util_per = util_core[i] / tot_core[i]
        printer += f"  {util_per:.6f}   "
        ovr_core += util_core[i]

    printer += "\nQueue:  "
    for i in range(num_queue):
        printer += f"    {i}        "

    printer += "\nQ.Util:  "
    for i in range(num_queue):
        util_per = util_queue[i] / tot_queue[i]
        printer += f"  {util_per:.6f}   "

    printer += "\nOvr.Util: "
    util_ovr_per = ovr_core / num_core
    printer += f" {util_ovr_per:.6f}   \n"

    #printer += f"==== total : 22467 ===="
    #printer += f" delegate_job_num : {fin_job_num[0]}"
    #printer += f" fetcher_job_num : {fin_job_num[1]}"
    #printer += f" dispatcher_job_num : {fin_job_num[2]}"
    #printer += f" execute_job_num : {fin_job_num[3]}"
    #printer += f" release_job_num : {fin_job_num[4]}"

    #print(printer)
    # if flag_fout:
    with open('result', 'a') as file:
        file.write(printer + "\n")
