import time


def run_monitor(tplg, topo, fn_res, stop_event, fin_job_num):
    # result 파일 초기화
    with open(fn_res, "w") as file:
        pass

    num_host = tplg.num_host_types
    num_queue = tplg.num_queue_types
    num_core = tplg.num_total_cores
    tot_core = [0 for i in range(num_host)]
    util_core = [0 for i in range(num_host)]
    tot_queue = [0 for i in range(num_queue)]
    util_queue = [0 for i in range(num_queue)]

    iteration = 0

    for i in range(len(tplg.hosts)):
        tot_core[tplg.hosts[i].host_type] += tplg.hosts[i].num_cores
        tot_queue[tplg.hosts[i].queue_type] += tplg.hosts[i].num_cores

    while not stop_event.is_set():
        time.sleep(3)
        ovr_core = 0
        printer = ""

        util_core = [0 for i in range(num_host)]
        util_queue = [0 for i in range(num_queue)]

        for i in range(len(topo)):
            for j in range(len(topo[i])):
                curr_core = topo[i][j].num_cores - topo[i][j].avail_num_cores
                util_core[topo[i][j].host_type] += curr_core
                util_queue[topo[i][j].queue_type] += curr_core

        printer += "===========[ Iteration "
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

        printer += "\nutil_core : "
        for i in range(num_host):
            printer += f"{util_core[i]}({tot_core[i] - util_core[i]}) "

        printer += "\nutil_queue : "
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

        printer += f"==== total : 22467 ===="
        printer += f" delegate_job_num : {fin_job_num[0]}"
        printer += f" fetcher_job_num : {fin_job_num[1]}"
        printer += f" dispatcher_job_num : {fin_job_num[2]}"
        printer += f" execute_job_num : {fin_job_num[3]}"
        printer += f" release_job_num : {fin_job_num[4]}"
        printer += f" now_executing_job_num : {fin_job_num[5]}"

        print(printer)
        # if flag_fout:
        with open(fn_res, "a") as file:
            file.write(printer + "\n")

        iteration += 1
