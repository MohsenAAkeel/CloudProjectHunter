import sys
import os
import json
import subprocess
from job_timers import *


# opens the job json file and returns the conents
def read_job(file_name, path):

    route = str(path + '\\' + file_name)

    with open(route, 'r') as file:
        data = json.load(file)

    return data


# assign a new job to a a vm, returns a tuple (success, [VM#, job num, CPU alloc, Mem alloc, Net alloc, Sec transfer, runtime, source domain])
def assign_job(obj_vm_list, obj_job, job_num, alpha=0):
    # Incemental reduction factor reached 1 i.e. assignment not possible
    if alpha >= 100:
        return (0, -1, -1, -1)

    # Assignment may be possible. Assign to VM with highest net benefit
    else:
        Benefit_vm1 = -1
        Benefit_vm2 = -1
        Benefit_vm3 = -1

        vm1_specs = obj_vm_list.get('domaina')
        vm2_specs = obj_vm_list.get('domainb')
        vm3_specs = obj_vm_list.get('domainc')

        # Calculate VM 1 benefit if all constraints are met
        if (float(vm1_specs[4]) - float(obj_job['max_cpu']) + alpha >= 0) and (
                float(vm1_specs[5]) - float(obj_job['max_mem']) + alpha >= 0) and (
                float(obj_job['sec_level']) <= float(vm1_specs[6])) and (
                float(obj_job['max_cpu']) - alpha >= float(obj_job['min_cpu'])) and (
                float(obj_job['max_mem']) - alpha >= float(obj_job['min_mem'])):
            Benefit_vm1 = (float(vm1_specs[4]) - float(obj_job['max_cpu']) + alpha) + (
                        float(vm1_specs[5]) - float(obj_job['max_mem']) + alpha)

        # Calculate VM 2 benefit if all constraints are met
        if (float(vm2_specs[4]) - float(obj_job['max_cpu']) + alpha >= 0) and (
                float(vm2_specs[5]) - float(obj_job['max_mem']) + alpha >= 0) and (
                float(obj_job['sec_level']) <= float(vm2_specs[6])) and (
                float(obj_job['max_cpu']) - alpha >= float(obj_job['min_cpu'])) and (
                float(obj_job['max_mem']) - alpha >= float(obj_job['min_mem'])):
            Benefit_vm2 = (float(vm2_specs[4]) - float(obj_job['max_cpu']) + alpha) + (
                        float(vm2_specs[5]) - float(obj_job['max_mem']) + alpha)

            # Calculate VM 3 benefit if all constraints are met
        if (float(vm3_specs[4]) - float(obj_job['max_cpu']) + alpha >= 0) and (
                float(vm3_specs[5]) - float(obj_job['max_mem']) + alpha >= 0) and (
                float(obj_job['sec_level']) <= float(vm3_specs[6])) and (
                float(obj_job['max_cpu']) - alpha >= float(obj_job['min_cpu'])) and (
                float(obj_job['max_mem']) - alpha >= float(obj_job['min_mem'])):
            Benefit_vm3 = (float(vm3_specs[4]) - float(obj_job['max_cpu']) + alpha) + (
                        float(vm3_specs[5]) - float(obj_job['max_mem']) + alpha)

        # Max benefit unchanged. Try with higher incremental reduction factor
        if max(Benefit_vm1, Benefit_vm2, Benefit_vm3) == -1:
            return assign_job(obj_vm_list, obj_job, alpha + 0.5)

         # (success, job_num, [VM  # , CPU alloc, Mem alloc, Net alloc, SSpec])
         # Assignment successful. Return tuple: {1 i.e. successful, [VM#, job_num, CPU alloc, Mem alloc, sec_tramsfer}
        else:
            if Benefit_vm1 == max(Benefit_vm1, Benefit_vm2, Benefit_vm3):
                return (1, ['domaina', job_num, float(obj_job['max_cpu']) - alpha, float(obj_job['max_mem']) - alpha, obj_job['sec_transfer'], obj_job["runtime"], obj_job["source"]])
            if Benefit_vm2 == max(Benefit_vm1, Benefit_vm2, Benefit_vm3):
                return (1, ['domainb', job_num, float(obj_job['max_cpu']) - alpha, float(obj_job['max_mem']) - alpha, obj_job['sec_transfer'], obj_job["runtime"], obj_job["source"]])
            else:
                return (1, ['domainc', job_num, float(obj_job['max_cpu']) - alpha, float(obj_job['max_mem']) - alpha, obj_job['sec_transfer'], obj_job["runtime"], obj_job["source"]])


# send a job to an assigned vm
def send_job(assigned_job, vm_list, source):
    vm = assigned_job[0]
    destination = vm_list[vm][0]

    command = "sudo ./sendt " + assigned_job[1] + str(source) + ' ' + str(destination) + ' ' + str(assigned_job[5])
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()

    return 1


# update the available resources on vms
def update_vm_list(orig_list, aligned_vm, option):
    vm = aligned_vm[0]
    if option == "sub":
        orig_list[vm][5] = int(orig_list[vm][5]) - aligned_vm[2]
        orig_list[vm][6] = int(orig_list[vm][6]) - aligned_vm[3]
    elif option == "add":
        orig_list[vm][5] = int(orig_list[vm][5]) + aligned_vm[2]
        orig_list[vm][6] = int(orig_list[vm][6]) + aligned_vm[3]
    return orig_list


# send the user ui info as a json to a designated file
def send_user_ui(pass_test, comment, path):
    filename = 'user_data.json'
    path_to_file = os.path.join(path, filename)
    x = json.dumps([pass_test, comment])
    with open(path_to_file, 'w') as json_file:
        json.dump(x, json_file)


# send admin info as a json to a designated file
def send_admin_ui(data, path):
    filename = 'admin_data.json'
    path_to_file = os.path.join(path, filename)
    x = json.dumps(data)
    with open(path_to_file, 'w') as json_file:
        json.dump(x, json_file)


# check a designated folder for admin requests
def check_admin_requests(path):
    kill_jobs = []
    for filename in os.listdir(path):
        path_to_file = os.path.join(path, filename)
        with open(path_to_file, 'r') as file:
            tmp = list(file)
        os.remove(path_to_file)
        for x in tmp:
            kill_jobs.append(x)
    return kill_jobs


# kill a job being processed or in queue
def kill_job(job_num, job_list, job_queue, job_timers, send_queue, vm_list):
    # Locate the job
    tmp = None
    jqueued = False # check if job in job queue
    squeued = True # check if job in send queue

    for x in range(0, len(job_list)):
        if job_list[x][1] == job_num:
            tmp = job_list.pop(x)
    for x in range(0, len(job_queue)):
        if job_list[x][1] == job_num:
            tmp = job_queue.pop(x)
            jqueued = True
    for x in range(0, len(send_queue)):
        if send_queue[x][1] == job_num:
            tmp = send_queue.pop(x)
            squeued = True

    # Update the VM list and job timer list
    if tmp and jqueued is False:
        update_vm_list(vm_list, tmp, "add")
        if squeued is False:
            job_timers.remove(job_num)
    return job_list, job_queue, job_timers, send_queue, vm_list


def main():
    # path is the directory where new jobs are sent
    job_path = './job_path/'
    user_path = './ui_data/'
    admin_req_path = './admin_req/'
    admin_ui_path = './ui_data/'
    job_list = []
    job_queue = []
    send_queue = []
    job_timers = JobTimer()
    job_number = 0
    comment = ''
    host_domain = ''
    pass_test = 0

# 'VM1':['host name', 'port', 'CPU cap', 'Mem cap', 'Net cap', 'avail cpu', 'avail mem', 'avail net', 'SSpec']
    vm_list = {'domaina': ['host name', 'port', '64', '180', '10', '64', '180', '10', '3'],
               'domainb': ['host name', 'port', '128', '240', '10', '128', '240', '10', '2'],
               'domainc': ['host name', 'port', '64', '160', '10', '64', '160', '10', '1']}

    # collect the files that already exist in this dir
    job_file_list = os.listdir(job_path)

    # enter main loop
    while 1:
        # KILL JOBS BY ADMIN REQUEST OR TIMER #################################
        jobs_to_kill = []
        finished_jobs = []

        # Check for admin signals
        admin_requests = check_admin_requests(admin_req_path)
        for x in admin_requests:
            jobs_to_kill.append(x)

        # Check for completed jobs
        finished_jobs = job_timers.check_times()
        if len(finished_jobs) > 0:
            for x in finished_jobs:
                jobs_to_kill.append(x)
            finished_jobs = []

        if len(jobs_to_kill) > 0:
            for x in jobs_to_kill:
                job_list, job_queue, job_timers, send_queue, vm_list = kill_job(int(x), job_list, job_queue, job_timers, send_queue, vm_list)

        # CHECK FOR JOBS IN THE SEND QUEUE AND SEND THEM #######################
        if len(send_queue) > 0:
            for x in send_queue:
                if send_job(x, vm_list, x[7]):
                    job_list.append(x)
                    job_timers.add_job(x[1], int(x[6]))
                    send_queue.remove(x)

        # CHECK FOR JOBS IN QUEUE, OTHERWISE CHECK FOR A NEW JOB ###############
        job_hit = ""
        if len(job_queue) > 0:
            job_hit = job_queue.pop(0)
        else:
            for x in os.listdir(job_path):
                if x not in job_file_list:
                    job_hit = x
                    job_number += 1
                    job_file_list.append(x)

        # ASSIGNING A NEW JOB AND SENDING OR QUEUEING IT ########################
        if job_hit != "":
            # pull the data from the job and then assign it to a VM
            opened_job = read_job(job_hit, job_path)
            (pass_test, assigned_job) = assign_job(vm_list, opened_job, job_number)

            # if a VM meets the job's requirements, send it to a VM
            # otherwise place it in queue
            if pass_test == 1:
                vm_list = update_vm_list(vm_list, assigned_job, "sub")
                #
                if len(send_queue) == 0:
                    if send_job(assigned_job, vm_list, assigned_job[7]):
                        job_list.append(assigned_job)
                        job_timers.add_job(assigned_job[1], int(assigned_job[6]))
                    else:
                        send_queue.append(assigned_job)
                        pass_test = 0
                        comment = "Failed to connect. Will try again soon."
                else:
                    send_queue.append(assigned_job)
                    pass_test = 0
                    comment = "Waiting for bandwidth"
            else:
                job_queue.append(job_hit)
                comment = "No available resources currently match request. Job queued"

        # UPDATE GUIs ####################################################
        send_user_ui(pass_test, comment, user_path)
        send_admin_ui([job_list, job_queue, send_queue, vm_list], admin_ui_path)

        # reset values
        job_hit = ''
        comment = ''
        pass_test = 0
        print(job_list)
        print(job_queue)
        print(send_queue)
        input("Press Enter to continue...")

if __name__ == "__main__":
    main()
