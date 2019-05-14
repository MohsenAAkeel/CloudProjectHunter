import time

class JobTimer:
    def __init__(self):
        self.jobs = [] # list of lists, element = [job #, run time, start time]

    def add_job(self, job_number, job_runtime):
        self.jobs.append([job_number, job_runtime, time.time()])

    def remove_job(self, job_num):
        for x in self.jobs:
            if int(x[1]) == int(job_num):
                self.jobs.remove(x)

    def check_times(self):
        # returns a list of job numbers that are done 'computing'

        finished_jobs = []
        for x in self.jobs:
            time_diff = time.time() - x[2]
            if time_diff >= x[1]:  # check if runtime is met
                finished_jobs.append(int(x[0]))
        return finished_jobs
