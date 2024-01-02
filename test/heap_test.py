import heapq
from job import Job


heap = []

job1 = Job(1, 2, 4, 4, 5)
job2 = Job(2, 3, 1, 5, 6)
job3 = Job(3, 1, 1, 1, 1)
job4 = Job(4, 0, 11, 2, 5)

heapq.heappush(heap, (job1.submitted_time, job1.job_id, job1))
heapq.heappush(heap, (job2.submitted_time, job2.job_id, job2))
heapq.heappush(heap, (job3.submitted_time, job3.job_id, job3))
heapq.heappush(heap, (job4.submitted_time, job4.job_id, job4))

print(heapq.heappop(heap)[2].job_id)
