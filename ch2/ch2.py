import ray, time

ray.init()

print(ray.cluster_resources())  # 레이 클러스터의 사용량 확인

database = ["Learning", "Ray", "Flexible", "Distributed", "Python", "for", "Machine", "Learning"]

def retrieve(item):
    time.sleep(item / 10)
    return item, database[item]

def print_runtime(input_data, start_time):
    print(f'Runtime: {time.time() - start_time:.2f} seconds, data:')
    print(*input_data, sep="\n")

# start = time.time()
# data = [retrieve(item) for item in range(8)]
# print_runtime(data, start)

# @ray.remote # 어떤 파이썬 함수이든 레이 태스크로 만든다
# def retrieve_task(item):    # ray.remote를 붙이기 위해 만든 함수일 뿐임
#     return retrieve(item)
#
# start = time.time()
# object_references = [retrieve_task.remote(item) for item in range(8)]
# data = ray.get(object_references)
# print_runtime(data, start)

db_object_ref = ray.put(database)

@ray.remote # 어떤 파이썬 함수이든 레이 태스크로 만든다
def retrieve_task(item, db):    # ray.remote를 붙이기 위해 만든 함수일 뿐임
    time.sleep(item / 10)
    return item, db[item]

start = time.time()
object_references = [retrieve_task.remote(item, db_object_ref) for item in range(8)]
data = ray.get(object_references)
print_runtime(data, start)

start = time.time()
object_references = [
    retrieve_task.remote(item, db_object_ref) for item in range(8)
]
all_data = []

while len(object_references) > 0:
    finished, object_references = ray.wait(
        object_references, num_returns=2, timeout=7.0
    )
    data = ray.get(finished)
    print_runtime(data, start)
    all_data.extend(data)

print_runtime(all_data, start)

@ray.remote
class DataTracker:
    def __init__(self):
        self._counts = 0

    def increment(self):
        self._counts += 1

    def counts(self):
        return self._counts

@ray.remote
def retrieve_tracker_task(item, tracker, db):
    time.sleep(item / 10)
    tracker.increment.remote()
    return item, db[item]

tracker = DataTracker.remote()

object_references = [retrieve_tracker_task.remote(item, tracker, db_object_ref) for item in range(8)]
data = ray.get(object_references)

print(data)
print(ray.get(tracker.counts.remote()))