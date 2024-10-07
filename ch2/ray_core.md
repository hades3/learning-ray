## **레이 코어**

파이썬 인터프리터는 사실상 싱글 스레드이다. 일반 파이썬을 사용하면 전체 클러스터 뿐만 아니라 동일한 시스템에서 여러 CPU를 활용하기도 어렵다.

파이썬 라이브러리로 제공되는 레이 코어 API는 모든 파이썬 커뮤니티에서 범용 분산 프로그래밍을 쉽게 다루는 강력한 도구이다. 레이는 쉽게 학습할 수 있게 하기 위해서 파이썬의 데코레이터, 함수, 클래스를 제공한다. 레이 코어 API의 목표는 분산 컴퓨팅을 위한 범용 프로그래밍 인터페이스의 제공이다. 

### **레이 API를 활용한 첫 번째 예시**

```
database = ["Learning", "Ray", "Flexible", "Distributed", "Python", "for", "Machine", "Learning"]

def retrieve(item):
    time.sleep(item / 10)
    return item, database[item]

def print_runtime(input_data, start_time):
    print(f'Runtime: {time.time() - start_time:.2f} seconds, data:')
    print(*input_data, sep="\n")


start = time.time()
data = [retrieve(item) for item in range(8)]
print_runtime(data, start)


"""
Runtime: 2.80 seconds, data:
(0, 'Learning')
(1, 'Ray')
(2, 'Flexible')
(3, 'Distributed')
(4, 'Python')
(5, 'for')
(6, 'Machine')
(7, 'Learning')
"""
```

반복문을 순회하면, 0 + 0.1 + 0.2 + ... 0.7 = 2.5초가 나올 것으로 예상했으나 2.8초가 나왔다. 중요한 사실은 순수 파이썬 구현으로 함수를 병렬적으로 실행할 수 없다는 것이다. 

#### **함수와 원격 레이 태스크**

병렬적으로 실행되었다면, 런타임은 최대 0.7초일 것이다. 레이를 사용해보자.

```
@ray.remote # 어떤 파이썬 함수이든 레이 태스크로 만든다
def retrieve_task(item):    # ray.remote를 붙이기 위해 만든 함수일 뿐임
    return retrieve(item)
```

이제 retrieve\_task 함수는 레이 태스크가 되어 호출된 프로세스가 아닌 다른 프로세스, 즉 다른 컴퓨터에서 실행되는 함수이다.

```
start = time.time()
object_references = [retrieve_task.remote(item) for item in range(8)] # remote()로 item을 레이 클러스터로 전달해 retrieve_task 함수를 실행한다. 각 태스크는 오브젝트를 반환한다.
data = ray.get(object_references)	# 레이 오브젝트 레퍼런스와 실제 데이터를 가져오기 위해서 ray.get을 사용한다
print_runtime(data, start)
```

remote()를 호출하여 원격으로 레이 태스크를 실행한다. 레이는 로컬 클러스터에서 원격으로 실행하더라도 병렬적이고, 비동기로 실행한다. 

object\_references에는 Object가 아닌 ObjectRef가 들어있다. 이 ObjectRef를 이용하여 결과를 얻기 위해 ray.get을 호출한다. 레이 태스크는 오브젝트 레퍼런스를 생성한다.

정리하면, 파이썬 함수에 @ray.remote 데코레이터를 적용하여 레이 태스크로 만들고, 원래 함수를 호출하는 방법 대신 remote를 사용하여 원격으로 레이 태스크를 실행하고, 결과를 얻기 위해 오브젝트 레퍼런스를 대상으로 ray.get을 실행한다.

위 실행 결과는 본인의 컴퓨터에서는 1.16초가 나왔다. ( 워커 프로세스의 기본값은 CPU의 수인데, 본인의 CPU는 8이기 때문으로 추정 )

#### **put과 get을 사용해 오브젝트 스토어 사용하기**

앞에서는 retrieve 함수를 사용해 데이터베이스의 아이템에 직접 접근했다. 로컬 레이 클러스터에서는 괜찮지만 여러 대의 컴퓨터가 있는 실제 클러스터에서 실행된다고 생각하면, 어떻게 모든 컴퓨터가 동일한 데이터에 접근할 수 있을까?

데이터베이스는 현재, 드라이버에만 들어있지만, 태스크를 실행하는 워커가 retrieve 태스크를 실행하려면 워커에서도 그 데이터베이스에 접근해야 한다. 다행히 레이는 드라이버와 워커가 오브젝트를 공유하는 쉬운 방법을 제공한다. 데이터를 분산 오브젝트 스토어에 넣을 때는 put을 사용한다. 

```
db_object_ref = ray.put(database)

@ray.remote # 어떤 파이썬 함수이든 레이 태스크로 만든다
def retrieve_task(item, db):    # ray.remote를 붙이기 위해 만든 함수일 뿐임
    time.sleep(item / 10)
    return item, db[item]

start = time.time()
object_references = [retrieve_task.remote(item, db_object_ref) for item in range(8)]
data = ray.get(object_references)
print_runtime(data, start)
```

이렇게 오브젝트 스토어를 사용하면, 레이는 클러스터 전체에서 처리할 데이터에 접근할 수 있다. 오브젝트 스토어와 상호작용하면, 약간의 오버헤드가 발생하지만 더 크고 현실적인 데이터셋으로 작업할 때는 성능이 향상된다.

#### **논블로킹 호출에 대해 레이의 wait 함수 사용하기**

이전에 ray.get(object\_references)를 사용해 결과값을 얻었다. 이 호출은 블로킹 중이며, 드라이버는 태스크가 완료되어 모든 결과값이 나올 때까지 기다려야 한다. 하지만 데이터베이스 아이템을 처리하는 데 더 오랜 시간이 걸린다고 하면, 다른 태스크를 위해 드라이버 프로세스를 사용하는 것이 좋을 것이다. 

데이터베이스 작업 도중 데드락이 발생한다고 가정하자. 드라이버는 그냥 멈춰서 아무 것도 처리하지 않는다. 이럴 때는 타임아웃을 거는 것이 좋다.

```
start = time.time()
object_references = [
    retrieve_task.remote(item, db_object_ref) for item in range(8)
]
all_data = []

while len(object_references) > 0:	# 블로킹하는 대신에 아직 완료되지 않은 object_references를 반복 처리
    finished, object_references = ray.wait(	# 타임아웃으로 끝난 데이터를 비동기 방식으로 기다림
        object_references, num_returns=2, timeout=7.0
    )
    data = ray.get(finished)
    print_runtime(data, start)	# 결과값이 들어오는 대로 출력
    all_data.extend(data)	# 데이터 추가

print_runtime(all_data, start)


"""
Runtime: 0.11 seconds, data:
(0, 'Learning')
(1, 'Ray')
Runtime: 0.31 seconds, data:
(2, 'Flexible')
(3, 'Distributed')
Runtime: 0.51 seconds, data:
(4, 'Python')
(5, 'for')
Runtime: 0.72 seconds, data:
(6, 'Machine')
(7, 'Learning')
"""
```

ray.wait는 실행이 완료된 값과 완료되지 않은 값을 반환한다. 기본값이 1인 num\_returns 인자를 사용해 새 데이터베이스의 아이템의 태스크가 2개가 완료될 때마다 wait가 종료되어 값을 반환한다.

#### **태스크 의존성 다루기**

```
@ray.remote
def follow_up_task(retrieve_result):
    original_item, _ = retrieve_result
    follow_up_result = retrieve(original_item + 1)
    return retrieve_result, follow_up_result


retrieve_refs = [retrieve_task.remote(item, db_object_ref) for item in [0, 2, 4, 6]]
follow_up_refs = [follow_up_task.remote(ref) for ref in retrieve_refs]

result = [print(data) for data in ray.get(follow_up_refs)]
```

follow\_up\_task의 첫째 줄에서 retrieve\_result가 튜플이라고 가정하고 튜플을 언패킹한다.

하지만, \[follow\_up\_task.remote(ref) for ref in retrieve\_refs\] 과정에서 태스크에 튜플을 전달하지 않는다. 오브젝트 레퍼런스 값을 전달한다. 내부에서는 오브젝트 레퍼런스가 아닌 실제 값이 필요하다는 것을 알아서 자동으로 ray.get을 호출한다. 레이는 모든 태스크에 대한 종속성 그래프를 만들고 종속성에 따른 순서대로 실행한다. 레이는 자동으로 시간 정보를 추론하고, 처리 과정에서 발생하는 중간 데이터의 크기가 큰 경우에 드라이버에 다시 복사할 필요도 없이 한 번에 처리하는 레이 오브젝트 스토어의 강력한 기능이 있다. 실제 데이터가 아닌 오브젝트 레퍼런스만 다음 작업으로 처리하면, 레이가 직접 나머지를 처리한다.

후속 태스크는 개별 처리 작업이 완료된 후에만 실행되도록 스케쥴링된다.

[##_Image|kage@cANf3Z/btsJTDtB8Hy/lNlaVx7oT74weUcMIq18F1/img.png|CDM|1.3|{"originWidth":802,"originHeight":690,"style":"alignCenter","width":761,"height":655}_##]

#### **클래스에서 액터로**

데이터베이스가 쿼리된 빈도를 분산된 방식으로 측정하려 한다. 이를 위해서 레이는 액터라는 개념을 사용한다. 액터를 사용하면 클러스터에서 상태 저장 연산을 실행한다. 또한 액터는 서로 통신할 수도 있다. @ray.remote를 함수에 붙이면 레이 태스크가 되고, 클래스에 붙이면 레이 액터가 된다.

```
@ray.remote
class DataTracker:
    def __init__(self):
        self._counts = 0

    def increment(self):
        self._counts += 1

    def counts(self):
        return self._counts
```

DataTracker 클래스는 ray.remote 데코레이터를 장착했으므로 액터가 되었다. 액터는 상태를 추적하지만, 여기서는 단순히 카운터 역할이며, 이 클래스의 메소드는 레이 태스크가 된다.

```
@ray.remote
def retrieve_tracker_task(item, tracker, db):
    time.sleep(item / 10.)
    tracker.increment.remote()
    return item, db[item]


tracker = DataTracker.remote()

object_references = [
    retrieve_tracker_task.remote(item, tracker, db_object_ref) for item in range(8)
]
data = ray.get(object_references)

print(data)
print(ray.get(tracker.counts.remote()))
```

### **레이 API 개요**

-   ray.init(): 레이 클러스터를 초기화한다. 기존 클러스터에 연결할 주소를 전달한다.
-   @ray.remote: 함수를 태스크로, 클래스를 액터로 바꾼다.
-   ray.put(): 오브젝트 스토어에 값을 넣는다.
-   ray.get(): 오브젝트 스토어에서 값을 가져온다. 태스크나 액터가 계산한 값을 가져온다.
-   .remote(): 레이 클러스터에서 액터 메서드 혹은 태스크를 실행하고, 액터 인스턴스화 하기 위해 사용한다.
-   ray.wait(): 두 개의 오브젝트 레퍼런스를 반환한다. 하나는 완료된 작업이고 하나는 완료되지 않은 작업이다.

## **레이 시스템 컴포넌트**

레이의 작동방식을 알아보자.

### **노드에서 태스크 스케쥴링 및 실행**

레이 클러스터는 노드로 구성된다. 전체 클러스터가 어떻게 상호작용하는지 알아보기 전에 개별 노드에서 어떻게 작동하는지 살펴보자.

워커 노드는 여러 워커 프로세스로 구성된다. 워커 프로세스는 워커라고도 한다. 각 워커는 고유한 ID, IP주소 및 참조할 포트를 가진다. 워커는 명령받은 일을 실행하는 컴포넌트이다.

각 워커 노드에는 레이렛이라는 컴포넌트가 있다. 레이렛은 워커 프로세스를 관리한다. 레이렛은 태스크 간에 공유되고, 태스크 스케줄러와 오브젝트 스토어라는 2가지 구성요소로 이루어져 있다.

오브젝트 스토어를 먼저 살펴보자. 레이 클러스터의 각 노드에는 해당 노드의 레이렛 내에 오브젝트 스토어가 장착되어 있고, 저장된 모든 오브젝트가 클러스터의 분산 오브젝트 스토어를 구성한다. 오브젝트 스토어는 동일한 노드에 있는 워커 간의 공유 메모리 풀을 관리하며 워커가 다른 노드에 생성된 오브젝트에 접근하도록 한다.

스케줄러는 리소스 관리를 한다. 예를 들어 태스크가 4개의 CPU를 필요로 한다면 스케줄러는 일을 안하는 워커 프로세스 중에서 해당 리소스에 접근 가능한 프로세스를 찾는다. 기본적으로 스케줄러는 노드에서 사용 가능한 메모리 크기를 비롯해 CPU와 GPU 수 같은 정보도 파악해 가져온다. 스케줄러가 필요한 리소스를 제공할 수 없는 상태라면 태스크를 바로 스케줄링할 수 없기에 큐에 넣어야 한다. 스케줄러는 물리적으로 리소스가 부족하지 않도록 동시에 실행 중인 태스크를 제한한다.

스케줄러는 의존성도 해결한다. 각 워커가 로컬 오브젝트 스토어에서 태스크를 실행하는 데 필요한 모든 오브젝트를 가지고 있는지 확인해야 한다. 로컬 오브젝트 스토어에서 값을 찾아서 로컬 의존성을 해결하고, 없다면 다른 노드와 통신해 원격 의존성을 가져온다.

워커는 자신이 호출하는 모든 태스크에 대한 메타데이터와 해당 태스크에서 반환되는 오브젝트 레퍼런스를 저장한다. 소유권이라는 개념은 오브젝트 레퍼런스를 생성하는 프로세스가 오브젝트 레퍼런스의 처리도 담당한다는 의미이다. 예를 들어 워커 프로세스는 소유권 테이블을 가지고 있어, 장애가 발생하면 자신이 소유한 오브젝트 레퍼런스를 추적한다. 

[##_Image|kage@cmHZ8b/btsJTBCzCn3/RTfvLEJ7nkyFe3Q8XB1Z8K/img.png|CDM|1.3|{"originWidth":958,"originHeight":642,"style":"alignCenter"}_##]

### **헤드 노드**

헤드 노드에는 드라이버 프로세스가 있다. 드라이버 프로세스는 스스로 태스크를 제출해도 실행은 할 수 없다. 또한 헤드 노드에는 워커 프로세스가 있고, 이는 단일 노드로 구성된 로컬 클러스터를 실행하는 데 중요하다.

헤드 노드는 다른 워커 노드 기능에 클러스터 관리를 담당하는 프로세스를 추가로 실행한다. 이것은 클러스터에 대한 전역 정보를 전달하는 중요한 컴포넌트이다. GCS는 시스템 레벨 메타데이터 같은 정보를 저장하는 키-값 저장소이다. 

### **분산된 스케줄링과 실행**

클러스터 오케스트레이션과 노드가 태스크를 관리하고 계획하며 실행하는 방법을 간략하게 알아보자. 워커 노드에 대해서 설명하면서, 레이를 사용해 워크로드를 분산할 때 몇 가지 컴포넌트가 있다고 했다. 프로세스와 관련된 단계와 복잡성을 정리해보자.

#### **분산된 메모리**

레이렛의 오브젝트 스토어는 노드에서 메모리를 관리한다. 때로는 노드 사이에 오브젝트를 전송해야 하는 경우가 있는데, 이를 분산 오브젝트 전송이라고 부른다. 이는 워커가 태스크를 실행하는 데 필요한 오브젝트를 갖도록 하는 원격 종속성 해결을 할 때 사용한다.

#### **통신**

레이 클러스터에서 일어나는 오브젝트 전송 같은 대부분의 통신은 gRPC를 사용한다.

#### **리소스 관리 및 수행**

노드에서 레이렛은 리소스를 할당하고 태스크 소유자에게 워커 프로세스를 임대하는 역할을 맡는다. 노드 전체의 모든 스케줄러가 분산 스케줄러를 형성하고 이것은 효과적으로 노드가 다른 노드에 태스크를 스케줄링한다. 로컬 스케줄러는 GCS와의 통신을 통해 다른 노드의 리소스 상황을 파악한다.

#### **태스크 실행**

실행을 위해서 태스크가 제출되면 모든 의존성을 해결해야 한다.

## **레이를 사용한 간단한 맵 리듀스**

단순하게 맵리듀스 구현을 여러 문서에 걸쳐서 특정 단어가 나온 횟수를 세는 사용 사례로 살펴보자.

1\. 매핑 단계 : 문서 집합을 가져와서 기능에 따라 해당 요소(예를 들어, 문서에 포함된 단어)들을 변환하거나 매핑한다. 이 단계에서는 설계 상 키-값 쌍을 생성한다. 키는 문서의 요소를 나타내고, 값은 단순히 해당 요소에 대해서 계산하려는 지표이다. 단어를 세는 작업을 하기 때문에 문서에서 단어를 만날 때마다 map 함수는 나타내는 단어를 (단어, 1) 형태의 한 쌍을 내보내서 해당 단어가 한 번 발견되었음을 나타낸다.

2\. 셔플 단계 : 키에 따라서 매핑 단계의 모든 출력을 수집한 뒤 그룹화한다. 분산 설정에서 작업하면 동일 키가 여러 컴퓨팅 노드에서 있을 수 있기에 노드 간에 데이터를 섞어야 한다. 구체적인 사용 사례에서 그룹화가 무엇을 의미하는지 알기 위해서 매핑 단계에서 총 4개의 항목이 생성되었다고 가정해보자. 그런 다음 셔플은 같은 단어가 같은 노드에 있는 모든 항목을 같이 함께 배치한다.

3\. 리듀스 단계 : 셔플 단계에서 요소를 집계하거나 리듀스한다. 앞서 설명한 예를 이으면, 최종 개수를 알기 위해서 각 노드에서 발생하는 모든 단어를 단순하게 요약한다. (단어, 1) 4개를 (단어, 4)로 요약한다.

```
import ray

ray.init()

import subprocess

zen_of_python = subprocess.check_output(["python", "-c", "import this"])
corpus = zen_of_python.split()  # 단어 모음
num_partitions = 3

num_partition = len(corpus) // num_partitions

partitions = [corpus[i*num_partition : (i+1)*num_partition] for i in range(num_partitions)]
```

[##_Image|kage@yKOWP/btsJVarGz6D/K3xV65RYXdSlBS7XgxGZo0/img.png|CDM|1.3|{"originWidth":1280,"originHeight":581,"style":"alignCenter"}_##]

#### **매핑과 셔플**

매핑 단계를 정의하기 위해서는 각 문서에 적용되는 맵 함수가 필요하다. 이번에는 문서에서 찾은 각 단어에 대한 쌍 (단어, 1)을 방출하려고 한다. 파이썬 문자열로 로드된 간단한 텍스트 문서의 경우 맵 함수는 다음과 같다.

```
def map_function(document):
    yield document.lower(), 1
```

단어를 소문자로 바꾸고, (단어, 1) 형태로 반환한다.

```
@ray.remote
def apply_map(corpus, num_partitions):
    map_results = [list() for _ in range(num_partitions)]
    for document in corpus:
        for result in map_function(document):
            first_letter = result[0].decode("utf-8")[0]
            word_index = ord(first_letter) % num_partitions
            map_results[word_index].append(result)
    return map_results
```

corpus에 있는 단어들을 map\_function에 적용하고, 단어를 ord를 통해 숫자로 바꾸고, 나머지를 구해 파티션 번호로 사용한다. 그 파티션 번호에 (단어, 1)을 삽입한다. 모두 삽입했으면, 파티션들을 반환한다.

```
map_results = [
    apply_map.remote(data, num_partitions)
    for data in partitions
]
```

말뭉치를 나누어 놓은 data를 워커에 할당해 분류한다.

```
for i in range(num_partitions):
    mapper_results = ray.get(map_results[i])
    for j, result in enumerate(mapper_results):
        print(f"Mapper {i}, return value {j}: {result[:2]}")
        
"""
Mapper 0, return value 0: [(b'of', 1), (b'is', 1)]
Mapper 0, return value 1: [(b'python,', 1), (b'peters', 1)]
Mapper 0, return value 2: [(b'the', 1), (b'zen', 1)]
Mapper 1, return value 0: [(b'unless', 1), (b'in', 1)]
Mapper 1, return value 1: [(b'although', 1), (b'practicality', 1)]
Mapper 1, return value 2: [(b'beats', 1), (b'errors', 1)]
Mapper 2, return value 0: [(b'is', 1), (b'is', 1)]
Mapper 2, return value 1: [(b'although', 1), (b'a', 1)]
Mapper 2, return value 2: [(b'better', 1), (b'than', 1)]
"""
```

워커 i가 분류한 결과 중, j번째 파티션은 ~라는 의미이다. 같은 단어가 같은 파티션에 있는 것으로 보아 셔플까지 이루어진 것을 알 수 있다.

#### **리듀스**

```
@ray.remote
def apply_reduce(results):
    reduce_results = dict()
    for res in results:
        for key, value in res:
            if key not in reduce_results:
                reduce_results[key] = 0
            reduce_results[key] += value
    return reduce_results
```

단어의 개수를 하나씩 증가시킨다.

```
outputs = []
for i in range(num_partitions):
    outputs.append(
        apply_reduce.remote([partition[i] for partition in ray.get(map_results)])
    )
counts = {k: v for output in ray.get(outputs) for k, v in output.items()}

sorted_counts = sorted(counts.items(), key=lambda item: item[1], reverse=True)
for count in sorted_counts:
    print(f"{count[0].decode('utf-8')}: {count[1]}")
```

map\_results는 워커가 셔플까지 해놓은 파티션들을 저장하고 있는 참조 객체의 모임이다.  ray.get을 해서 각 워커가 셔플까지 해놓은 파티션들로 바꾼 후, i번째 파티션들을 모두 모아 apply\_reduce를 수행하여 (단어:개수)의 딕셔너리를 가리키는 참조 객체를 outputs에 넣는다.