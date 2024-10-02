import ray

ray.init()

items = [{"name":str(i), "data":i} for i in range(10000)]
ds = ray.data.from_items(items)
ds.show(5)

# squares = ds.map(lambda x: x["data"] ** 2)
#
# evens = squares.filter(lambda x: x % 2 == 0)
# evens.count()
#
# cubes = evens.flat_map(lambda x: [x, x**3])
# sample = cubes.take(10)
# print(sample)

