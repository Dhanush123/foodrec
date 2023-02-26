from prefect import flow, task, unmapped
import asyncio
import random
from prefect import task, flow


def thing():
    return [x**2 for x in range(10)]


@task
async def print_values(value):
    await thing()
    return value


@flow
async def async_flow():
    # await print_values([1, 2])  # runs immediately
    # t0 = asyncio.create_task(print_values(0))
    # t1 = asyncio.create_task(print_values(1))
    coros = [asyncio.create_task(print_values(i)) for i in range(4)]

    # asynchronously gather the tasks
    # results = await asyncio.gather(*coros)
    for res in asyncio.as_completed(coros):
        print(await res)


# @task
# def sum_plus(x):
#     print("!!!", type(x), x)
#     return x


# @flow
# def sum_it(numbers):
#     futures = sum_plus.map(numbers)
#     return futures


# x = sum_it([4, 5, 6])
# print(type(x), x)
# print([y.result() for y in x])


# @task
# def create_list():
#     return [1, 1, 2, 3]


# @task
# def add_one(x):
#     return x + 1


# @task
# def get_sum(x):
#     return sum(x)


# @flow
# def some_flow():
#     plus_one = add_one.map(create_list)
#     # plus_two = add_one.map(plus_one)


if __name__ == "__main__":
    # some_flow()
    asyncio.run(async_flow())
