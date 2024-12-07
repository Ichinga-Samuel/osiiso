import asyncio
import random
from src.taskqueue import TaskQueue, QueueItem

data = {}

async def task_one():
    print("Task one")
    sleep = random.randint(5, 10)
    rand = random.randint(999, 999_999_999)
    # await asyncio.sleep(sleep)
    data[rand] = sleep
    print("Task one done")


async def runner():
    tq = TaskQueue(mode='infinite', workers=50, timeout=10)
    for _ in range(10):
        tq.add(item=QueueItem(task_one))
    await asyncio.sleep(1)

    for _ in range(150):
        tq.add(item=QueueItem(task_one))

    await tq.run()


asyncio.run(runner())

print(len(data))
