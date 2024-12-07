import pytest
import asyncio

from async_queue import QueueItem


def task_one(a, b, c=3):
    print("Task one")
    print(a, b, c)
    print("Task one done")
    return a + b + c


async def task_two(e, f=6):
    print("Task two")
    print("Task two done")
    return e + f


@pytest.mark.asyncio
async def test_queue_item():
    queue_item = QueueItem(task_one, 1, 2, c=4)
    await asyncio.sleep(0.1)
    queue_item2 = QueueItem(task_two, 5, f=7)
    assert queue_item.task_item == task_one
    assert queue_item.args == (1, 2)
    assert queue_item.kwargs == {'c': 4}
    assert queue_item2.task_item == task_two
    assert queue_item2.args == (5,)
    assert queue_item2.kwargs == {'f': 7}
    assert queue_item.must_complete is False
    assert queue_item2.must_complete is False
    assert queue_item < queue_item2
    queue = asyncio.PriorityQueue()
    queue.put_nowait((0, queue_item2))
    queue.put_nowait((0, queue_item))
    _, item = queue.get_nowait()
    assert item == queue_item
    assert await item.run() == 7
