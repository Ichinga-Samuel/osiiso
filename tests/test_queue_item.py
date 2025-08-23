import pytest
import asyncio

from async_queue import QueueItem


def task_one(a, b, c=3):
    return a + b + c


async def task_two(e, f=6):
    return e + f


@pytest.mark.asyncio
async def test_queue_item():
    queue_item = QueueItem(task_one, 1, 2, c=4)
    queue_item2 = QueueItem(task_two, 5, f=7)
    assert queue_item.task == task_one
    assert queue_item.args == (1, 2)
    assert queue_item.kwargs == {'c': 4}
    assert queue_item2.task == task_two
    assert queue_item2.args == (5,)
    assert queue_item2.kwargs == {'f': 7}
    assert queue_item < queue_item2
    queue = asyncio.PriorityQueue()
    queue.put_nowait((0, queue_item2))
    queue.put_nowait((0, queue_item))
    _, item = queue.get_nowait()
    _, item2 = queue.get_nowait()
    assert item == queue_item
    assert await item() == 7
    assert await item2() == 12
