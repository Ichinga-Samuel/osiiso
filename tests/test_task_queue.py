import random
import asyncio

import pytest

from async_queue import TaskQueue, QueueItem


async def write_row(data):
    task_id = random.randint(999, 999_999_999)
    task_name = f"task-{task_id}"
    task_duration = random.randint(1, 10)
    row = {'task_id': task_id, 'task_name': task_name, 'task_duration': task_duration}
    await asyncio.sleep(task_duration)
    data.append(row)


@pytest.mark.asyncio
async def test_task_queue(data):
    tq = TaskQueue()
    assert tq.mode == 'finite'
    assert tq.workers == 10
    assert tq.queue_timeout is 0
    assert tq.absolute_timeout is 0
    assert tq.stop is False
    await tq.add_workers(no_of_workers=10)
    assert len(tq.worker_tasks) == 10
    for _ in range(100):
        task = write_row
        item = QueueItem(task, data)
        tq.add(item=item)
    await tq.run()
    assert len(data) == 100
