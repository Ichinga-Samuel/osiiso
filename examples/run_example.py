from hackernews_showcase import print_report, run_pipeline

import osiiso
from osiiso import Checkpoint


async def run_example():
    # 100 live items is exactly the kind of run worth checkpointing: kill it
    # halfway, start it again, and only the missing items are re-fetched.
    with Checkpoint("hacker_news_checkpoint.sqlite3") as cp:
        res = await run_pipeline(100, database="hacker_news.sqlite3", offline=False, checkpoint=cp)
        print_report(res)


if __name__ == "__main__":
    osiiso.run(run_example())
