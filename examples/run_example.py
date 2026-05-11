from hackernews_showcase import print_report, run_pipeline

import osiiso


async def run_example():
    res = await run_pipeline(100, database="hacker_news.sqlite3", offline=False)
    print_report(res)


if __name__ == "__main__":
    osiiso.run(run_example())
