import argparse
import os
import time
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy_playwright_test.spiders.quality_spider import QualitySpider


def load_urls(path="url_quality_list.txt"):
    with open(path, encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip()]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrapy框架网页抓取质量评估测试")
    parser.add_argument(
        "--mode", required=True,
        choices=["http", "chrome", "chrome-no-js", "chrome-no-media"],
        help="抓取模式"
    )
    parser.add_argument(
        "--concurrency", type=int, default=16,
        help="并发请求数，应用于 Scrapy 设置"
    )
    parser.add_argument(
        "--output_dir", default="results",
        help="质量评估结果输出目录"
    )
    args = parser.parse_args()

    # 加载 URL 列表
    urls = load_urls("url_quality_list.txt")
    print(f"Loaded {len(urls)} URLs → mode={args.mode}, concurrency={args.concurrency}")

    # 设置 Scrapy
    settings = get_project_settings()
    settings.set("CONCURRENT_REQUESTS", args.concurrency)
    os.makedirs(args.output_dir, exist_ok=True)

    # 启动爬虫
    process = CrawlerProcess(settings=settings)
    start_time = time.time()
    process.crawl(
        QualitySpider,
        urls=urls,
        mode=args.mode,
        output_dir=args.output_dir
    )
    process.start()

    duration = time.time() - start_time
    print(f"Quality evaluation completed in {duration:.1f}s; results saved to {args.output_dir}/quality_metrics.jsonl")

