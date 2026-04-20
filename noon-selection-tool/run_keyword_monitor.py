"""
Keyword monitor runner.

用途：
- 提供一个稳定的关键词轮巡入口，便于接 Windows 计划任务。
- 默认使用 keyword runtime，不影响类目爬虫运行目录。
"""

from __future__ import annotations

from keyword_main import build_parser, keyword_core, logger, prepare_settings, run_monitor


def main() -> None:
    parser = build_parser()
    parser.description = "Noon 关键词轮巡任务入口"
    args = parser.parse_args()
    args.step = "monitor"
    args.runtime_scope = "keyword"

    keyword_core.setup_logging(args.verbose)
    settings = prepare_settings(args)
    logger.info("Keyword Monitor Runner")
    logger.info("Data Root: %s", settings.data_dir)
    logger.info("Product DB: %s", settings.product_store_db_path)
    run_monitor(settings, args)


if __name__ == "__main__":
    main()
