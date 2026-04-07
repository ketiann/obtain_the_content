#!/usr/bin/env python3
"""
主入口：根据配置抓取网页正文并写回数据表
支持 requests + Playwright 双引擎抓取
"""

import logging
import sys
import yaml

from data_handler import DataHandler
from fetcher import ContentFetcher


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def load_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("=== 开始任务 ===")

    config = load_config()
    logger.info("配置加载成功")

    data_handler = DataHandler(config)
    fetcher = ContentFetcher(config)

    try:
        pending_df = data_handler.load_pending_records()
        if pending_df.empty:
            logger.info("没有需要处理的记录（flag=0 的记录为空）")
            return

        logger.info(f"待处理记录数: {len(pending_df)}")

        full_df = data_handler.load_full_dataframe() if config["output"]["type"] == "excel" else None

        updates = []
        for i, (idx, row) in enumerate(pending_df.iterrows()):
            record_id = row["id"]
            url = row["url"]

            logger.info(f"[{i+1}/{len(pending_df)}] 处理 ID={record_id}, URL={url}")

            content = fetcher.fetch_content(url)
            if content:
                updates.append({"id": record_id, "content_all": content, "flag": 1})
                logger.info(f"  -> 成功抓取，内容长度: {len(content)} 字符")
            else:
                logger.warning(f"  -> 抓取失败，跳过")

        if updates:
            data_handler.save_results(full_df, updates)
            logger.info(f"任务完成，成功更新 {len(updates)} 条记录")
        else:
            logger.warning("没有成功抓取任何内容")

    finally:
        fetcher.close()

    logger.info("=== 任务结束 ===")


if __name__ == "__main__":
    main()
