"""
数据读写模块 —— 支持 Excel、CSV 和 MySQL 三种数据源/输出
通过 config.yaml 中的 input.type / output.type 自动切换
"""

import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)


class DataHandler:
    """统一数据读写接口"""

    def __init__(self, config: dict):
        self.config = config

    # ======================== 读取（输入） ========================

    def load_pending_records(self) -> pd.DataFrame:
        input_cfg = self.config["input"]
        src_type = input_cfg["type"]
        if src_type == "excel":
            return self._load_from_excel(input_cfg["excel"])
        elif src_type == "csv":
            return self._load_from_csv(input_cfg["csv"])
        elif src_type == "mysql":
            return self._load_from_mysql(input_cfg["mysql"])
        else:
            raise ValueError(f"不支持的输入类型: {src_type}")

    def _load_from_excel(self, cfg: dict) -> pd.DataFrame:
        file_path = cfg["file_path"]
        sheet = cfg.get("sheet_name", 0)
        flag_val = cfg.get("flag_filter_value", 0)
        fields = cfg.get("fields", None)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"输入文件不存在: {file_path}")
        df = pd.read_excel(file_path, sheet_name=sheet)
        logger.info(f"从 Excel 读取到 {len(df)} 条记录")
        result = df[df["flag"] == flag_val].copy()
        if fields:
            result = result[[c for c in fields if c in result.columns]]
        logger.info(f"其中 flag={flag_val} 的记录有 {len(result)} 条")
        return result

    def _load_from_csv(self, cfg: dict) -> pd.DataFrame:
        file_path = cfg["file_path"]
        flag_val = cfg.get("flag_filter_value", 0)
        fields = cfg.get("fields", None)
        encoding = cfg.get("encoding", "utf-8")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"输入文件不存在: {file_path}")
        df = pd.read_csv(file_path, encoding=encoding)
        logger.info(f"从 CSV 读取到 {len(df)} 条记录")
        result = df[df["flag"] == flag_val].copy()
        if fields:
            result = result[[c for c in fields if c in result.columns]]
        logger.info(f"其中 flag={flag_val} 的记录有 {len(result)} 条")
        return result

    def _load_from_mysql(self, cfg: dict) -> pd.DataFrame:
        try:
            import pymysql
        except ImportError:
            raise ImportError("使用 MySQL 需要安装 pymysql: pip install pymysql")
        conn = pymysql.connect(
            host=cfg["host"], port=cfg["port"], user=cfg["user"],
            password=cfg["password"], database=cfg["database"], charset="utf8mb4",
        )
        query = cfg.get("query", "SELECT id, url FROM articles WHERE flag = 0")
        df = pd.read_sql(query, conn)
        conn.close()
        logger.info(f"从 MySQL 读取到 {len(df)} 条待处理记录")
        return df

    # ======================== 加载完整数据 ========================

    def load_full_dataframe(self) -> pd.DataFrame:
        input_cfg = self.config["input"]
        src_type = input_cfg["type"]
        if src_type == "excel":
            cfg = input_cfg["excel"]
            return pd.read_excel(cfg["file_path"], sheet_name=cfg.get("sheet_name", 0))
        elif src_type == "csv":
            cfg = input_cfg["csv"]
            return pd.read_csv(cfg["file_path"], encoding=cfg.get("encoding", "utf-8"))
        else:
            raise NotImplementedError("MySQL 模式下请直接执行 UPDATE SQL")

    # ======================== 写入（输出） ========================

    def save_results(self, full_df: pd.DataFrame, updates: list[dict]):
        output_cfg = self.config["output"]
        out_type = output_cfg["type"]
        if out_type == "excel":
            self._save_to_excel(full_df, updates, output_cfg["excel"])
        elif out_type == "csv":
            self._save_to_csv(full_df, updates, output_cfg["csv"])
        elif out_type == "mysql":
            self._save_to_mysql(updates, output_cfg["mysql"])
        else:
            raise ValueError(f"不支持的输出类型: {out_type}")

    def _apply_updates(self, full_df, updates):
        if "content_all" in full_df.columns:
            full_df["content_all"] = full_df["content_all"].astype(object)
        update_map = {u["id"]: u for u in updates}
        for idx, row in full_df.iterrows():
            if row["id"] in update_map:
                u = update_map[row["id"]]
                full_df.at[idx, "content_all"] = u["content_all"]
                full_df.at[idx, "flag"] = u["flag"]

    def _save_to_excel(self, full_df, updates, cfg):
        self._apply_updates(full_df, updates)
        out_path = cfg["file_path"]
        full_df.to_excel(out_path, index=False)
        logger.info(f"已保存结果到 {out_path}，共更新 {len(updates)} 条记录")

    def _save_to_csv(self, full_df, updates, cfg):
        self._apply_updates(full_df, updates)
        out_path = cfg["file_path"]
        encoding = cfg.get("encoding", "utf-8")
        full_df.to_csv(out_path, index=False, encoding=encoding)
        logger.info(f"已保存结果到 {out_path}，共更新 {len(updates)} 条记录")

    def _save_to_mysql(self, updates, cfg):
        try:
            import pymysql
        except ImportError:
            raise ImportError("使用 MySQL 需要安装 pymysql: pip install pymysql")
        conn = pymysql.connect(
            host=cfg["host"], port=cfg["port"], user=cfg["user"],
            password=cfg["password"], database=cfg["database"], charset="utf8mb4",
        )
        update_sql = cfg.get(
            "update_sql",
            "UPDATE articles SET content_all = %(content_all)s, flag = %(flag)s WHERE id = %(id)s",
        )
        cursor = conn.cursor()
        for u in updates:
            cursor.execute(update_sql, u)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"已更新 MySQL {len(updates)} 条记录")
