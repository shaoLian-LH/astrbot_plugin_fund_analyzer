import os
import re
import sqlite3
import time
from datetime import date, datetime
from typing import Any

DEFAULT_DB_PATH = "data/plugins/astrbot_plugin_fund_analyzer_advance/fund.db"
DEFAULT_DATA_PATH = DEFAULT_DB_PATH


class DataHandler:
    """基金数据持久化处理器（基金主数据、用户持仓、历史净值）。"""

    SCHEMA_VERSION = "5"
    LEGACY_NAV_TABLE = "fund_nav_history"
    NAV_PARTITION_SUFFIX = "fund_nav_history"
    NAV_PARTITION_TABLE_PATTERN = re.compile(r"^\d{4}_\d{2}_fund_nav_history$")
    NAV_PARTITION_TABLE_GLOB = "[0-9][0-9][0-9][0-9]_[0-9][0-9]_fund_nav_history"

    def __init__(self, path: str = DEFAULT_DB_PATH):
        self.path = path
        self._nav_partition_table_cache: list[str] | None = None
        self._ensure_parent_dir()
        self._init_db()

    def _ensure_parent_dir(self) -> None:
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS funds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fund_code TEXT NOT NULL UNIQUE,
                    fund_name TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_funds_code ON funds(fund_code);

                CREATE TABLE IF NOT EXISTS user_fund_positions (
                    platform TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    fund_id INTEGER NOT NULL,
                    avg_cost REAL NOT NULL,
                    shares REAL NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (platform, user_id, fund_id),
                    FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_user_fund_positions_user
                ON user_fund_positions(platform, user_id);

                CREATE TABLE IF NOT EXISTS user_fund_position_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    fund_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    shares_delta REAL NOT NULL,
                    shares_before REAL NOT NULL,
                    shares_after REAL NOT NULL,
                    avg_cost REAL NOT NULL,
                    settlement_nav REAL,
                    settlement_nav_date TEXT,
                    expected_settlement_date TEXT,
                    settlement_rule TEXT NOT NULL DEFAULT '',
                    profit_amount REAL,
                    note TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL,
                    FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_user_fund_position_logs_user
                ON user_fund_position_logs(platform, user_id, created_at DESC);

                CREATE INDEX IF NOT EXISTS idx_user_fund_position_logs_action
                ON user_fund_position_logs(action, created_at DESC);

                CREATE TABLE IF NOT EXISTS fund_nav_history (
                    fund_id INTEGER NOT NULL,
                    nav_date TEXT NOT NULL,
                    unit_nav REAL NOT NULL,
                    accum_nav REAL,
                    change_rate REAL,
                    source TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (fund_id, nav_date),
                    FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_fund_nav_history_fund_date
                ON fund_nav_history(fund_id, nav_date DESC);

                CREATE TABLE IF NOT EXISTS exchange_rate_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    currency_pair TEXT NOT NULL,
                    rate_date TEXT NOT NULL,
                    rate REAL NOT NULL,
                    source TEXT NOT NULL DEFAULT '',
                    source_text TEXT NOT NULL DEFAULT '',
                    update_time TEXT NOT NULL DEFAULT '',
                    query_time TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_exchange_rate_history_pair_date
                ON exchange_rate_history(currency_pair, rate_date DESC, created_at DESC);

                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
            stored_version = self._get_schema_meta_value_tx(
                conn=conn,
                key="schema_version",
                default="0",
            )
            if stored_version != self.SCHEMA_VERSION:
                self._set_schema_meta_value_tx(
                    conn=conn,
                    key="schema_version",
                    value=self.SCHEMA_VERSION,
                )

    @staticmethod
    def _normalize_key(value: Any, fallback: str = "") -> str:
        text = str(value).strip() if value is not None else ""
        return text or fallback

    @staticmethod
    def _normalize_fund_code(fund_code: Any) -> str:
        code = str(fund_code).strip() if fund_code is not None else ""
        if code.isdigit():
            return code.zfill(6)
        return code

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _as_positive_float(value: Any, field_name: str) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError) as e:
            raise ValueError(f"{field_name} 必须是数字") from e
        if number <= 0:
            raise ValueError(f"{field_name} 必须大于 0")
        return number

    @staticmethod
    def _normalize_nav_date(nav_date: Any) -> str:
        if isinstance(nav_date, datetime):
            return nav_date.strftime("%Y-%m-%d")
        if isinstance(nav_date, date):
            return nav_date.isoformat()

        text = str(nav_date or "").strip()
        if not text:
            raise ValueError("净值日期不能为空")

        text = text[:10]
        try:
            datetime.strptime(text, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"净值日期格式错误: {nav_date}") from e
        return text

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        text = str(identifier or "")
        return '"' + text.replace('"', '""') + '"'

    @classmethod
    def _is_nav_partition_table_name(cls, table_name: str) -> bool:
        return bool(cls.NAV_PARTITION_TABLE_PATTERN.match(str(table_name or "").strip()))

    @classmethod
    def _build_nav_partition_table_name(cls, nav_date_text: str) -> str:
        nav_day = datetime.strptime(nav_date_text, "%Y-%m-%d").date()
        return f"{nav_day.year:04d}_{nav_day.month:02d}_{cls.NAV_PARTITION_SUFFIX}"

    @classmethod
    def _extract_month_key_from_table_name(cls, table_name: str) -> int | None:
        if not cls._is_nav_partition_table_name(table_name):
            return None
        year_text = table_name[0:4]
        month_text = table_name[5:7]
        try:
            year_num = int(year_text)
            month_num = int(month_text)
        except ValueError:
            return None
        if month_num < 1 or month_num > 12:
            return None
        return year_num * 100 + month_num

    @staticmethod
    def _extract_month_key_from_date_text(nav_date_text: str) -> int:
        nav_day = datetime.strptime(nav_date_text, "%Y-%m-%d").date()
        return nav_day.year * 100 + nav_day.month

    def _set_schema_meta_value_tx(
        self,
        conn: sqlite3.Connection,
        key: str,
        value: str,
    ) -> None:
        conn.execute(
            "INSERT OR REPLACE INTO schema_meta(key, value) VALUES(?, ?)",
            (str(key), str(value)),
        )

    def _get_schema_meta_value_tx(
        self,
        conn: sqlite3.Connection,
        key: str,
        default: str = "",
    ) -> str:
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = ?",
            (str(key),),
        ).fetchone()
        if row is None:
            return default
        return str(row["value"] or default)

    def _list_nav_partition_tables_tx(
        self,
        conn: sqlite3.Connection,
        refresh: bool = False,
    ) -> list[str]:
        if self._nav_partition_table_cache is not None and not refresh:
            return list(self._nav_partition_table_cache)

        rows = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name GLOB ?
            ORDER BY name DESC
            """,
            (self.NAV_PARTITION_TABLE_GLOB,),
        ).fetchall()
        tables = [
            str(row["name"])
            for row in rows
            if self._is_nav_partition_table_name(str(row["name"]))
        ]
        self._nav_partition_table_cache = list(tables)
        return list(tables)

    def _ensure_nav_partition_table_tx(
        self,
        conn: sqlite3.Connection,
        table_name: str,
    ) -> None:
        if not self._is_nav_partition_table_name(table_name):
            raise ValueError(f"净值分表名称非法: {table_name}")

        quoted_table = self._quote_identifier(table_name)
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {quoted_table} (
                fund_id INTEGER NOT NULL,
                nav_date TEXT NOT NULL,
                unit_nav REAL NOT NULL,
                accum_nav REAL,
                change_rate REAL,
                source TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (fund_id, nav_date),
                FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE
            )
            """
        )

        index_name = f"idx_{table_name}_fund_date"
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {self._quote_identifier(index_name)}
            ON {quoted_table}(fund_id, nav_date DESC)
            """
        )
        self._nav_partition_table_cache = None

    def _resolve_nav_tables_tx(
        self,
        conn: sqlite3.Connection,
        start_date: str | None = None,
        end_date: str | None = None,
        include_legacy: bool = True,
        order_desc: bool = True,
    ) -> list[str]:
        tables = self._list_nav_partition_tables_tx(conn=conn)

        start_key = (
            self._extract_month_key_from_date_text(start_date)
            if start_date
            else None
        )
        end_key = (
            self._extract_month_key_from_date_text(end_date)
            if end_date
            else None
        )
        if start_key is not None or end_key is not None:
            filtered: list[str] = []
            for table_name in tables:
                table_key = self._extract_month_key_from_table_name(table_name)
                if table_key is None:
                    continue
                if start_key is not None and table_key < start_key:
                    continue
                if end_key is not None and table_key > end_key:
                    continue
                filtered.append(table_name)
            tables = filtered

        tables = sorted(tables, reverse=order_desc)
        if include_legacy:
            tables.append(self.LEGACY_NAV_TABLE)
        return tables

    def _row_to_fund(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "fund_code": str(row["fund_code"]),
            "fund_name": str(row["fund_name"] or ""),
            "created_at": int(row["created_at"]),
            "updated_at": int(row["updated_at"]),
        }

    def _row_to_position(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "platform": str(row["platform"]),
            "user_id": str(row["user_id"]),
            "fund_id": int(row["fund_id"]),
            "fund_code": str(row["fund_code"]),
            "fund_name": str(row["fund_name"] or ""),
            "avg_cost": float(row["avg_cost"]),
            "shares": float(row["shares"]),
            "created_at": int(row["created_at"]),
            "updated_at": int(row["updated_at"]),
        }

    def _row_to_nav(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "fund_id": int(row["fund_id"]),
            "fund_code": str(row["fund_code"]),
            "fund_name": str(row["fund_name"] or ""),
            "nav_date": str(row["nav_date"]),
            "unit_nav": float(row["unit_nav"]),
            "accum_nav": (None if row["accum_nav"] is None else float(row["accum_nav"])),
            "change_rate": (None if row["change_rate"] is None else float(row["change_rate"])),
            "source": str(row["source"] or ""),
            "created_at": int(row["created_at"]),
            "updated_at": int(row["updated_at"]),
        }

    def _row_to_position_log(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "platform": str(row["platform"]),
            "user_id": str(row["user_id"]),
            "fund_id": int(row["fund_id"]),
            "fund_code": str(row["fund_code"]),
            "fund_name": str(row["fund_name"] or ""),
            "action": str(row["action"]),
            "shares_delta": float(row["shares_delta"]),
            "shares_before": float(row["shares_before"]),
            "shares_after": float(row["shares_after"]),
            "avg_cost": float(row["avg_cost"]),
            "settlement_nav": (
                None if row["settlement_nav"] is None else float(row["settlement_nav"])
            ),
            "settlement_nav_date": str(row["settlement_nav_date"] or ""),
            "expected_settlement_date": str(row["expected_settlement_date"] or ""),
            "settlement_rule": str(row["settlement_rule"] or ""),
            "profit_amount": (
                None if row["profit_amount"] is None else float(row["profit_amount"])
            ),
            "note": str(row["note"] or ""),
            "created_at": int(row["created_at"]),
        }

    def _row_to_exchange_rate(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "currency_pair": str(row["currency_pair"] or ""),
            "rate_date": str(row["rate_date"]),
            "rate": float(row["rate"]),
            "source": str(row["source"] or ""),
            "source_text": str(row["source_text"] or ""),
            "update_time": str(row["update_time"] or ""),
            "query_time": str(row["query_time"] or ""),
            "created_at": int(row["created_at"]),
        }

    def add_exchange_rate_record(
        self,
        currency_pair: Any,
        rate: Any,
        rate_date: Any = None,
        source: Any = "",
        source_text: Any = "",
        update_time: Any = "",
        query_time: Any = "",
    ) -> dict[str, Any]:
        pair = self._normalize_key(currency_pair, fallback="USD/CNY").upper()
        if not pair:
            raise ValueError("货币对不能为空")

        rate_num = self._as_positive_float(rate, "汇率")
        date_text = self._normalize_nav_date(rate_date or date.today().isoformat())
        now_ts = int(time.time())
        source_value = str(source or "").strip()
        source_text_value = str(source_text or "").strip()
        update_time_value = str(update_time or "").strip()
        query_time_value = str(query_time or "").strip()

        with self._connect() as conn:
            conn.execute("BEGIN")
            cursor = conn.execute(
                """
                INSERT INTO exchange_rate_history (
                    currency_pair,
                    rate_date,
                    rate,
                    source,
                    source_text,
                    update_time,
                    query_time,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pair,
                    date_text,
                    rate_num,
                    source_value,
                    source_text_value,
                    update_time_value,
                    query_time_value,
                    now_ts,
                ),
            )
            row = conn.execute(
                """
                SELECT
                    id,
                    currency_pair,
                    rate_date,
                    rate,
                    source,
                    source_text,
                    update_time,
                    query_time,
                    created_at
                FROM exchange_rate_history
                WHERE id = ?
                """,
                (int(cursor.lastrowid or 0),),
            ).fetchone()
            if row is None:
                raise RuntimeError("汇率记录保存失败")
            return self._row_to_exchange_rate(row)

    def get_exchange_rate_on_date(
        self,
        currency_pair: Any = "USD/CNY",
        rate_date: Any = None,
    ) -> dict[str, Any] | None:
        pair = self._normalize_key(currency_pair, fallback="USD/CNY").upper()
        date_text = self._normalize_nav_date(rate_date or date.today().isoformat())

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    id,
                    currency_pair,
                    rate_date,
                    rate,
                    source,
                    source_text,
                    update_time,
                    query_time,
                    created_at
                FROM exchange_rate_history
                WHERE currency_pair = ? AND rate_date = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (pair, date_text),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_exchange_rate(row)

    def get_latest_exchange_rate(
        self,
        currency_pair: Any = "USD/CNY",
    ) -> dict[str, Any] | None:
        pair = self._normalize_key(currency_pair, fallback="USD/CNY").upper()
        if not pair:
            return None

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    id,
                    currency_pair,
                    rate_date,
                    rate,
                    source,
                    source_text,
                    update_time,
                    query_time,
                    created_at
                FROM exchange_rate_history
                WHERE currency_pair = ?
                ORDER BY rate_date DESC, created_at DESC, id DESC
                LIMIT 1
                """,
                (pair,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_exchange_rate(row)

    def list_exchange_rate_history(
        self,
        currency_pair: Any = "USD/CNY",
        start_date: Any = None,
        end_date: Any = None,
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        pair = self._normalize_key(currency_pair, fallback="USD/CNY").upper()
        if not pair:
            return []

        date_from = self._normalize_nav_date(start_date) if start_date else None
        date_to = self._normalize_nav_date(end_date) if end_date else None
        query_limit = max(1, min(int(limit or 30), 1000))

        sql = """
            SELECT
                id,
                currency_pair,
                rate_date,
                rate,
                source,
                source_text,
                update_time,
                query_time,
                created_at
            FROM exchange_rate_history
            WHERE currency_pair = ?
        """
        params: list[Any] = [pair]
        if date_from:
            sql += " AND rate_date >= ?"
            params.append(date_from)
        if date_to:
            sql += " AND rate_date <= ?"
            params.append(date_to)
        sql += " ORDER BY rate_date DESC, created_at DESC, id DESC LIMIT ?"
        params.append(query_limit)

        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [self._row_to_exchange_rate(row) for row in rows]

    def _get_fund_by_code_tx(
        self, conn: sqlite3.Connection, fund_code: str
    ) -> sqlite3.Row | None:
        cursor = conn.execute(
            """
            SELECT id, fund_code, fund_name, created_at, updated_at
            FROM funds
            WHERE fund_code = ?
            """,
            (fund_code,),
        )
        return cursor.fetchone()

    def _ensure_fund_tx(
        self,
        conn: sqlite3.Connection,
        fund_code: str,
        fund_name: str = "",
    ) -> dict[str, Any]:
        code = self._normalize_fund_code(fund_code)
        if not code:
            raise ValueError("基金代码不能为空")

        now_ts = int(time.time())
        name = str(fund_name or "").strip()
        conn.execute(
            """
            INSERT INTO funds (fund_code, fund_name, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(fund_code) DO UPDATE SET
                fund_name = CASE
                    WHEN excluded.fund_name != '' THEN excluded.fund_name
                    ELSE funds.fund_name
                END,
                updated_at = excluded.updated_at
            """,
            (code, name, now_ts, now_ts),
        )

        row = self._get_fund_by_code_tx(conn, code)
        if row is None:
            raise RuntimeError("基金信息保存失败")
        return self._row_to_fund(row)

    def get_or_create_fund(self, fund_code: Any, fund_name: str = "") -> dict[str, Any]:
        code = self._normalize_fund_code(fund_code)
        if not code:
            raise ValueError("基金代码不能为空")

        with self._connect() as conn:
            conn.execute("BEGIN")
            return self._ensure_fund_tx(conn, code, fund_name)

    def get_fund_by_code(self, fund_code: Any) -> dict[str, Any] | None:
        code = self._normalize_fund_code(fund_code)
        if not code:
            return None

        with self._connect() as conn:
            row = self._get_fund_by_code_tx(conn, code)
            if row is None:
                return None
            return self._row_to_fund(row)

    def list_funds(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, fund_code, fund_name, created_at, updated_at
                FROM funds
                ORDER BY fund_code ASC
                """
            ).fetchall()
        return [self._row_to_fund(row) for row in rows]

    def list_position_funds(self) -> list[dict[str, Any]]:
        """列出当前存在持仓记录的基金（去重）。"""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT
                    f.id,
                    f.fund_code,
                    f.fund_name,
                    f.created_at,
                    f.updated_at
                FROM user_fund_positions p
                JOIN funds f ON f.id = p.fund_id
                ORDER BY f.fund_code ASC
                """
            ).fetchall()
        return [self._row_to_fund(row) for row in rows]

    def get_latest_nav_date(self, fund_code: Any) -> str | None:
        """获取某只基金已入库的最新净值日期（YYYY-MM-DD）。"""
        code = self._normalize_fund_code(fund_code)
        if not code:
            return None

        with self._connect() as conn:
            fund_row = self._get_fund_by_code_tx(conn=conn, fund_code=code)
            if fund_row is None:
                return None

            fund_id = int(fund_row["id"])
            nav_tables = self._resolve_nav_tables_tx(
                conn=conn,
                include_legacy=True,
                order_desc=True,
            )
            latest_date: str | None = None
            for table_name in nav_tables:
                quoted_table = self._quote_identifier(table_name)
                row = conn.execute(
                    f"""
                    SELECT nav_date
                    FROM {quoted_table}
                    WHERE fund_id = ?
                    ORDER BY nav_date DESC
                    LIMIT 1
                    """,
                    (fund_id,),
                ).fetchone()
                if row is None:
                    continue
                nav_date = str(row["nav_date"])
                if latest_date is None or nav_date > latest_date:
                    latest_date = nav_date
        return latest_date

    def _get_position_tx(
        self,
        conn: sqlite3.Connection,
        platform: str,
        user_id: str,
        fund_id: int,
    ) -> sqlite3.Row | None:
        cursor = conn.execute(
            """
            SELECT
                p.platform,
                p.user_id,
                p.fund_id,
                f.fund_code,
                f.fund_name,
                p.avg_cost,
                p.shares,
                p.created_at,
                p.updated_at
            FROM user_fund_positions p
            JOIN funds f ON f.id = p.fund_id
            WHERE p.platform = ? AND p.user_id = ? AND p.fund_id = ?
            """,
            (platform, user_id, fund_id),
        )
        return cursor.fetchone()

    def _get_position_by_code_tx(
        self,
        conn: sqlite3.Connection,
        platform: str,
        user_id: str,
        fund_code: str,
    ) -> sqlite3.Row | None:
        cursor = conn.execute(
            """
            SELECT
                p.platform,
                p.user_id,
                p.fund_id,
                f.fund_code,
                f.fund_name,
                p.avg_cost,
                p.shares,
                p.created_at,
                p.updated_at
            FROM user_fund_positions p
            JOIN funds f ON f.id = p.fund_id
            WHERE p.platform = ? AND p.user_id = ? AND f.fund_code = ?
            """,
            (platform, user_id, fund_code),
        )
        return cursor.fetchone()

    def _upsert_position_tx(
        self,
        conn: sqlite3.Connection,
        platform: str,
        user_id: str,
        fund_id: int,
        avg_cost: float,
        shares: float,
    ) -> dict[str, Any]:
        now_ts = int(time.time())

        existing = self._get_position_tx(conn, platform, user_id, fund_id)
        if existing is None:
            conn.execute(
                """
                INSERT INTO user_fund_positions (
                    platform, user_id, fund_id, avg_cost, shares, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (platform, user_id, fund_id, avg_cost, shares, now_ts, now_ts),
            )
        else:
            old_avg_cost = float(existing["avg_cost"])
            old_shares = float(existing["shares"])
            merged_shares = old_shares + shares
            if merged_shares <= 0:
                raise ValueError("合并后的持有份额必须大于 0")

            merged_avg_cost = (
                old_avg_cost * old_shares + avg_cost * shares
            ) / merged_shares

            conn.execute(
                """
                UPDATE user_fund_positions
                SET avg_cost = ?, shares = ?, updated_at = ?
                WHERE platform = ? AND user_id = ? AND fund_id = ?
                """,
                (merged_avg_cost, merged_shares, now_ts, platform, user_id, fund_id),
            )

        row = self._get_position_tx(conn, platform, user_id, fund_id)
        if row is None:
            raise RuntimeError("持仓保存失败")
        return self._row_to_position(row)

    def add_or_merge_position(
        self,
        platform: Any,
        user_id: Any,
        fund_code: Any,
        avg_cost: Any,
        shares: Any,
        fund_name: str = "",
    ) -> dict[str, Any]:
        platform_key = self._normalize_key(platform, fallback="unknown")
        user_key = self._normalize_key(user_id)
        if not user_key:
            raise ValueError("用户 ID 不能为空")

        code = self._normalize_fund_code(fund_code)
        if not code:
            raise ValueError("基金代码不能为空")

        avg_cost_num = self._as_positive_float(avg_cost, "平均成本")
        shares_num = self._as_positive_float(shares, "持有份额")

        with self._connect() as conn:
            conn.execute("BEGIN")
            fund = self._ensure_fund_tx(
                conn=conn,
                fund_code=code,
                fund_name=str(fund_name or "").strip(),
            )
            return self._upsert_position_tx(
                conn=conn,
                platform=platform_key,
                user_id=user_key,
                fund_id=int(fund["id"]),
                avg_cost=avg_cost_num,
                shares=shares_num,
            )

    def add_or_merge_positions(
        self,
        platform: Any,
        user_id: Any,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        platform_key = self._normalize_key(platform, fallback="unknown")
        user_key = self._normalize_key(user_id)
        if not user_key:
            raise ValueError("用户 ID 不能为空")
        if not records:
            return []

        saved_records: list[dict[str, Any]] = []
        with self._connect() as conn:
            conn.execute("BEGIN")
            for record in records:
                code = self._normalize_fund_code(record.get("fund_code"))
                if not code:
                    raise ValueError("基金代码不能为空")

                avg_cost = self._as_positive_float(record.get("avg_cost"), "平均成本")
                shares = self._as_positive_float(record.get("shares"), "持有份额")
                fund_name = str(record.get("fund_name") or "").strip()

                fund = self._ensure_fund_tx(
                    conn=conn,
                    fund_code=code,
                    fund_name=fund_name,
                )
                saved_records.append(
                    self._upsert_position_tx(
                        conn=conn,
                        platform=platform_key,
                        user_id=user_key,
                        fund_id=int(fund["id"]),
                        avg_cost=avg_cost,
                        shares=shares,
                    )
                )

        return saved_records

    def list_positions(self, platform: Any, user_id: Any) -> list[dict[str, Any]]:
        platform_key = self._normalize_key(platform, fallback="unknown")
        user_key = self._normalize_key(user_id)
        if not user_key:
            return []

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    p.platform,
                    p.user_id,
                    p.fund_id,
                    f.fund_code,
                    f.fund_name,
                    p.avg_cost,
                    p.shares,
                    p.created_at,
                    p.updated_at
                FROM user_fund_positions p
                JOIN funds f ON f.id = p.fund_id
                WHERE p.platform = ? AND p.user_id = ?
                ORDER BY f.fund_code ASC
                """,
                (platform_key, user_key),
            ).fetchall()

        return [self._row_to_position(row) for row in rows]

    def repair_user_position_funds(
        self,
        platform: Any,
        user_id: Any,
        fund_name_map: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        修复某个用户持仓相关的基金数据（仅当前用户范围）：
        1) 规范化基金代码（如 7721 -> 007721）
        2) 合并规范化后产生的重复持仓
        3) 补齐/更新基金名称
        4) 将该用户的历史日志关联到规范化后的基金
        """
        platform_key = self._normalize_key(platform, fallback="unknown")
        user_key = self._normalize_key(user_id)
        if not user_key:
            raise ValueError("用户 ID 不能为空")

        name_lookup: dict[str, str] = {}
        for key, value in (fund_name_map or {}).items():
            normalized_key = self._normalize_fund_code(key)
            name_text = str(value or "").strip()
            if normalized_key and name_text:
                name_lookup[normalized_key] = name_text

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    p.platform,
                    p.user_id,
                    p.fund_id,
                    f.fund_code,
                    f.fund_name,
                    p.avg_cost,
                    p.shares,
                    p.created_at,
                    p.updated_at
                FROM user_fund_positions p
                JOIN funds f ON f.id = p.fund_id
                WHERE p.platform = ? AND p.user_id = ?
                ORDER BY f.fund_code ASC
                """,
                (platform_key, user_key),
            ).fetchall()

            stats: dict[str, Any] = {
                "positions_total": len(rows),
                "funds_total": len({int(row["fund_id"]) for row in rows}),
                "funds_processed": 0,
                "codes_normalized": 0,
                "fund_names_fixed": 0,
                "positions_relinked": 0,
                "positions_merged": 0,
                "logs_relinked": 0,
                "failed": 0,
                "errors": [],
            }
            if not rows:
                return stats

            conn.execute("BEGIN")
            processed_fund_ids: set[int] = set()
            for row in rows:
                origin_fund_id = int(row["fund_id"])
                if origin_fund_id in processed_fund_ids:
                    continue
                processed_fund_ids.add(origin_fund_id)
                stats["funds_processed"] += 1

                try:
                    current_position = self._get_position_tx(
                        conn=conn,
                        platform=platform_key,
                        user_id=user_key,
                        fund_id=origin_fund_id,
                    )
                    if current_position is None:
                        continue

                    current_code = str(current_position["fund_code"] or "").strip()
                    normalized_code = self._normalize_fund_code(current_code)
                    if not normalized_code:
                        raise ValueError("基金代码为空")

                    target_name = str(name_lookup.get(normalized_code) or "").strip()
                    if not target_name:
                        target_name = str(current_position["fund_name"] or "").strip()

                    target_before = self._get_fund_by_code_tx(conn, normalized_code)
                    before_name = str(target_before["fund_name"] or "").strip() if target_before else ""
                    target_fund = self._ensure_fund_tx(
                        conn=conn,
                        fund_code=normalized_code,
                        fund_name=target_name,
                    )
                    target_fund_id = int(target_fund["id"])
                    after_name = str(target_fund.get("fund_name") or "").strip()

                    if target_name and after_name and after_name != before_name:
                        stats["fund_names_fixed"] += 1
                    if normalized_code != current_code:
                        stats["codes_normalized"] += 1

                    if target_fund_id == origin_fund_id:
                        continue

                    now_ts = int(time.time())
                    target_position = self._get_position_tx(
                        conn=conn,
                        platform=platform_key,
                        user_id=user_key,
                        fund_id=target_fund_id,
                    )
                    latest_origin_position = self._get_position_tx(
                        conn=conn,
                        platform=platform_key,
                        user_id=user_key,
                        fund_id=origin_fund_id,
                    )
                    if latest_origin_position is None:
                        continue

                    if target_position is None:
                        cursor = conn.execute(
                            """
                            UPDATE user_fund_positions
                            SET fund_id = ?, updated_at = ?
                            WHERE platform = ? AND user_id = ? AND fund_id = ?
                            """,
                            (
                                target_fund_id,
                                now_ts,
                                platform_key,
                                user_key,
                                origin_fund_id,
                            ),
                        )
                        stats["positions_relinked"] += int(cursor.rowcount or 0)
                    else:
                        origin_shares = float(latest_origin_position["shares"])
                        target_shares = float(target_position["shares"])
                        merged_shares = origin_shares + target_shares

                        if merged_shares <= 0:
                            conn.execute(
                                """
                                DELETE FROM user_fund_positions
                                WHERE platform = ? AND user_id = ? AND fund_id = ?
                                """,
                                (platform_key, user_key, origin_fund_id),
                            )
                        else:
                            origin_avg_cost = float(latest_origin_position["avg_cost"])
                            target_avg_cost = float(target_position["avg_cost"])
                            merged_avg_cost = (
                                origin_avg_cost * origin_shares + target_avg_cost * target_shares
                            ) / merged_shares
                            conn.execute(
                                """
                                UPDATE user_fund_positions
                                SET avg_cost = ?, shares = ?, updated_at = ?
                                WHERE platform = ? AND user_id = ? AND fund_id = ?
                                """,
                                (
                                    merged_avg_cost,
                                    merged_shares,
                                    now_ts,
                                    platform_key,
                                    user_key,
                                    target_fund_id,
                                ),
                            )
                            conn.execute(
                                """
                                DELETE FROM user_fund_positions
                                WHERE platform = ? AND user_id = ? AND fund_id = ?
                                """,
                                (platform_key, user_key, origin_fund_id),
                            )
                        stats["positions_merged"] += 1

                    log_cursor = conn.execute(
                        """
                        UPDATE user_fund_position_logs
                        SET fund_id = ?
                        WHERE platform = ? AND user_id = ? AND fund_id = ?
                        """,
                        (target_fund_id, platform_key, user_key, origin_fund_id),
                    )
                    stats["logs_relinked"] += int(log_cursor.rowcount or 0)
                except Exception as e:
                    stats["failed"] += 1
                    if len(stats["errors"]) < 5:
                        stats["errors"].append(f"{row['fund_code']}: {str(e)}")

            return stats

    def get_position(
        self,
        platform: Any,
        user_id: Any,
        fund_code: Any,
    ) -> dict[str, Any] | None:
        platform_key = self._normalize_key(platform, fallback="unknown")
        user_key = self._normalize_key(user_id)
        code = self._normalize_fund_code(fund_code)
        if not user_key or not code:
            return None

        with self._connect() as conn:
            row = self._get_position_by_code_tx(
                conn=conn,
                platform=platform_key,
                user_id=user_key,
                fund_code=code,
            )
            if row is None:
                return None
            return self._row_to_position(row)

    def reduce_position(
        self,
        platform: Any,
        user_id: Any,
        fund_code: Any,
        shares: Any,
    ) -> dict[str, Any]:
        platform_key = self._normalize_key(platform, fallback="unknown")
        user_key = self._normalize_key(user_id)
        code = self._normalize_fund_code(fund_code)
        if not user_key:
            raise ValueError("用户 ID 不能为空")
        if not code:
            raise ValueError("基金代码不能为空")

        shares_num = self._as_positive_float(shares, "卖出份额")

        with self._connect() as conn:
            conn.execute("BEGIN")
            row = self._get_position_by_code_tx(
                conn=conn,
                platform=platform_key,
                user_id=user_key,
                fund_code=code,
            )
            if row is None:
                raise ValueError(f"未找到基金 {code} 的持仓记录")

            current_shares = float(row["shares"])
            if shares_num > current_shares + 1e-8:
                raise ValueError(
                    f"卖出份额不能超过当前持有份额（当前: {current_shares:,.4f}）"
                )

            remaining = current_shares - shares_num
            now_ts = int(time.time())
            deleted = False
            if remaining <= 1e-8:
                remaining = 0.0
                deleted = True
                conn.execute(
                    """
                    DELETE FROM user_fund_positions
                    WHERE platform = ? AND user_id = ? AND fund_id = ?
                    """,
                    (platform_key, user_key, int(row["fund_id"])),
                )
            else:
                conn.execute(
                    """
                    UPDATE user_fund_positions
                    SET shares = ?, updated_at = ?
                    WHERE platform = ? AND user_id = ? AND fund_id = ?
                    """,
                    (
                        remaining,
                        now_ts,
                        platform_key,
                        user_key,
                        int(row["fund_id"]),
                    ),
                )

            return {
                "platform": platform_key,
                "user_id": user_key,
                "fund_id": int(row["fund_id"]),
                "fund_code": str(row["fund_code"]),
                "fund_name": str(row["fund_name"] or ""),
                "avg_cost": float(row["avg_cost"]),
                "shares_before": current_shares,
                "shares_sold": shares_num,
                "shares_after": remaining,
                "position_deleted": deleted,
            }

    def reduce_position_with_log(
        self,
        platform: Any,
        user_id: Any,
        fund_code: Any,
        shares: Any,
        action: Any,
        settlement_nav: Any = None,
        settlement_nav_date: Any = None,
        expected_settlement_date: Any = None,
        settlement_rule: Any = "",
        profit_amount: Any = None,
        note: Any = "",
        fund_name: str = "",
    ) -> dict[str, Any]:
        platform_key = self._normalize_key(platform, fallback="unknown")
        user_key = self._normalize_key(user_id)
        code = self._normalize_fund_code(fund_code)
        action_text = str(action or "").strip().lower()
        if not user_key:
            raise ValueError("用户 ID 不能为空")
        if not code:
            raise ValueError("基金代码不能为空")
        if not action_text:
            raise ValueError("操作类型不能为空")

        shares_num = self._as_positive_float(shares, "卖出份额")
        settlement_nav_num = None if settlement_nav is None else float(settlement_nav)
        settlement_nav_date_text = (
            self._normalize_nav_date(settlement_nav_date) if settlement_nav_date else None
        )
        expected_settlement_date_text = (
            self._normalize_nav_date(expected_settlement_date)
            if expected_settlement_date
            else None
        )
        settlement_rule_text = str(settlement_rule or "").strip()
        profit_amount_num = None if profit_amount is None else float(profit_amount)
        note_text = str(note or "").strip()
        fund_name_text = str(fund_name or "").strip()

        with self._connect() as conn:
            conn.execute("BEGIN")
            row = self._get_position_by_code_tx(
                conn=conn,
                platform=platform_key,
                user_id=user_key,
                fund_code=code,
            )
            if row is None:
                raise ValueError(f"未找到基金 {code} 的持仓记录")

            if fund_name_text:
                self._ensure_fund_tx(
                    conn=conn,
                    fund_code=code,
                    fund_name=fund_name_text,
                )

            current_shares = float(row["shares"])
            if shares_num > current_shares + 1e-8:
                raise ValueError(
                    f"卖出份额不能超过当前持有份额（当前: {current_shares:,.4f}）"
                )

            remaining = current_shares - shares_num
            now_ts = int(time.time())
            deleted = False
            if remaining <= 1e-8:
                remaining = 0.0
                deleted = True
                conn.execute(
                    """
                    DELETE FROM user_fund_positions
                    WHERE platform = ? AND user_id = ? AND fund_id = ?
                    """,
                    (platform_key, user_key, int(row["fund_id"])),
                )
            else:
                conn.execute(
                    """
                    UPDATE user_fund_positions
                    SET shares = ?, updated_at = ?
                    WHERE platform = ? AND user_id = ? AND fund_id = ?
                    """,
                    (
                        remaining,
                        now_ts,
                        platform_key,
                        user_key,
                        int(row["fund_id"]),
                    ),
                )

            cursor = conn.execute(
                """
                INSERT INTO user_fund_position_logs (
                    platform,
                    user_id,
                    fund_id,
                    action,
                    shares_delta,
                    shares_before,
                    shares_after,
                    avg_cost,
                    settlement_nav,
                    settlement_nav_date,
                    expected_settlement_date,
                    settlement_rule,
                    profit_amount,
                    note,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    platform_key,
                    user_key,
                    int(row["fund_id"]),
                    action_text,
                    -shares_num,
                    current_shares,
                    remaining,
                    float(row["avg_cost"]),
                    settlement_nav_num,
                    settlement_nav_date_text,
                    expected_settlement_date_text,
                    settlement_rule_text,
                    profit_amount_num,
                    note_text,
                    now_ts,
                ),
            )

            return {
                "platform": platform_key,
                "user_id": user_key,
                "fund_id": int(row["fund_id"]),
                "fund_code": str(row["fund_code"]),
                "fund_name": str(row["fund_name"] or ""),
                "avg_cost": float(row["avg_cost"]),
                "shares_before": current_shares,
                "shares_sold": shares_num,
                "shares_after": remaining,
                "position_deleted": deleted,
                "action": action_text,
                "log_id": int(cursor.lastrowid or 0),
            }

    def add_position_log(
        self,
        platform: Any,
        user_id: Any,
        fund_code: Any,
        action: Any,
        shares_delta: Any,
        shares_before: Any,
        shares_after: Any,
        avg_cost: Any,
        settlement_nav: Any = None,
        settlement_nav_date: Any = None,
        expected_settlement_date: Any = None,
        settlement_rule: Any = "",
        profit_amount: Any = None,
        note: Any = "",
        fund_name: str = "",
    ) -> int:
        platform_key = self._normalize_key(platform, fallback="unknown")
        user_key = self._normalize_key(user_id)
        code = self._normalize_fund_code(fund_code)
        action_text = str(action or "").strip().lower()
        if not user_key:
            raise ValueError("用户 ID 不能为空")
        if not code:
            raise ValueError("基金代码不能为空")
        if not action_text:
            raise ValueError("操作类型不能为空")

        shares_delta_num = float(shares_delta)
        shares_before_num = float(shares_before)
        shares_after_num = float(shares_after)
        avg_cost_num = float(avg_cost)
        settlement_nav_num = None if settlement_nav is None else float(settlement_nav)
        profit_amount_num = None if profit_amount is None else float(profit_amount)
        settlement_nav_date_text = (
            self._normalize_nav_date(settlement_nav_date) if settlement_nav_date else None
        )
        expected_settlement_date_text = (
            self._normalize_nav_date(expected_settlement_date)
            if expected_settlement_date
            else None
        )
        settlement_rule_text = str(settlement_rule or "").strip()
        note_text = str(note or "").strip()
        fund_name_text = str(fund_name or "").strip()
        now_ts = int(time.time())

        with self._connect() as conn:
            conn.execute("BEGIN")
            fund = self._ensure_fund_tx(
                conn=conn,
                fund_code=code,
                fund_name=fund_name_text,
            )
            cursor = conn.execute(
                """
                INSERT INTO user_fund_position_logs (
                    platform,
                    user_id,
                    fund_id,
                    action,
                    shares_delta,
                    shares_before,
                    shares_after,
                    avg_cost,
                    settlement_nav,
                    settlement_nav_date,
                    expected_settlement_date,
                    settlement_rule,
                    profit_amount,
                    note,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    platform_key,
                    user_key,
                    int(fund["id"]),
                    action_text,
                    shares_delta_num,
                    shares_before_num,
                    shares_after_num,
                    avg_cost_num,
                    settlement_nav_num,
                    settlement_nav_date_text,
                    expected_settlement_date_text,
                    settlement_rule_text,
                    profit_amount_num,
                    note_text,
                    now_ts,
                ),
            )
            return int(cursor.lastrowid or 0)

    def list_position_logs(
        self,
        platform: Any,
        user_id: Any,
        limit: int = 50,
        actions: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        platform_key = self._normalize_key(platform, fallback="unknown")
        user_key = self._normalize_key(user_id)
        if not user_key:
            return []

        query_limit = max(1, min(int(limit or 50), 500))
        action_texts = [str(item or "").strip().lower() for item in (actions or [])]
        action_texts = [item for item in action_texts if item]

        sql = """
            SELECT
                l.id,
                l.platform,
                l.user_id,
                l.fund_id,
                f.fund_code,
                f.fund_name,
                l.action,
                l.shares_delta,
                l.shares_before,
                l.shares_after,
                l.avg_cost,
                l.settlement_nav,
                l.settlement_nav_date,
                l.expected_settlement_date,
                l.settlement_rule,
                l.profit_amount,
                l.note,
                l.created_at
            FROM user_fund_position_logs l
            JOIN funds f ON f.id = l.fund_id
            WHERE l.platform = ? AND l.user_id = ?
        """
        params: list[Any] = [platform_key, user_key]

        if action_texts:
            placeholders = ",".join("?" for _ in action_texts)
            sql += f" AND l.action IN ({placeholders})"
            params.extend(action_texts)

        sql += " ORDER BY l.created_at DESC, l.id DESC LIMIT ?"
        params.append(query_limit)

        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [self._row_to_position_log(row) for row in rows]

    def delete_position(self, platform: Any, user_id: Any, fund_code: Any) -> bool:
        platform_key = self._normalize_key(platform, fallback="unknown")
        user_key = self._normalize_key(user_id)
        code = self._normalize_fund_code(fund_code)
        if not user_key or not code:
            return False

        with self._connect() as conn:
            row = self._get_fund_by_code_tx(conn, code)
            if row is None:
                return False
            cursor = conn.execute(
                """
                DELETE FROM user_fund_positions
                WHERE platform = ? AND user_id = ? AND fund_id = ?
                """,
                (platform_key, user_key, int(row["id"])),
            )
            return cursor.rowcount > 0

    def clear_positions(self, platform: Any, user_id: Any) -> int:
        platform_key = self._normalize_key(platform, fallback="unknown")
        user_key = self._normalize_key(user_id)
        if not user_key:
            return 0

        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM user_fund_positions WHERE platform = ? AND user_id = ?",
                (platform_key, user_key),
            )
            return int(cursor.rowcount)

    def upsert_fund_nav_history(
        self,
        fund_code: Any,
        nav_records: list[dict[str, Any]],
        fund_name: str = "",
        source: str = "",
    ) -> int:
        """
        保存基金历史净值。

        nav_records 每条记录支持字段：
        - nav_date/date: 净值日期（YYYY-MM-DD）
        - unit_nav: 单位净值（必填）
        - accum_nav: 累计净值（可选）
        - change_rate: 涨跌幅（可选，百分比数值）
        """
        code = self._normalize_fund_code(fund_code)
        if not code:
            raise ValueError("基金代码不能为空")
        if not nav_records:
            return 0

        source_text = str(source or "").strip()
        now_ts = int(time.time())
        affected = 0
        prepared: dict[str, list[tuple[Any, ...]]] = {}

        for record in nav_records:
            nav_date = self._normalize_nav_date(
                record.get("nav_date", record.get("date"))
            )
            unit_nav = self._as_positive_float(record.get("unit_nav"), "单位净值")
            accum_nav_raw = record.get("accum_nav")
            accum_nav = (
                None
                if accum_nav_raw in (None, "", "--")
                else self._safe_float(accum_nav_raw, default=0.0)
            )
            change_rate_raw = record.get("change_rate")
            change_rate = (
                None
                if change_rate_raw in (None, "", "--")
                else self._safe_float(change_rate_raw, default=0.0)
            )
            table_name = self._build_nav_partition_table_name(nav_date)
            prepared.setdefault(table_name, []).append(
                (
                    nav_date,
                    unit_nav,
                    accum_nav,
                    change_rate,
                    source_text,
                    now_ts,
                    now_ts,
                )
            )

        with self._connect() as conn:
            conn.execute("BEGIN")
            fund = self._ensure_fund_tx(
                conn=conn,
                fund_code=code,
                fund_name=str(fund_name or "").strip(),
            )
            fund_id = int(fund["id"])

            for table_name, rows in prepared.items():
                self._ensure_nav_partition_table_tx(conn=conn, table_name=table_name)
                quoted_table = self._quote_identifier(table_name)
                for row_data in rows:
                    conn.execute(
                        f"""
                        INSERT INTO {quoted_table} (
                            fund_id,
                            nav_date,
                            unit_nav,
                            accum_nav,
                            change_rate,
                            source,
                            created_at,
                            updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(fund_id, nav_date) DO UPDATE SET
                            unit_nav = excluded.unit_nav,
                            accum_nav = excluded.accum_nav,
                            change_rate = excluded.change_rate,
                            source = CASE
                                WHEN excluded.source != '' THEN excluded.source
                                ELSE {quoted_table}.source
                            END,
                            updated_at = excluded.updated_at
                        """,
                        (
                            fund_id,
                            row_data[0],
                            row_data[1],
                            row_data[2],
                            row_data[3],
                            row_data[4],
                            row_data[5],
                            row_data[6],
                        ),
                    )
                    affected += 1

        return affected

    def list_fund_nav_history(
        self,
        fund_code: Any,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 120,
    ) -> list[dict[str, Any]]:
        code = self._normalize_fund_code(fund_code)
        if not code:
            return []

        date_from = self._normalize_nav_date(start_date) if start_date else None
        date_to = self._normalize_nav_date(end_date) if end_date else None
        query_limit = max(1, min(int(limit or 120), 2000))

        with self._connect() as conn:
            nav_tables = self._resolve_nav_tables_tx(
                conn=conn,
                start_date=date_from,
                end_date=date_to,
                include_legacy=True,
                order_desc=True,
            )
            merged_rows: list[sqlite3.Row] = []
            seen_dates: set[str] = set()

            for table_name in nav_tables:
                quoted_table = self._quote_identifier(table_name)
                sql = f"""
                    SELECT
                        h.fund_id,
                        f.fund_code,
                        f.fund_name,
                        h.nav_date,
                        h.unit_nav,
                        h.accum_nav,
                        h.change_rate,
                        h.source,
                        h.created_at,
                        h.updated_at
                    FROM {quoted_table} h
                    JOIN funds f ON f.id = h.fund_id
                    WHERE f.fund_code = ?
                """
                params: list[Any] = [code]

                if date_from:
                    sql += " AND h.nav_date >= ?"
                    params.append(date_from)
                if date_to:
                    sql += " AND h.nav_date <= ?"
                    params.append(date_to)
                sql += " ORDER BY h.nav_date DESC LIMIT ?"
                params.append(query_limit)

                rows = conn.execute(sql, tuple(params)).fetchall()
                for row in rows:
                    nav_date_key = str(row["nav_date"])
                    if nav_date_key in seen_dates:
                        continue
                    seen_dates.add(nav_date_key)
                    merged_rows.append(row)

        sorted_rows = sorted(
            merged_rows,
            key=lambda row: (
                str(row["nav_date"]),
                int(row["updated_at"]),
            ),
            reverse=True,
        )
        return [self._row_to_nav(row) for row in sorted_rows[:query_limit]]

    def get_nav_on_or_after(
        self,
        fund_code: Any,
        start_date: Any,
        end_date: Any = None,
    ) -> dict[str, Any] | None:
        code = self._normalize_fund_code(fund_code)
        if not code:
            return None

        start_date_text = self._normalize_nav_date(start_date)
        end_date_text = self._normalize_nav_date(end_date) if end_date else None

        with self._connect() as conn:
            nav_tables = self._resolve_nav_tables_tx(
                conn=conn,
                start_date=start_date_text,
                end_date=end_date_text,
                include_legacy=True,
                order_desc=False,
            )
            best_row: sqlite3.Row | None = None
            for table_name in nav_tables:
                quoted_table = self._quote_identifier(table_name)
                sql = f"""
                    SELECT
                        h.fund_id,
                        f.fund_code,
                        f.fund_name,
                        h.nav_date,
                        h.unit_nav,
                        h.accum_nav,
                        h.change_rate,
                        h.source,
                        h.created_at,
                        h.updated_at
                    FROM {quoted_table} h
                    JOIN funds f ON f.id = h.fund_id
                    WHERE f.fund_code = ? AND h.nav_date >= ?
                """
                params: list[Any] = [code, start_date_text]
                if end_date_text:
                    sql += " AND h.nav_date <= ?"
                    params.append(end_date_text)
                sql += " ORDER BY h.nav_date ASC LIMIT 1"

                row = conn.execute(sql, tuple(params)).fetchone()
                if row is None:
                    continue
                if best_row is None or str(row["nav_date"]) < str(best_row["nav_date"]):
                    best_row = row

        if best_row is None:
            return None
        return self._row_to_nav(best_row)

    def get_latest_nav_record(self, fund_code: Any) -> dict[str, Any] | None:
        records = self.list_fund_nav_history(fund_code=fund_code, limit=1)
        if not records:
            return None
        return records[0]
