import os
import sqlite3
import time
from datetime import date, datetime
from typing import Any

DEFAULT_DB_PATH = "data/plugins/astrbot_plugin_fund_analyzer_advance/fund.db"
DEFAULT_DATA_PATH = DEFAULT_DB_PATH


class DataHandler:
    """基金数据持久化处理器（基金主数据、用户持仓、历史净值）。"""

    SCHEMA_VERSION = "2"

    def __init__(self, path: str = DEFAULT_DB_PATH):
        self.path = path
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

                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
            conn.execute(
                "INSERT OR REPLACE INTO schema_meta(key, value) VALUES('schema_version', ?)",
                (self.SCHEMA_VERSION,),
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
            row = conn.execute(
                """
                SELECT h.nav_date
                FROM fund_nav_history h
                JOIN funds f ON f.id = h.fund_id
                WHERE f.fund_code = ?
                ORDER BY h.nav_date DESC
                LIMIT 1
                """,
                (code,),
            ).fetchone()
        if row is None:
            return None
        return str(row["nav_date"])

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

        with self._connect() as conn:
            conn.execute("BEGIN")
            fund = self._ensure_fund_tx(
                conn=conn,
                fund_code=code,
                fund_name=str(fund_name or "").strip(),
            )
            fund_id = int(fund["id"])

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

                conn.execute(
                    """
                    INSERT INTO fund_nav_history (
                        fund_id, nav_date, unit_nav, accum_nav, change_rate, source, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(fund_id, nav_date) DO UPDATE SET
                        unit_nav = excluded.unit_nav,
                        accum_nav = excluded.accum_nav,
                        change_rate = excluded.change_rate,
                        source = CASE
                            WHEN excluded.source != '' THEN excluded.source
                            ELSE fund_nav_history.source
                        END,
                        updated_at = excluded.updated_at
                    """,
                    (
                        fund_id,
                        nav_date,
                        unit_nav,
                        accum_nav,
                        change_rate,
                        source_text,
                        now_ts,
                        now_ts,
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

        sql = """
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
            FROM fund_nav_history h
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

        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [self._row_to_nav(row) for row in rows]
