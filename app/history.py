"""历史溯源：四表基线 + 行级留痕，按实际提交顺序重放，重建任一更正前后的关联现场。

机制概述
--------
- 「启用」在单个事务内完成：短暂锁定四表（SHARE ROW EXCLUSIVE，只挡写入不挡读取）、
  在 audit schema 下创建留痕对象、快照四表基线。任一步失败整体回滚，
  不会留下"半个启用状态"；拿不到锁会快速失败（lock_timeout），不长时间排队。
- 行级留痕由数据库触发器完成，对任意 SQL 连接生效（不依赖本应用）：
  - AFTER ROW 触发器 audit.log_row()：把增删改的旧/新行整行镜像写入
    audit.row_change，并按事务号在 audit.tx 登记一行（同事务多行只登记一次，
    因此一个事务的多次修改天然归为一笔完整更正）。
  - 可延迟约束触发器 audit.seal_tx()：COMMIT 时才从序列 audit.commit_seq 发放
    提交序号。序列顺序即实际提交顺序——先写后提交的事务按提交时刻归位，不会漏。
  - BEFORE TRUNCATE 触发器：直接报错。TRUNCATE 不产生行级事件，放行会悄悄弄丢历史。
- 留痕与源修改同属一个事务：回滚则两者一起消失；留痕失败则源修改随之失败回滚，
  不会留下"半次更正"。
- 历史只追加不改写（commit_seq 在提交瞬间一次性发放）；删除后复用编号、
  后续主档修改、服务重启都不会改写已记录的历史。
- 浏览历史只读 audit.* 表（MVCC 快照），不阻塞任何写入。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from psycopg import sql
from psycopg.rows import dict_row

from extractor import ROOT_TABLE, SCHEMA, TableMeta, load_metadata
from sqlgen import quote_ident

AUDIT_SCHEMA = "audit"

# 历史页表格展示顺序：根表在前，其余按名称
_PREFERRED_ORDER = ["test_orders", "samples", "batches", "retest_links"]

_OP_LABEL = {"I": "增", "U": "改", "D": "删"}


class AlreadyEnabled(Exception):
    """留痕此前已启用；基线与既有历史保持不变。"""


# ---------------------------------------------------------------- 启用留痕

_DDL_META = """
CREATE TABLE IF NOT EXISTS audit.meta (
    id            boolean PRIMARY KEY DEFAULT true CHECK (id),
    enabled_at    timestamptz NOT NULL DEFAULT now(),
    baseline_rows integer NOT NULL
)
"""

_DDL_SEQUENCE = "CREATE SEQUENCE audit.commit_seq AS bigint"

_DDL_TX = """
CREATE TABLE audit.tx (
    txid             bigint PRIMARY KEY,          -- pg 事务号（含 epoch，不会回卷重用）
    started_at       timestamptz NOT NULL,        -- 事务内首次行级变更的时刻
    committed_at     timestamptz,                 -- 提交处理时刻（clock_timestamp）
    commit_seq       bigint,                      -- 提交序号：提交时一次性发放
    db_user          text NOT NULL,
    application_name text NOT NULL DEFAULT '',
    client_addr      text
)
"""

_DDL_TX_SEQ_INDEX = """
CREATE INDEX audit_tx_commit_seq_idx ON audit.tx (commit_seq)
"""

_DDL_ROW_CHANGE = """
CREATE TABLE audit.row_change (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    txid       bigint NOT NULL REFERENCES audit.tx (txid),
    table_name text NOT NULL,
    op         char(1) NOT NULL CHECK (op IN ('I', 'U', 'D')),
    old_row    jsonb,                             -- 改前整行镜像（插入为 NULL）
    new_row    jsonb                              -- 改后整行镜像（删除为 NULL）
)
"""

_DDL_ROW_CHANGE_TX_INDEX = """
CREATE INDEX audit_row_change_tx_idx ON audit.row_change (txid, id)
"""

_DDL_BASELINE = """
CREATE TABLE audit.baseline (
    table_name text NOT NULL,
    row_data   jsonb NOT NULL                     -- 启用瞬间的整行镜像
)
"""

_FUNC_LOG_ROW = """
CREATE FUNCTION audit.log_row() RETURNS trigger
LANGUAGE plpgsql AS $func$
DECLARE
    v_txid bigint := txid_current();
BEGIN
    INSERT INTO audit.tx (txid, started_at, db_user, application_name, client_addr)
    VALUES (v_txid, now(), session_user,
            coalesce(current_setting('application_name', true), ''),
            host(inet_client_addr()))
    ON CONFLICT (txid) DO NOTHING;

    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit.row_change (txid, table_name, op, old_row, new_row)
        VALUES (v_txid, TG_TABLE_NAME, 'I', NULL, to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit.row_change (txid, table_name, op, old_row, new_row)
        VALUES (v_txid, TG_TABLE_NAME, 'U', to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    ELSE
        INSERT INTO audit.row_change (txid, table_name, op, old_row, new_row)
        VALUES (v_txid, TG_TABLE_NAME, 'D', to_jsonb(OLD), NULL);
        RETURN OLD;
    END IF;
END
$func$
"""

# 可延迟约束触发器：COMMIT 时才执行，为整个事务发放唯一的提交序号。
# 同一事务修改多行会触发多次，只有第一次生效（commit_seq IS NULL）。
_FUNC_SEAL_TX = """
CREATE FUNCTION audit.seal_tx() RETURNS trigger
LANGUAGE plpgsql AS $func$
BEGIN
    UPDATE audit.tx
       SET commit_seq   = nextval('audit.commit_seq'),
           committed_at = clock_timestamp()
     WHERE txid = txid_current()
       AND commit_seq IS NULL;
    RETURN NULL;
END
$func$
"""

# TRUNCATE 不产生行级事件，无法留痕——直接禁止，避免悄悄弄丢历史。
_FUNC_BLOCK_TRUNCATE = """
CREATE FUNCTION audit.block_truncate() RETURNS trigger
LANGUAGE plpgsql AS $func$
BEGIN
    RAISE EXCEPTION '历史留痕已启用：禁止 TRUNCATE 表 %（会绕过行级留痕）', TG_TABLE_NAME;
END
$func$
"""

_TRIG_LOG = """
CREATE TRIGGER audit_log_row
AFTER INSERT OR UPDATE OR DELETE ON {}
FOR EACH ROW EXECUTE FUNCTION audit.log_row()
"""

_TRIG_SEAL = """
CREATE CONSTRAINT TRIGGER audit_seal_tx
AFTER INSERT OR UPDATE OR DELETE ON {}
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION audit.seal_tx()
"""

_TRIG_TRUNCATE = """
CREATE TRIGGER audit_block_truncate
BEFORE TRUNCATE ON {}
FOR EACH STATEMENT EXECUTE FUNCTION audit.block_truncate()
"""

_TABLE_NAMES_SQL = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
ORDER BY table_name
"""


def enable_audit(conn) -> int:
    """在 conn 的当前事务内启用留痕，返回基线行数；调用方负责提交/回滚。

    单事务完成：锁表 -> 建对象 -> 快照基线。任一步失败整体回滚。
    """
    with conn.cursor(row_factory=dict_row) as cur:
        names = [r["table_name"] for r in cur.execute(_TABLE_NAMES_SQL)]
        if not names:
            raise RuntimeError("public schema 中没有需要留痕的表")

        # 1) 短暂锁表：只阻塞写入，读者不受影响；拿不到锁快速失败，不长时间排队
        cur.execute("SET LOCAL lock_timeout = '5s'")
        cur.execute(
            sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(
                sql.SQL(", ").join(sql.Identifier(SCHEMA, n) for n in names)
            )
        )
        # 锁定期内若有新表出现则整体失败重来（基线必须覆盖启用瞬间的全部表）
        names2 = [r["table_name"] for r in cur.execute(_TABLE_NAMES_SQL)]
        if names2 != names:
            raise RuntimeError("启用期间表集合发生变化，已回滚，请重试")

        # 2) 建 audit 对象（全部 DDL 与基线同事务，失败整体回滚）
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_SCHEMA}")
        cur.execute(_DDL_META)
        if cur.execute("SELECT 1 FROM audit.meta WHERE id").fetchone():
            raise AlreadyEnabled("历史留痕此前已启用，基线与既有历史保持不变")
        cur.execute(_DDL_SEQUENCE)
        cur.execute(_DDL_TX)
        cur.execute(_DDL_TX_SEQ_INDEX)
        cur.execute(_DDL_ROW_CHANGE)
        cur.execute(_DDL_ROW_CHANGE_TX_INDEX)
        cur.execute(_DDL_BASELINE)
        cur.execute(_FUNC_LOG_ROW)
        cur.execute(_FUNC_SEAL_TX)
        cur.execute(_FUNC_BLOCK_TRUNCATE)
        for name in names:
            qname = sql.Identifier(SCHEMA, name)
            cur.execute(sql.SQL(_TRIG_LOG).format(qname))
            cur.execute(sql.SQL(_TRIG_SEAL).format(qname))
            cur.execute(sql.SQL(_TRIG_TRUNCATE).format(qname))

        # 3) 快照四表基线（启用瞬间的已提交状态）
        total = 0
        for name in names:
            cur.execute(
                sql.SQL(
                    "INSERT INTO audit.baseline (table_name, row_data) "
                    "SELECT %s, to_jsonb(t) FROM {} t"
                ).format(sql.Identifier(SCHEMA, name)),
                (name,),
            )
            total += cur.rowcount
        cur.execute(
            "INSERT INTO audit.meta (id, baseline_rows) VALUES (true, %s)", (total,)
        )
        return total


def audit_status(conn) -> dict:
    """留痕状态：未启用 / 已启用（启用时间、基线行数、已提交更正数）。"""
    with conn.cursor(row_factory=dict_row) as cur:
        if not cur.execute("SELECT to_regclass('audit.meta') IS NOT NULL AS e").fetchone()["e"]:
            return {"enabled": False}
        meta = cur.execute(
            "SELECT enabled_at, baseline_rows FROM audit.meta WHERE id"
        ).fetchone()
        if meta is None:
            return {"enabled": False}
        agg = cur.execute(
            "SELECT count(*) AS n FROM audit.tx WHERE commit_seq IS NOT NULL"
        ).fetchone()
        return {
            "enabled": True,
            "enabled_at": meta["enabled_at"],
            "baseline_rows": meta["baseline_rows"],
            "corrections": agg["n"],
        }


# ---------------------------------------------------------------- 时间线重放

@dataclass
class RowChange:
    table: str
    op: str               # 'I' / 'U' / 'D'
    old: dict | None
    new: dict | None

    def touched_keys(self, tables: dict[str, TableMeta]) -> set[tuple[str, tuple]]:
        keys: set[tuple[str, tuple]] = set()
        tmeta = tables.get(self.table)
        if tmeta is None:
            return keys
        for img in (self.old, self.new):
            if img is not None:
                keys.add((self.table, pk_of(tmeta, img)))
        return keys


@dataclass
class Correction:
    """一笔完整更正 = 一个已提交事务的全部行级修改。"""
    commit_seq: int
    txid: int
    started_at: datetime
    committed_at: datetime
    db_user: str
    application_name: str
    changes: list[RowChange] = field(default_factory=list)

    def summary(self) -> str:
        counts: dict[tuple[str, str], int] = {}
        for ch in self.changes:
            key = (ch.table, ch.op)
            counts[key] = counts.get(key, 0) + 1
        if not counts:
            return "（无行级变更）"
        return "；".join(
            f"{t} {_OP_LABEL[o]}×{n}"
            for (t, o), n in sorted(counts.items())
        )


@dataclass
class Timeline:
    tables: dict[str, TableMeta]
    baseline: dict[str, dict[tuple, dict]]   # 表 -> 主键 -> 行
    corrections: list[Correction]            # 按 commit_seq 升序 = 实际提交顺序

    def state_at(self, upto_seq: int) -> dict[str, dict[tuple, dict]]:
        """重放到 commit_seq <= upto_seq 为止的四表状态；0 表示启用时的基线。"""
        state = {t: dict(rows) for t, rows in self.baseline.items()}
        for corr in self.corrections:
            if corr.commit_seq > upto_seq:
                break
            apply_correction(self.tables, state, corr)
        return state


def pk_of(tmeta: TableMeta, row: dict) -> tuple:
    return tuple(row.get(c) for c in tmeta.pk_columns)


def apply_correction(tables, state, corr: Correction) -> None:
    for ch in corr.changes:
        tmeta = tables.get(ch.table)
        if tmeta is None:
            continue
        bucket = state.setdefault(ch.table, {})
        if ch.old is not None:  # U / D：先摘除旧主键（主键本身可能被改）
            bucket.pop(pk_of(tmeta, ch.old), None)
        if ch.new is not None:  # I / U：写入新镜像
            bucket[pk_of(tmeta, ch.new)] = ch.new


def build_timeline(conn) -> Timeline:
    """从 audit.* 读取基线与全部已提交更正（只读，MVCC 快照不阻塞写入）。"""
    with conn.cursor(row_factory=dict_row) as cur:
        tables = load_metadata(cur)
        baseline: dict[str, dict[tuple, dict]] = {t: {} for t in tables}
        for r in cur.execute("SELECT table_name, row_data FROM audit.baseline"):
            tmeta = tables.get(r["table_name"])
            if tmeta is None:
                continue
            baseline[r["table_name"]][pk_of(tmeta, r["row_data"])] = r["row_data"]

        txs = cur.execute(
            "SELECT txid, commit_seq, started_at, committed_at, db_user, application_name "
            "FROM audit.tx WHERE commit_seq IS NOT NULL ORDER BY commit_seq"
        ).fetchall()
        changes_by_tx: dict[int, list[RowChange]] = {}
        for r in cur.execute(
            "SELECT txid, table_name, op, old_row, new_row "
            "FROM audit.row_change ORDER BY id"
        ):
            changes_by_tx.setdefault(r["txid"], []).append(
                RowChange(r["table_name"], r["op"], r["old_row"], r["new_row"])
            )
        corrections = [
            Correction(
                commit_seq=t["commit_seq"],
                txid=t["txid"],
                started_at=t["started_at"],
                committed_at=t["committed_at"],
                db_user=t["db_user"],
                application_name=t["application_name"],
                changes=changes_by_tx.get(t["txid"], []),
            )
            for t in txs
        ]
    return Timeline(tables=tables, baseline=baseline, corrections=corrections)


# ---------------------------------------------------------------- 闭包与差异

def closure(tables, state, root_pk: tuple) -> dict[str, dict[tuple, dict]]:
    """从检测单出发沿外键（本表 -> 被引用表）可达的全部行。

    与 extractor 同语义：复合外键任一列为空不追查；悬空引用跳过。
    """
    out: dict[str, dict[tuple, dict]] = {t: {} for t in tables}
    stack = [(ROOT_TABLE, root_pk)]
    while stack:
        table, pk = stack.pop()
        if pk in out[table]:
            continue
        row = state.get(table, {}).get(pk)
        if row is None:
            continue  # 根行不存在或引用悬空：跳过
        out[table][pk] = row
        for fk in tables[table].fks:
            values = tuple(row.get(c) for c in fk.columns)
            if any(v is None for v in values):
                continue
            stack.append((fk.ref_table, values))
    return out


def closure_keys(tables, state, root_pk: tuple) -> set[tuple[str, tuple]]:
    clo = closure(tables, state, root_pk)
    return {(t, pk) for t, rows in clo.items() for pk in rows}


def correction_relevance(timeline: Timeline, order_id: int) -> dict[int, bool]:
    """逐笔重放，判断每次更正是否触及该检测单改前或改后的关联闭包。

    只改关联主档（如批次备注）也会命中——闭包按当时状态计算。
    """
    rel: dict[int, bool] = {}
    state = {t: dict(rows) for t, rows in timeline.baseline.items()}
    root = (order_id,)
    for corr in timeline.corrections:
        before = closure_keys(timeline.tables, state, root)
        touched: set[tuple[str, tuple]] = set()
        for ch in corr.changes:
            touched |= ch.touched_keys(timeline.tables)
        apply_correction(timeline.tables, state, corr)
        after = closure_keys(timeline.tables, state, root)
        rel[corr.commit_seq] = bool(touched & (before | after))
    return rel


@dataclass
class RowDiff:
    pk: tuple
    status: str            # 'added' / 'removed' / 'changed' / 'same'
    old: dict | None
    new: dict | None
    changed_cols: set


def _display_order(tables) -> list[str]:
    pref = [t for t in _PREFERRED_ORDER if t in tables]
    rest = sorted(t for t in tables if t not in _PREFERRED_ORDER)
    return pref + rest


def diff_closures(tables, before, after) -> list[dict]:
    """对比两个闭包，按表给出行级差异（只含闭包内出现过的行）。"""
    result = []
    for t in _display_order(tables):
        keys = set(before.get(t, ())) | set(after.get(t, ()))
        if not keys:
            continue
        cols = [c.name for c in tables[t].columns]
        rows = []
        for pk in sorted(keys):
            old = before[t].get(pk)
            new = after[t].get(pk)
            if old is None:
                status, changed = "added", set(cols)
            elif new is None:
                status, changed = "removed", set(cols)
            elif old != new:
                status = "changed"
                changed = {c for c in cols if old.get(c) != new.get(c)}
            else:
                status, changed = "same", set()
            rows.append(RowDiff(pk, status, old, new, changed))
        result.append({
            "table": t,
            "columns": cols,
            "rows": rows,
            "n_changed": sum(1 for r in rows if r.status != "same"),
        })
    return result


def retest_path(state, order_id: int) -> str:
    """复测引用路径：沿 latest_retest 复合外键展开，如
    ORD-004 ─(4,5)→ ORD-005 ─(5,6)→ ORD-006"""
    parts: list[str] = []
    seen: set[int] = set()
    cur = order_id
    orders = state.get(ROOT_TABLE, {})
    links = state.get("retest_links", {})
    while True:
        if cur in seen:
            parts.append("↩ 回到已出现的单")
            break
        seen.add(cur)
        row = orders.get((cur,))
        if row is None:
            parts.append(f"#{cur}（不存在）")
            break
        parts.append(row.get("order_no") or f"#{cur}")
        lo, lr = row.get("latest_retest_orig"), row.get("latest_retest_retest")
        if lo is None or lr is None:
            break
        if (lo, lr) not in links:
            parts.append(f"─({lo},{lr})→ 关联缺失")
            break
        parts.append(f"─({lo},{lr})→")
        cur = lr
    return " ".join(parts)


def known_orders(timeline: Timeline, conn) -> list[dict]:
    """检测单选项：当前存在的 + 历史上出现过（可能已删除）的。"""
    latest: dict[int, str] = {}

    def see(img) -> None:
        if img is not None and img.get("id") is not None:
            latest[img["id"]] = img.get("order_no")

    for row in timeline.baseline.get(ROOT_TABLE, {}).values():
        see(row)
    for corr in timeline.corrections:
        for ch in corr.changes:
            if ch.table == ROOT_TABLE:
                see(ch.old)
                see(ch.new)
    alive: set[int] = set()
    with conn.cursor(row_factory=dict_row) as cur:
        for r in cur.execute(
            f"SELECT id, order_no FROM {quote_ident(ROOT_TABLE)} ORDER BY id"
        ):
            see(r)
            alive.add(r["id"])
    return [
        {"id": i, "order_no": latest[i], "exists_now": i in alive}
        for i in sorted(latest)
    ]


def describe_changes(timeline: Timeline, corr: Correction,
                     clo_keys: set[tuple[str, tuple]]) -> list[dict]:
    """本次更正的行级操作清单（含闭包外修改），供详情页展示。"""
    views = []
    for ch in corr.changes:
        keys = ch.touched_keys(timeline.tables)
        fields = []
        if ch.op == "U":
            for c in sorted(set(ch.old) | set(ch.new)):
                o, n = ch.old.get(c), ch.new.get(c)
                fields.append({"col": c, "old": o, "new": n, "changed": o != n})
        else:
            img = ch.new if ch.op == "I" else ch.old
            for c in sorted(img):
                fields.append({
                    "col": c,
                    "old": None if ch.op == "I" else img.get(c),
                    "new": img.get(c) if ch.op == "I" else None,
                    "changed": True,
                })
        views.append({
            "table": ch.table,
            "op": ch.op,
            "in_closure": bool(keys & clo_keys),
            "fields": fields,
        })
    return views
