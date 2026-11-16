#!/usr/bin/env python3
"""端到端自测：网页、抽取 SQL 内容、空库还原、快照一致性、失败路径。

用法（环境变量均有默认值）：
    APP_URL       应用地址，默认 http://127.0.0.1:8000
    BAD_APP_URL   指向坏数据库的应用地址（可选，用于验证失败路径）
    DATABASE_URL  源库连接串，默认 postgresql://lab:lab@127.0.0.1:5432/labdb
    ADMIN_URL     管理库连接串，默认 postgresql://lab:lab@127.0.0.1:5432/postgres
    RESTORE_DB    还原测试库名，默认 rtdb（不存在则创建；各用例顺序覆盖执行，
                  依赖生成脚本自带的 DROP TABLE IF EXISTS ... CASCADE）
    PSQL          psql 可执行文件路径（可选；不提供则改用 psycopg 执行脚本）
    WORKDIR       下载文件存放目录，默认 /workspace/tmp_extract
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import urllib.error
import urllib.request

import psycopg

APP_URL = os.environ.get("APP_URL", "http://127.0.0.1:8000").rstrip("/")
BAD_APP_URL = os.environ.get("BAD_APP_URL", "").rstrip("/")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://lab:lab@127.0.0.1:5432/labdb")
ADMIN_URL = os.environ.get("ADMIN_URL", "postgresql://lab:lab@127.0.0.1:5432/postgres")
RESTORE_DB = os.environ.get("RESTORE_DB", "rtdb")
PSQL = os.environ.get("PSQL", "")
WORKDIR = os.environ.get("WORKDIR", "/workspace/tmp_extract")

FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = ""):
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {name}" + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def http_get(url: str):
    """返回 (status, headers, body bytes)；headers 键统一小写；HTTP 错误状态也正常返回。"""
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in e.headers.items()}, e.read()


def http_post(url: str):
    """POST（空 body）；303 重定向由 urllib 自动跟随为 GET。"""
    req = urllib.request.Request(url, data=b"", method="POST")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in e.headers.items()}, e.read()


def download(order_id: int) -> tuple[int, dict, bytes]:
    return http_get(f"{APP_URL}/export/{order_id}")


def save_sql(order_id: int, body: bytes) -> str:
    os.makedirs(WORKDIR, exist_ok=True)
    path = os.path.join(WORKDIR, f"order_{order_id}.sql")
    with open(path, "wb") as f:
        f.write(body)
    return path


def db_url(dbname: str) -> str:
    """把 ADMIN_URL 路径部分的数据库名替换为 dbname，保留查询参数。"""
    return re.sub(r"^((?:postgresql|postgres)://[^/]*)/[^/?]*",
                  lambda m: m.group(1) + "/" + dbname, ADMIN_URL)


def ensure_restore_db():
    """还原测试库只建一次；不在测试里 DROP DATABASE（部分环境无权限）。"""
    try:
        with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
            conn.execute(f'CREATE DATABASE "{RESTORE_DB}"')
    except psycopg.errors.DuplicateDatabase:
        pass


def apply_sql(dbname: str, path: str):
    """用真实客户端把脚本应用到库中；任何错误都视为失败。"""
    if PSQL:
        env = dict(os.environ, PGCONNECT_TIMEOUT="10")
        r = subprocess.run(
            [PSQL, "-v", "ON_ERROR_STOP=1", "-X", "-d", dbname, "-f", path],
            capture_output=True, text=True, env=env,
        )
        if r.returncode != 0:
            raise RuntimeError(f"psql 执行失败: {r.stderr}\n{r.stdout}")
    else:
        with open(path, encoding="utf-8") as f:
            script = f.read()
        with psycopg.connect(db_url(dbname), autocommit=True) as conn:
            conn.execute(script)


def db_counts(dbname: str) -> dict[str, int]:
    out = {}
    with psycopg.connect(db_url(dbname)) as conn:
        for t in ("batches", "samples", "test_orders", "retest_links"):
            out[t] = conn.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0]
    return out


def test_index():
    status, _, body = http_get(f"{APP_URL}/")
    html = body.decode("utf-8", "replace")
    check("首页返回 200", status == 200, f"status={status}")
    check("首页列出全部 8 张检测单",
          all(f"ORD-00{i}" in html for i in range(1, 9)))
    check("首页显示四表行数", all(
        t in html for t in ("batches", "samples", "test_orders", "retest_links")))


def test_export_content():
    # 检测单 4：循环引用链 4->5->6
    status, headers, body = download(4)
    sql = body.decode("utf-8")
    check("导出 4 返回 200", status == 200, f"status={status}")
    check("导出 4 是附件下载",
          "attachment" in headers.get("content-disposition", "")
          and "test_order_4_extract.sql" in headers.get("content-disposition", ""))
    check("导出 4 含 4 个 CREATE TABLE", sql.count("CREATE TABLE") == 4)
    check("导出 4 含 5 个补外键 ALTER", sql.count("ALTER TABLE") == 5)
    check("导出 4 保留可延迟复合外键",
          "DEFERRABLE" in sql and "INITIALLY DEFERRED" in sql)
    check("导出 4 含循环链上的检测单",
          all(s in sql for s in ("'ORD-004'", "'ORD-005'", "'ORD-006'")))
    check("导出 4 含复测关联行", "'设备故障，安排复测'" in sql and "'复测仍超限，再次复测'" in sql)
    check("导出 4 不带入无关行",
          not any(s in sql for s in ("'ORD-001'", "'ORD-002'", "'ORD-007'", "'ORD-008'",
                                     "'S-001'", "'S-004'", "'S-005'", "'S-006'",
                                     "'B-2026-001'", "'B-2026-003'")))
    save_sql(4, body)

    # 检测单 1：无复测；同批次的其他样本/检测单不应被反向带入
    _, _, body1 = download(1)
    sql1 = body1.decode("utf-8")
    check("导出 1 不含同批次其他样本（无反向引用）",
          "'S-002'" not in sql1 and "'S-006'" not in sql1 and "'ORD-002'" not in sql1)
    check("导出 1 无复测关联行", 'INSERT INTO "retest_links"' not in sql1)
    save_sql(1, body1)

    # 检测单 8：复合外键一列为空 -> 不追查；样本批次为空 -> 不追查
    _, _, body8 = download(8)
    sql8 = body8.decode("utf-8")
    check("导出 8 含自身行", "'ORD-008'" in sql8 and "'S-005'" in sql8)
    check("导出 8 复合外键部分为空不追查", 'INSERT INTO "retest_links"' not in sql8)
    check("导出 8 可空外键为空不追查批次", 'INSERT INTO "batches"' not in sql8)
    save_sql(8, body8)

    # 检测单 2：单级复测
    _, _, body2 = download(2)
    sql2 = body2.decode("utf-8")
    check("导出 2 含原单与复测单", "'ORD-002'" in sql2 and "'ORD-003'" in sql2)
    check("导出 2 不重复导出共享的样本行", sql2.count("'S-002'") == 1)
    save_sql(2, body2)


def test_restore():
    ensure_restore_db()
    # 首个用例在全新空库上执行；后续用例依赖脚本自带的 DROP IF EXISTS 覆盖
    cases = [
        (4, {"batches": 1, "samples": 1, "test_orders": 3, "retest_links": 2}),
        (1, {"batches": 1, "samples": 1, "test_orders": 1, "retest_links": 0}),
        (8, {"batches": 0, "samples": 1, "test_orders": 1, "retest_links": 0}),
        (2, {"batches": 1, "samples": 1, "test_orders": 2, "retest_links": 1}),
    ]
    for order_id, expect in cases:
        path = os.path.join(WORKDIR, f"order_{order_id}.sql")
        try:
            apply_sql(RESTORE_DB, path)
            counts = db_counts(RESTORE_DB)
            check(f"空库还原 order {order_id} 行数正确", counts == expect,
                  f"got {counts}")
        except Exception as e:  # noqa: BLE001
            check(f"空库还原 order {order_id}", False, str(e))

    # 幂等：同一脚本可重复执行
    try:
        apply_sql(RESTORE_DB, os.path.join(WORKDIR, "order_4.sql"))
        check("脚本可在同一库重复执行",
              db_counts(RESTORE_DB)["test_orders"] == 3)
    except Exception as e:  # noqa: BLE001
        check("脚本可重复执行", False, str(e))

    # 循环引用完整还原 + 外键约束真实存在（此时库中是 order 4 的数据）
    try:
        with psycopg.connect(db_url(RESTORE_DB)) as conn:
            row = conn.execute(
                "SELECT latest_retest_orig, latest_retest_retest "
                "FROM test_orders WHERE id = 4"
            ).fetchone()
            check("循环引用数据完整 (order4 -> link(4,5))", row == (4, 5), f"got {row}")
            fk_count = conn.execute(
                "SELECT count(*) FROM pg_constraint WHERE contype = 'f'"
            ).fetchone()[0]
            check("还原库外键数量为 5", fk_count == 5, f"got {fk_count}")
            deferrable = conn.execute(
                """
                SELECT condeferrable AND condeferred FROM pg_constraint
                WHERE contype = 'f' AND conrelid = 'test_orders'::regclass
                  AND array_length(conkey, 1) = 2
                """
            ).fetchone()
            check("复合外键保持 DEFERRABLE INITIALLY DEFERRED", deferrable == (True,))
            # 外键约束真实生效：插入违反外键的行应被拒绝
            violated = False
            try:
                conn.execute(
                    "INSERT INTO samples (id, batch_id, sample_code) "
                    "VALUES (900, 424242, 'BAD')")
            except psycopg.errors.ForeignKeyViolation:
                violated = True
            check("还原库外键约束真实生效", violated)
    except Exception as e:  # noqa: BLE001
        check("循环引用还原校验", False, str(e))


def test_not_found():
    status, headers, body = download(999)
    check("不存在的检测单返回 404", status == 404, f"status={status}")
    check("404 不提供附件下载",
          "attachment" not in headers.get("content-disposition", ""))
    check("404 页面说明未生成部分结果", "未生成任何部分结果" in body.decode("utf-8", "replace"))


def test_snapshot_isolation():
    """REPEATABLE READ 快照：读取期间源端写入不被阻塞，且读不到新数据。"""
    probe = psycopg.connect(DATABASE_URL)
    probe.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    probe.read_only = True
    before = probe.execute("SELECT count(*) FROM test_orders").fetchone()[0]

    writer = psycopg.connect(DATABASE_URL, autocommit=True)
    writer.execute(
        "INSERT INTO test_orders VALUES (9, 6, 'ORD-009', 'done', NULL, NULL)"
    )  # 若快照阻塞写入，这里会挂起或报错
    during = probe.execute("SELECT count(*) FROM test_orders").fetchone()[0]
    check("快照内计数不受并发写入影响", during == before, f"before={before} during={during}")
    writer.execute("DELETE FROM test_orders WHERE id = 9")
    writer.close()
    probe.close()


def test_failure_path():
    if not BAD_APP_URL:
        print("[SKIP] 未提供 BAD_APP_URL，跳过失败路径测试")
        return
    status, headers, _ = http_get(f"{BAD_APP_URL}/export/1")
    check("数据库不可用时导出返回 500", status == 500, f"status={status}")
    check("500 不提供附件下载",
          "attachment" not in headers.get("content-disposition", ""))
    status2, _, _ = http_get(f"{BAD_APP_URL}/")
    check("数据库不可用时首页返回 500 错误页", status2 == 500, f"status={status2}")


# ---------------------------------------------------------------- 历史溯源

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUDIT_TABLES = ("batches", "samples", "test_orders", "retest_links")


def _history_cleanup():
    """还原到启用前状态：删触发器与 audit 方案、恢复演示数据。

    保证自测可重复运行；也用于上次运行中途失败后的现场清理。
    """
    try:
        with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
            if not conn.execute(
                "SELECT to_regclass('audit.meta') IS NOT NULL"
            ).fetchone()[0]:
                return
            for t in AUDIT_TABLES:
                for trg in ("audit_log_row", "audit_seal_tx", "audit_block_truncate"):
                    conn.execute(f'DROP TRIGGER IF EXISTS {trg} ON "{t}"')
            conn.execute("DROP SCHEMA audit CASCADE")
            conn.execute(
                "TRUNCATE batches, samples, test_orders, retest_links "
                "RESTART IDENTITY CASCADE"
            )
            with open(os.path.join(REPO_ROOT, "db", "init", "02_seed.sql"),
                      encoding="utf-8") as f:
                conn.execute(f.read())
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] 留痕清理失败（可手动执行 DROP SCHEMA audit CASCADE 后重试）: {e}")


def _test_history_inner():
    # 1) 未启用状态
    status, _, body = http_get(f"{APP_URL}/history")
    html = body.decode("utf-8", "replace")
    check("历史页(未启用)返回 200", status == 200, f"status={status}")
    check("历史页(未启用)显示未启用", "未启用" in html)

    # 2) 启用留痕（POST 303 -> GET）
    status, _, body = http_post(f"{APP_URL}/history/enable")
    html = body.decode("utf-8", "replace")
    check("启用留痕成功", status == 200 and "留痕已启用" in html, f"status={status}")
    status, _, body = http_post(f"{APP_URL}/history/enable")
    check("重复启用提示已启用、不改写历史",
          status == 200 and "此前已启用" in body.decode("utf-8", "replace"),
          f"status={status}")

    # 3) 基线行数 = 启用瞬间的四表内容
    with psycopg.connect(DATABASE_URL) as conn:
        base = dict(conn.execute(
            "SELECT table_name, count(*) FROM audit.baseline GROUP BY table_name"
        ).fetchall())
    check("基线四表行数正确",
          base == {"batches": 3, "samples": 6, "test_orders": 8, "retest_links": 3},
          f"got {base}")

    # 4) 原导出不受留痕影响（audit 对象不在 public schema）
    st, _, b4 = download(4)
    sql4 = b4.decode("utf-8")
    check("启用留痕后导出仍只含 4 个 CREATE TABLE",
          st == 200 and sql4.count("CREATE TABLE") == 4, f"status={st}")

    # 5) 通过独立 SQL 连接（不经过应用）制造更正
    #    预期提交顺序: C1=1, C2=2, B=3, A=4, C6=5, C7=6
    w = psycopg.connect(DATABASE_URL, autocommit=True)
    w.execute("SET application_name = 'selftest-writer'")
    w.execute("UPDATE samples SET sample_code='S-001*' WHERE id=1")        # C1 单行修改
    with psycopg.connect(DATABASE_URL) as tx:                               # C2 一个事务两处修改
        tx.execute("UPDATE batches SET note='加急批次·已复核' WHERE id=2")
        tx.execute("UPDATE test_orders SET status='closed' WHERE id=4")
    rb = psycopg.connect(DATABASE_URL)                                      # C3 回滚不留痕
    rb.execute("UPDATE samples SET sample_code='GHOST' WHERE id=2")
    rb.rollback()
    rb.close()
    tx_a = psycopg.connect(DATABASE_URL)                                    # A 先写
    tx_a.execute("UPDATE batches SET note='常规批次·改A' WHERE id=1")
    tx_b = psycopg.connect(DATABASE_URL)
    tx_b.execute("UPDATE samples SET sample_code='S-003*' WHERE id=3")      # B 后写
    tx_b.commit()                                                           # B 先提交 -> 序号更小
    tx_b.close()
    tx_a.commit()                                                           # A 后提交 -> 序号更大
    tx_a.close()
    with psycopg.connect(DATABASE_URL) as tx:                               # C6 删除并复用编号
        tx.execute("DELETE FROM test_orders WHERE id=8")
        tx.execute("INSERT INTO test_orders VALUES (8, 6, 'ORD-008R', 'done', NULL, NULL)")
    w.execute("UPDATE batches SET note='常规批次·已锁定' WHERE id=1")        # C7 只改主档
    w.close()

    # 6) 数据库侧断言
    with psycopg.connect(DATABASE_URL) as conn:
        seqs = [r[0] for r in conn.execute(
            "SELECT commit_seq FROM audit.tx WHERE commit_seq IS NOT NULL "
            "ORDER BY commit_seq").fetchall()]
        check("已提交更正共 6 笔且序号连续", seqs == [1, 2, 3, 4, 5, 6],
              f"got {seqs}")
        c2_txid = conn.execute(
            "SELECT txid FROM audit.row_change "
            "WHERE table_name='batches' AND new_row->>'note'='加急批次·已复核'"
        ).fetchone()[0]
        n = conn.execute(
            "SELECT count(*) FROM audit.row_change WHERE txid=%s", (c2_txid,)
        ).fetchone()[0]
        check("一个事务的多次修改归为一笔更正", n == 2, f"changes={n}")
        ghost = conn.execute(
            "SELECT count(*) FROM audit.row_change "
            "WHERE coalesce(new_row->>'sample_code', old_row->>'sample_code')='GHOST'"
        ).fetchone()[0]
        check("回滚不留历史", ghost == 0)
        seq_b = conn.execute(
            "SELECT commit_seq FROM audit.tx t JOIN audit.row_change r USING (txid) "
            "WHERE r.table_name='samples' AND r.new_row->>'sample_code'='S-003*'"
        ).fetchone()[0]
        seq_a = conn.execute(
            "SELECT commit_seq FROM audit.tx t JOIN audit.row_change r USING (txid) "
            "WHERE r.table_name='batches' AND r.new_row->>'note'='常规批次·改A'"
        ).fetchone()[0]
        check("先写后提交按实际提交顺序归位", seq_b < seq_a,
              f"B(seq={seq_b}) 应先于 A(seq={seq_a})")
        ntrig = conn.execute(
            "SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'audit\\_%'"
        ).fetchone()[0]
        check("留痕触发器持久化于数据库目录(4表×3)", ntrig == 12, f"got {ntrig}")

    # 7) 网页断言
    st, _, b = http_get(f"{APP_URL}/history?order_id=1")
    h = b.decode("utf-8", "replace")
    check("检测单1的更正列表返回 200", st == 200, f"status={st}")
    check("检测单1列出相关更正(1,4,6)",
          all(f"/history/order/1/correction/{s}" in h for s in (1, 4, 6)))
    check("检测单1默认不列出无关更正(2,3,5)的对比链接",
          not any(f"/history/order/1/correction/{s}" in h for s in (2, 3, 5)))
    st, _, b = http_get(f"{APP_URL}/history?order_id=1&all=1")
    check("全部模式下无关更正可见但标注不影响本单",
          st == 200 and "不影响本单" in b.decode("utf-8", "replace"))

    st, _, b = http_get(f"{APP_URL}/history/order/1/correction/1")
    h = b.decode("utf-8", "replace")
    check("更正1前后对比返回 200", st == 200, f"status={st}")
    check("更正1显示样本改前改后", "S-001" in h and "S-001*" in h)

    st, _, b = http_get(f"{APP_URL}/history/order/4/correction/2")
    h = b.decode("utf-8", "replace")
    check("更正2显示检测单状态与批次主档变化",
          st == 200 and "closed" in h and "加急批次·已复核" in h, f"status={st}")
    check("更正2显示复测引用路径(ORD-004→…→ORD-006)",
          "ORD-004" in h and "ORD-006" in h)

    st, _, b = http_get(f"{APP_URL}/history/order/8/correction/5")
    h = b.decode("utf-8", "replace")
    check("删除后复用编号：前后分别为 ORD-008 / ORD-008R",
          st == 200 and "ORD-008" in h and "ORD-008R" in h, f"status={st}")

    st, _, _ = http_get(f"{APP_URL}/history/order/1/correction/999")
    check("不存在的更正返回 404", st == 404, f"status={st}")

    # 8) 未提交长事务不阻塞无关样本更正；其回滚后无留痕
    long_tx = psycopg.connect(DATABASE_URL)
    long_tx.execute("UPDATE samples SET sample_code='S-004*' WHERE id=4")  # 保持未提交
    blocked = False
    try:
        with psycopg.connect(DATABASE_URL, autocommit=True) as w2:
            w2.execute("SET lock_timeout = '8s'")
            w2.execute("UPDATE samples SET sample_code='S-002*' WHERE id=2")
    except psycopg.errors.LockNotAvailable:
        blocked = True
    check("未提交长事务不阻塞无关样本更正", not blocked)
    long_tx.rollback()
    long_tx.close()
    with psycopg.connect(DATABASE_URL) as conn:
        n = conn.execute(
            "SELECT count(*) FROM audit.row_change "
            "WHERE new_row->>'sample_code'='S-004*'").fetchone()[0]
    check("长事务回滚后无留痕", n == 0)

    # 9) 浏览历史(快照读)不阻塞写入
    reader = psycopg.connect(DATABASE_URL)
    reader.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    reader.read_only = True
    reader.execute("SELECT count(*) FROM audit.tx").fetchone()
    blocked = False
    try:
        with psycopg.connect(DATABASE_URL, autocommit=True) as w3:
            w3.execute("SET lock_timeout = '8s'")
            w3.execute("UPDATE batches SET note='加急批次·二改' WHERE id=2")
    except psycopg.errors.LockNotAvailable:
        blocked = True
    check("浏览历史(快照读)不阻塞写入", not blocked)
    reader.close()

    # 10) 留痕失败须连同源修改回滚，不留半次更正
    adm = psycopg.connect(DATABASE_URL, autocommit=True)
    adm.execute("ALTER TABLE audit.row_change DROP COLUMN new_row")  # 人为制造留痕故障
    failed = False
    try:
        with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
            conn.execute("UPDATE samples SET sample_code='ZAP' WHERE id=5")
    except psycopg.Error:
        failed = True
    finally:  # 无论断言结果如何都先恢复留痕能力
        adm.execute("ALTER TABLE audit.row_change ADD COLUMN new_row jsonb")
    check("留痕失败时源修改随之失败", failed)
    with psycopg.connect(DATABASE_URL) as conn:
        v = conn.execute("SELECT sample_code FROM samples WHERE id=5").fetchone()[0]
        n = conn.execute(
            "SELECT count(*) FROM audit.row_change "
            "WHERE coalesce(new_row->>'sample_code', old_row->>'sample_code')='ZAP'"
        ).fetchone()[0]
    check("留痕失败不留半次更正", v == "S-005" and n == 0, f"sample5={v!r} audit={n}")
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        conn.execute("UPDATE samples SET sample_code='S-005*' WHERE id=5")
    with psycopg.connect(DATABASE_URL) as conn:
        n = conn.execute(
            "SELECT count(*) FROM audit.row_change "
            "WHERE new_row->>'sample_code'='S-005*'").fetchone()[0]
    check("留痕恢复后正常记录", n == 1)
    adm.close()

    # 11) TRUNCATE 被拦截（TRUNCATE 不产生行级事件，放行会丢历史）
    trunc_blocked = False
    try:
        with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
            conn.execute("TRUNCATE samples")
    except psycopg.Error:
        trunc_blocked = True
    check("留痕期间禁止 TRUNCATE 被留痕表", trunc_blocked)
    with psycopg.connect(DATABASE_URL) as conn:
        n = conn.execute("SELECT count(*) FROM samples").fetchone()[0]
    check("TRUNCATE 被拦截后数据完好", n == 6, f"samples={n}")


def test_history():
    """历史溯源端到端：启用/基线/捕获/提交顺序/回滚/编号复用/主档影响/原子性。

    会修改演示数据并启用留痕；结束时还原到启用前状态，保证自测可重复运行。
    """
    _history_cleanup()  # 上次运行若中途失败，先还原现场
    try:
        _test_history_inner()
    finally:
        _history_cleanup()


def main():
    test_index()
    test_export_content()
    test_restore()
    test_not_found()
    test_snapshot_isolation()
    test_failure_path()
    test_history()
    print()
    if FAILURES:
        print(f"共 {len(FAILURES)} 项失败: {FAILURES}")
        sys.exit(1)
    print("全部自测通过")


if __name__ == "__main__":
    main()
