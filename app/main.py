"""FastAPI 入口：选择检测单，下载可在空库执行的关联数据 SQL；历史溯源网页。"""
from __future__ import annotations

import os
from urllib.parse import quote

import psycopg
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from psycopg.rows import dict_row

from extractor import ROOT_TABLE, ExtractError, RootNotFound, extract_order
from history import (
    AlreadyEnabled,
    audit_status,
    build_timeline,
    closure,
    closure_keys,
    correction_relevance,
    describe_changes,
    diff_closures,
    enable_audit,
    known_orders,
    retest_path,
)
from sqlgen import build_sql

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://lab:lab@localhost:5432/labdb"
)

app = FastAPI(title="检测单关联数据抽取")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))


def _fmt_dt(value) -> str:
    if value is None:
        return "—"
    return value.strftime("%Y-%m-%d %H:%M:%S %Z").strip()


def _fmt_val(value) -> str:
    return "NULL" if value is None else str(value)


templates.env.filters["dt"] = _fmt_dt
templates.env.filters["val"] = _fmt_val


def _connect() -> psycopg.Connection:
    return psycopg.connect(DATABASE_URL)


def _error_page(status: int, message: str) -> HTMLResponse:
    html = templates.get_template("error.html").render(status=status, message=message)
    return HTMLResponse(content=html, status_code=status)


@app.exception_handler(psycopg.Error)
def db_error_handler(request: Request, exc: psycopg.Error):
    return _error_page(500, f"数据库访问失败：{exc}")


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    with _connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        orders = cur.execute(
            """
            SELECT o.id, o.order_no, o.status,
                   s.sample_code, b.batch_no,
                   o.latest_retest_orig, o.latest_retest_retest
            FROM test_orders o
            LEFT JOIN samples s ON s.id = o.sample_id
            LEFT JOIN batches b ON b.id = s.batch_id
            ORDER BY o.id
            """
        ).fetchall()
        counts = {
            t: cur.execute(f'SELECT count(*) AS n FROM "{t}"').fetchone()["n"]
            for t in ("batches", "samples", "test_orders", "retest_links")
        }
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"orders": orders, "counts": counts},
    )


@app.get("/export/{order_id}")
def export(order_id: int):
    try:
        with _connect() as conn:
            # 同一已提交视图：REPEATABLE READ 只读事务，MVCC 快照不阻塞源端写入
            conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            conn.read_only = True
            result = extract_order(conn, order_id)
            script = build_sql(result)
    except RootNotFound as exc:
        return _error_page(404, str(exc))
    except (ExtractError, psycopg.Error) as exc:
        # 读取或生成失败：整体报错，不提供任何部分下载
        return _error_page(500, f"抽取或生成失败：{exc}")
    # 全部生成成功后才返回完整文件（内存一次性装配，不做流式输出）
    return Response(
        content=script.encode("utf-8"),
        media_type="application/sql; charset=utf-8",
        headers={
            "Content-Disposition": "attachment; "
            f'filename="test_order_{order_id}_extract.sql"'
        },
    )


# ---------------------------------------------------------------- 历史溯源

@app.post("/history/enable")
def history_enable():
    """启用留痕：单事务内锁表 -> 建对象 -> 快照基线；失败整体回滚。"""
    try:
        with _connect() as conn:
            baseline_rows = enable_audit(conn)
    except AlreadyEnabled as exc:
        return RedirectResponse(f"/history?msg={quote(str(exc))}", status_code=303)
    except psycopg.errors.LockNotAvailable:
        return _error_page(
            503, "有未提交的长事务正在写入，未能在 5 秒内取得锁；"
                 "未做任何改动，请稍后重试。")
    except (RuntimeError, psycopg.Error) as exc:
        return _error_page(500, f"启用留痕失败（已整体回滚，未留下半启用状态）：{exc}")
    msg = (f"留痕已启用：已保存四表基线共 {baseline_rows} 行；"
           "此后任何 SQL 连接对四表的行级增删改都会按事务记录。")
    return RedirectResponse(f"/history?msg={quote(msg)}", status_code=303)


@app.get("/history", response_class=HTMLResponse)
def history_page(request: Request, order_id: int | None = None,
                 all: int = 0, msg: str = ""):
    with _connect() as conn:
        conn.read_only = True  # 浏览历史只读快照，不阻塞写入
        status = audit_status(conn)
        context: dict = {
            "status": status, "msg": msg,
            "order_id": order_id, "show_all": bool(all),
        }
        if status["enabled"]:
            timeline = build_timeline(conn)
            orders = known_orders(timeline, conn)
            context["orders"] = orders
            if order_id is not None:
                context["order_exists"] = any(o["id"] == order_id for o in orders)
                context["corrections"] = timeline.corrections
                context["relevance"] = correction_relevance(timeline, order_id)
    return templates.TemplateResponse(
        request=request, name="history.html", context=context)


@app.get("/history/order/{order_id}/correction/{seq}", response_class=HTMLResponse)
def history_detail(request: Request, order_id: int, seq: int):
    with _connect() as conn:
        conn.read_only = True
        if not audit_status(conn)["enabled"]:
            return _error_page(404, "历史留痕尚未启用。")
        timeline = build_timeline(conn)
        corr = next(
            (c for c in timeline.corrections if c.commit_seq == seq), None)
        if corr is None:
            return _error_page(404, f"不存在序号为 {seq} 的已提交更正。")
        before = timeline.state_at(seq - 1)
        after = timeline.state_at(seq)
        root = (order_id,)
        clo_before = closure(timeline.tables, before, root)
        clo_after = closure(timeline.tables, after, root)
        if not clo_before[ROOT_TABLE] and not clo_after[ROOT_TABLE]:
            return _error_page(
                404, f"检测单 id={order_id} 在第 {seq} 次更正前后均不存在。")
        diffs = diff_closures(timeline.tables, clo_before, clo_after)
        clo_keys = closure_keys(timeline.tables, before, root) | \
            closure_keys(timeline.tables, after, root)
        relevance = correction_relevance(timeline, order_id)
        relevant_seqs = sorted(s for s, r in relevance.items() if r)
        order_row = clo_after[ROOT_TABLE].get(root) or clo_before[ROOT_TABLE].get(root)
        context = {
            "order_id": order_id,
            "order_no": (order_row or {}).get("order_no"),
            "corr": corr,
            "relevant": relevance.get(seq, False),
            "path_before": retest_path(before, order_id),
            "path_after": retest_path(after, order_id),
            "diffs": diffs,
            "any_changed": any(b["n_changed"] for b in diffs),
            "changes": describe_changes(timeline, corr, clo_keys),
            "prev_seq": max((s for s in relevant_seqs if s < seq), default=None),
            "next_seq": min((s for s in relevant_seqs if s > seq), default=None),
        }
    return templates.TemplateResponse(
        request=request, name="history_detail.html", context=context)
