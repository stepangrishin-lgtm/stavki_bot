import os
from dotenv import load_dotenv
load_dotenv()

import asyncio
import aiosqlite
from datetime import datetime
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from zoneinfo import ZoneInfo
from statistics import median

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ---------------- CONFIG ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not found. Create .env рядом с bot.py и добавьте BOT_TOKEN=...")

ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
TZ = os.getenv("TZ", "Europe/Moscow")
DB_PATH = "predictions.sqlite3"

# Очки: в этой версии без общего банка/лимита (по запросу пользователя)
MIN_POINTS = 1
MAX_POINTS = 10_000

# Точность: допуск = 10% от прогноза пользователя
TOLERANCE_RATE = Decimal("0.10")

# Чтобы не было T=0 для прогноза 0 (или слишком мелких чисел)
# используем минимальный допуск = step
# (step задаётся на вопросе)
# ---------------- HELPERS ----------------
def now_tz() -> datetime:
    return datetime.now(ZoneInfo(TZ))

def dec(s: str) -> Decimal:
    return Decimal(s.replace(",", ".").strip())

def round_display(x: Decimal, q: str = "0.1") -> str:
    return str(x.quantize(Decimal(q), rounding=ROUND_HALF_UP))

def validate_step(value: Decimal, step: Decimal) -> bool:
    if step == 0:
        return True
    q = value / step
    return q == q.to_integral_value()

def to_minutes_hhmm(s: str, step_min: int) -> int | None:
    """
    HH:MM -> minutes from 00:00
    """
    s = s.strip()
    if ":" not in s:
        return None
    hh, mm = s.split(":", 1)
    if not (hh.isdigit() and mm.isdigit()):
        return None
    h = int(hh)
    m = int(mm)
    if h < 0 or h > 23 or m < 0 or m > 59:
        return None
    total = h * 60 + m
    if step_min > 0 and total % step_min != 0:
        return None
    return total

def minutes_to_hhmm(minutes: int) -> str:
    minutes = minutes % (24 * 60)
    h = minutes // 60
    m = minutes % 60
    return f"{h:02d}:{m:02d}"

def k_unique_from_ratio(r: Decimal) -> Decimal:
    # r = k/N
    if r <= Decimal("0.07"):
        return Decimal("2.8")
    if r <= Decimal("0.17"):
        return Decimal("2.0")
    if r <= Decimal("0.40"):
        return Decimal("1.4")
    return Decimal("1.1")

def k_accuracy(err: Decimal, T: Decimal) -> Decimal:
    if err > T:
        return Decimal("0")
    # 1 + (1 - err/T) in [1;2]
    return (Decimal("1") + (Decimal("1") - (err / T))).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

def compute_bin(value: Decimal, W: Decimal) -> int:
    # bin = floor(value / W)
    return int((value / W).to_integral_value(rounding=ROUND_FLOOR))

def choose_cluster_width_W(step: Decimal, all_forecasts: list[Decimal]) -> Decimal:
    """
    Уникальность должна считаться по общим кластерам. При этом допуск точности теперь персональный (10% от прогноза),
    поэтому W выбираем единообразно по вопросу в момент закрытия.

    Простое и устойчивое правило:
    W = max(step, 10% от медианы |прогнозов|)

    Это "в духе 10%", но даёт одну ширину кластера для всех.
    """
    if not all_forecasts:
        return step if step > 0 else Decimal("1")

    abs_vals = [abs(x) for x in all_forecasts]
    m = Decimal(str(median([float(x) for x in abs_vals])))  # медиана через float; достаточно для W
    W = (m * TOLERANCE_RATE)
    if W <= 0:
        W = step if step > 0 else Decimal("1")
    if step > 0 and W < step:
        W = step
    return W.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

# ---------------- DB ----------------
async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            full_name TEXT,
            created_at TEXT NOT NULL
        );
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS questions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            qtype TEXT NOT NULL,          -- NUM or TIME
            step TEXT NOT NULL,           -- Decimal string for NUM, integer minutes for TIME
            status TEXT NOT NULL,         -- OPEN or SETTLED
            created_at TEXT NOT NULL,
            settled_at TEXT,
            fact_value TEXT               -- Decimal string for NUM, integer minutes for TIME
        );
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS bets (
            user_id INTEGER NOT NULL,
            question_id INTEGER NOT NULL,
            forecast_value TEXT NOT NULL, -- Decimal string for NUM, integer minutes for TIME
            points INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (user_id, question_id),
            FOREIGN KEY (question_id) REFERENCES questions(id)
        );
        """)
        await db.commit()

async def upsert_user(user_id: int, full_name: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        INSERT INTO users(user_id, full_name, created_at)
        VALUES(?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET full_name=excluded.full_name
        """, (user_id, full_name or "", now_tz().isoformat()))
        await db.commit()

async def create_question(title: str, qtype: str, step: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
        INSERT INTO questions(title, qtype, step, status, created_at)
        VALUES(?,?,?,?,?)
        """, (title, qtype, step, "OPEN", now_tz().isoformat()))
        await db.commit()
        return cur.lastrowid

async def list_open_questions():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
        SELECT id, title, qtype, step FROM questions
        WHERE status='OPEN'
        ORDER BY id DESC
        """)
        return await cur.fetchall()

async def get_question(qid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
        SELECT id, title, qtype, step, status, fact_value FROM questions WHERE id=?
        """, (qid,))
        return await cur.fetchone()

async def upsert_bet(user_id: int, qid: int, forecast_value: str, points: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        INSERT INTO bets(user_id, question_id, forecast_value, points, created_at)
        VALUES(?,?,?,?,?)
        ON CONFLICT(user_id, question_id) DO UPDATE SET
          forecast_value=excluded.forecast_value,
          points=excluded.points,
          created_at=excluded.created_at
        """, (user_id, qid, forecast_value, points, now_tz().isoformat()))
        await db.commit()

async def list_user_bets(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
        SELECT b.question_id, q.title, q.qtype, q.step, q.status, b.forecast_value, b.points, q.fact_value
        FROM bets b
        JOIN questions q ON q.id=b.question_id
        WHERE b.user_id=?
        ORDER BY b.created_at DESC
        """, (user_id,))
        return await cur.fetchall()

async def settle_question(qid: int, fact_value: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        UPDATE questions
        SET status='SETTLED', fact_value=?, settled_at=?
        WHERE id=? AND status='OPEN'
        """, (fact_value, now_tz().isoformat(), qid))
        await db.commit()

async def get_question_bets(qid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
        SELECT user_id, forecast_value, points
        FROM bets
        WHERE question_id=?
        """, (qid,))
        return await cur.fetchall()

# ---------------- UI ----------------
def kb_main(is_admin: bool):
    kb = InlineKeyboardBuilder()
    kb.button(text="Сделать прогноз", callback_data="bet:start")
    kb.button(text="Мои ставки", callback_data="bet:mine")
    if is_admin:
        kb.button(text="Админ: создать вопрос", callback_data="admin:create")
        kb.button(text="Админ: закрыть вопрос (ввести факт)", callback_data="admin:settle_pick")
    kb.adjust(1)
    return kb.as_markup()

def kb_questions(rows, prefix: str):
    kb = InlineKeyboardBuilder()
    for qid, title, qtype, step in rows:
        kb.button(text=f"#{qid} — {title}", callback_data=f"{prefix}:{qid}")
    kb.button(text="Назад", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()

# ---------------- BOT ----------------
bot = Bot(BOT_TOKEN)
dp = Dispatcher()

STATE: dict[int, dict] = {}  # user_id -> state dict

@dp.message(F.text.in_({"/start", "/help"}))
async def start(m: Message):
    await upsert_user(m.from_user.id, m.from_user.full_name or "")
    is_admin = m.from_user.id in ADMIN_IDS
    text = (
        "Бот прогнозов КЦ\n\n"
        "• Админ публикует вопрос — ставки открыты до публикации факта\n"
        "• Можно прогнозировать KPI и любые темы\n"
        "• Допуск точности: ±10% от вашего прогноза\n"
        "• После закрытия вопроса вы получите уведомление с результатом\n"
    )
    await m.answer(text, reply_markup=kb_main(is_admin))

@dp.callback_query(F.data == "menu")
async def menu(c: CallbackQuery):
    is_admin = c.from_user.id in ADMIN_IDS
    await c.message.edit_text("Меню:", reply_markup=kb_main(is_admin))
    await c.answer()

# ----------- USER: BET FLOW -----------
@dp.callback_query(F.data == "bet:start")
async def bet_start(c: CallbackQuery):
    rows = await list_open_questions()
    if not rows:
        await c.answer("Сейчас нет открытых вопросов.", show_alert=True)
        return
    STATE[c.from_user.id] = {"stage": "choose_question"}
    await c.message.edit_text("Выберите вопрос:", reply_markup=kb_questions(rows, "bet:q"))
    await c.answer()

@dp.callback_query(F.data.startswith("bet:q:"))
async def bet_choose_question(c: CallbackQuery):
    qid = int(c.data.split(":")[-1])
    q = await get_question(qid)
    if not q or q[4] != "OPEN":
        await c.answer("Вопрос не найден или уже закрыт.", show_alert=True)
        return

    _, title, qtype, step, status, _fact = q
    STATE[c.from_user.id] = {"stage": "enter_forecast", "qid": qid}

    if qtype == "NUM":
        await c.message.edit_text(
            f"Вопрос #{qid}: {title}\n\n"
            f"Введите число.\n"
            f"Шаг: {step}\n"
            f"Допуск точности будет ±10% от вашего значения."
        )
    else:
        await c.message.edit_text(
            f"Вопрос #{qid}: {title}\n\n"
            f"Введите время в формате HH:MM.\n"
            f"Шаг: {step} минут\n"
            f"Допуск точности будет ±10% от вашего времени (в минутах)."
        )
    await c.answer()

@dp.callback_query(F.data == "bet:mine")
async def bet_mine(c: CallbackQuery):
    rows = await list_user_bets(c.from_user.id)
    if not rows:
        await c.answer("У вас пока нет ставок.", show_alert=True)
        return

    lines = ["Ваши ставки:"]
    for qid, title, qtype, step, status, forecast, points, fact_value in rows[:20]:
        if qtype == "TIME":
            forecast_disp = minutes_to_hhmm(int(forecast))
        else:
            forecast_disp = forecast
        lines.append(f"• #{qid} ({status}) — {title}\n  прогноз: {forecast_disp}, очки: {points}")
    if len(rows) > 20:
        lines.append("\n…показаны последние 20")
    await c.message.edit_text("\n".join(lines))
    await c.answer()

# ----------- ADMIN: CREATE QUESTION -----------
@dp.callback_query(F.data == "admin:create")
async def admin_create(c: CallbackQuery):
    if c.from_user.id not in ADMIN_IDS:
        await c.answer("Нет доступа.", show_alert=True)
        return
    STATE[c.from_user.id] = {"stage": "admin_create_title"}
    await c.message.edit_text("Создание вопроса.\n\nВведите текст вопроса (title).")
    await c.answer()

# ----------- ADMIN: SETTLE QUESTION -----------
@dp.callback_query(F.data == "admin:settle_pick")
async def admin_settle_pick(c: CallbackQuery):
    if c.from_user.id not in ADMIN_IDS:
        await c.answer("Нет доступа.", show_alert=True)
        return
    rows = await list_open_questions()
    if not rows:
        await c.answer("Нет открытых вопросов.", show_alert=True)
        return
    STATE[c.from_user.id] = {"stage": "admin_settle_choose"}
    await c.message.edit_text("Выберите вопрос для закрытия (ввода факта):", reply_markup=kb_questions(rows, "admin:settle"))
    await c.answer()

@dp.callback_query(F.data.startswith("admin:settle:"))
async def admin_settle_choose(c: CallbackQuery):
    if c.from_user.id not in ADMIN_IDS:
        await c.answer("Нет доступа.", show_alert=True)
        return
    qid = int(c.data.split(":")[-1])
    q = await get_question(qid)
    if not q or q[4] != "OPEN":
        await c.answer("Вопрос уже закрыт или не найден.", show_alert=True)
        return

    _, title, qtype, step, status, _fact = q
    STATE[c.from_user.id] = {"stage": "admin_settle_enter_fact", "qid": qid}

    if qtype == "NUM":
        await c.message.edit_text(f"Ввод факта.\n\nВопрос #{qid}: {title}\nВведите итоговое число (факт).")
    else:
        await c.message.edit_text(f"Ввод факта.\n\nВопрос #{qid}: {title}\nВведите итоговое время HH:MM.")
    await c.answer()

# ----------- TEXT INPUT HANDLER -----------
@dp.message()
async def on_text(m: Message):
    uid = m.from_user.id
    st = STATE.get(uid)
    if not st:
        return

    # ---------- USER: forecast then points ----------
    if st.get("stage") == "enter_forecast":
        qid = st["qid"]
        q = await get_question(qid)
        if not q or q[4] != "OPEN":
            STATE.pop(uid, None)
            return await m.answer("Вопрос закрыт. Вернитесь в меню: /start")

        _, title, qtype, step, status, _fact = q

        if qtype == "NUM":
            try:
                v = dec(m.text)
            except Exception:
                return await m.answer("Введите число.")
            step_d = Decimal(step)
            if not validate_step(v, step_d):
                return await m.answer(f"Неверный шаг. Нужно кратно {step}.")
            st["forecast_value"] = str(v)
        else:
            step_min = int(step)
            mins = to_minutes_hhmm(m.text, step_min)
            if mins is None:
                return await m.answer(f"Неверный формат. Нужно HH:MM и кратно шагу {step_min} мин.")
            st["forecast_value"] = str(mins)

        st["stage"] = "enter_points"
        return await m.answer(f"Введите очки для ставки ({MIN_POINTS}–{MAX_POINTS}).")

    if st.get("stage") == "enter_points":
        qid = st["qid"]
        q = await get_question(qid)
        if not q or q[4] != "OPEN":
            STATE.pop(uid, None)
            return await m.answer("Вопрос закрыт. Ставка не сохранена. /start")

        try:
            pts = int(m.text.strip())
        except Exception:
            return await m.answer("Очки должны быть целым числом.")
        if pts < MIN_POINTS or pts > MAX_POINTS:
            return await m.answer(f"Очки должны быть в диапазоне {MIN_POINTS}–{MAX_POINTS}.")

        _, title, qtype, step, status, _fact = q
        forecast_value = st["forecast_value"]
        await upsert_bet(uid, qid, forecast_value, pts)

        # Показываем кластер и диапазон уникальности "на сейчас"
        bets = await get_question_bets(qid)
        all_forecasts = []
        if qtype == "NUM":
            all_forecasts = [Decimal(b[1]) for b in bets]
            my_v = Decimal(forecast_value)
            step_d = Decimal(step)
        else:
            all_forecasts = [Decimal(int(b[1])) for b in bets]
            my_v = Decimal(int(forecast_value))
            step_d = Decimal(int(step))  # minutes

        W = choose_cluster_width_W(step_d, all_forecasts)
        my_bin = compute_bin(my_v, W)

        # cluster count k and N so far
        k = 0
        for _u, fv, _p in bets:
            v = Decimal(fv) if qtype == "NUM" else Decimal(int(fv))
            if compute_bin(v, W) == my_bin:
                k += 1
        N = len(bets)

        # диапазон уникальности до закрытия (используем текущий N и k как "на сейчас")
        # так как пользователей не знаем заранее, показываем честный интервал при предположении, что N уже финальный:
        # (проще и не вводит в заблуждение).
        # Если хочешь "как раньше" с верхней границей участников — можно добавить TOTAL_USERS.
        ratio = Decimal(k) / Decimal(N) if N else Decimal("1")
        kuniq_now = k_unique_from_ratio(ratio)

        # кластерный интервал:
        cluster_from = (Decimal(my_bin) * W)
        cluster_to = (Decimal(my_bin + 1) * W)

        if qtype == "TIME":
            cluster_text = f"[{minutes_to_hhmm(int(cluster_from))}; {minutes_to_hhmm(int(cluster_to))})"
            my_disp = minutes_to_hhmm(int(my_v))
        else:
            cluster_text = f"[{round_display(cluster_from,'0.1')}; {round_display(cluster_to,'0.1')})"
            my_disp = str(my_v)

        STATE.pop(uid, None)
        is_admin = uid in ADMIN_IDS
        return await m.answer(
            f"✅ Ставка сохранена.\n\n"
            f"Вопрос #{qid}: {title}\n"
            f"Прогноз: {my_disp}\n"
            f"Очки: {pts}\n"
            f"Кластер (для уникальности): {cluster_text}\n"
            f"Текущий K_unique (может измениться пока вопрос открыт): {kuniq_now}\n\n"
            f"Меню: /start",
            reply_markup=kb_main(is_admin)
        )

    # ---------- ADMIN: create question ----------
    if st.get("stage") == "admin_create_title":
        if uid not in ADMIN_IDS:
            STATE.pop(uid, None)
            return await m.answer("Нет доступа.")
        title = m.text.strip()
        if len(title) < 3:
            return await m.answer("Слишком коротко. Введите нормальный текст вопроса.")
        st["title"] = title
        st["stage"] = "admin_create_type"
        return await m.answer("Тип вопроса: NUM (число) или TIME (время HH:MM)? Введите NUM или TIME.")

    if st.get("stage") == "admin_create_type":
        if uid not in ADMIN_IDS:
            STATE.pop(uid, None)
            return await m.answer("Нет доступа.")
        qtype = m.text.strip().upper()
        if qtype not in {"NUM", "TIME"}:
            return await m.answer("Введите NUM или TIME.")
        st["qtype"] = qtype
        st["stage"] = "admin_create_step"
        if qtype == "NUM":
            return await m.answer("Введите шаг (например 1 или 0.5 или 0.1).")
        else:
            return await m.answer("Введите шаг в минутах (например 5 или 10 или 15).")

    if st.get("stage") == "admin_create_step":
        if uid not in ADMIN_IDS:
            STATE.pop(uid, None)
            return await m.answer("Нет доступа.")
        qtype = st["qtype"]
        title = st["title"]

        if qtype == "NUM":
            try:
                step = dec(m.text)
            except Exception:
                return await m.answer("Введите число для шага.")
            if step <= 0:
                return await m.answer("Шаг должен быть > 0.")
            step_str = str(step)
        else:
            if not m.text.strip().isdigit():
                return await m.answer("Введите целое число минут.")
            step_min = int(m.text.strip())
            if step_min <= 0 or step_min > 240:
                return await m.answer("Шаг должен быть в разумных пределах (1..240).")
            step_str = str(step_min)

        qid = await create_question(title, qtype, step_str)
        STATE.pop(uid, None)
        is_admin = uid in ADMIN_IDS
        return await m.answer(
            f"✅ Вопрос создан и открыт.\n\n#{qid}: {title}\nТип: {qtype}\nШаг: {step_str}\n\n"
            f"Ставки принимаются до публикации факта.",
            reply_markup=kb_main(is_admin)
        )

    # ---------- ADMIN: settle question (enter fact) ----------
    if st.get("stage") == "admin_settle_enter_fact":
        if uid not in ADMIN_IDS:
            STATE.pop(uid, None)
            return await m.answer("Нет доступа.")

        qid = st["qid"]
        q = await get_question(qid)
        if not q or q[4] != "OPEN":
            STATE.pop(uid, None)
            return await m.answer("Вопрос уже закрыт или не найден.")

        _, title, qtype, step, status, _fact = q

        if qtype == "NUM":
            try:
                fact = dec(m.text)
            except Exception:
                return await m.answer("Введите число.")
            fact_str = str(fact)
        else:
            step_min = int(step)
            mins = to_minutes_hhmm(m.text, step_min=1)  # факт разрешаем любой HH:MM, без кратности шагу
            if mins is None:
                return await m.answer("Введите время в формате HH:MM.")
            fact_str = str(mins)

        # Закрываем вопрос
        await settle_question(qid, fact_str)

        # Считаем и рассылаем результаты всем, кто ставил
        bets = await get_question_bets(qid)
        if not bets:
            STATE.pop(uid, None)
            is_admin = uid in ADMIN_IDS
            return await m.answer(f"Вопрос #{qid} закрыт. Ставок не было.", reply_markup=kb_main(is_admin))

        # Готовим общие данные для уникальности (по вопросу)
        if qtype == "NUM":
            forecasts = [Decimal(b[1]) for b in bets]
            fact_val = Decimal(fact_str)
            step_d = Decimal(step)
        else:
            forecasts = [Decimal(int(b[1])) for b in bets]
            fact_val = Decimal(int(fact_str))
            step_d = Decimal(int(step))

        W = choose_cluster_width_W(step_d, forecasts)

        # Предподсчёт k по bin
        bins = [compute_bin(v, W) for v in forecasts]
        N = len(bins)
        bin_counts = {}
        for b in bins:
            bin_counts[b] = bin_counts.get(b, 0) + 1

        # Рассылка каждому
        for user_id, fv, points in bets:
            user_forecast = Decimal(fv) if qtype == "NUM" else Decimal(int(fv))
            err = (user_forecast - fact_val).copy_abs()

            # персональный допуск точности: 10% от прогноза пользователя, но не меньше step
            T_user = (abs(user_forecast) * TOLERANCE_RATE)
            if T_user < step_d:
                T_user = step_d
            # точность
            acc = k_accuracy(err, T_user)

            # уникальность
            b = compute_bin(user_forecast, W)
            k = bin_counts.get(b, 0)
            ratio = Decimal(k) / Decimal(N) if N else Decimal("1")
            kuniq = k_unique_from_ratio(ratio)

            # выигрыш (сложность = 1.0 для универсальных вопросов)
            if acc == 0:
                w = Decimal("0")
            else:
                w = (Decimal(points) * acc * kuniq).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            # форматирование
            if qtype == "TIME":
                forecast_disp = minutes_to_hhmm(int(user_forecast))
                fact_disp = minutes_to_hhmm(int(fact_val))
                t_disp = f"±{int(T_user)} мин"
                err_disp = f"{int(err)} мин"
            else:
                forecast_disp = str(user_forecast)
                fact_disp = str(fact_val)
                # покажем допуск как число (округлим)
                t_disp = f"±{round_display(T_user,'0.1')}"
                err_disp = round_display(err, "0.1")

            msg = (
                f"📌 Итоги по вопросу #{qid}\n"
                f"{title}\n\n"
                f"Ваш прогноз: {forecast_disp}\n"
                f"Факт: {fact_disp}\n"
                f"Ошибка: {err_disp}\n"
                f"Ваш допуск (10%): {t_disp}\n\n"
                f"K_accuracy: {acc}\n"
                f"K_unique: {kuniq} (k={k}/N={N})\n"
                f"Очки: {points}\n"
                f"Итог: {w}"
            )
            try:
                await bot.send_message(user_id, msg)
            except Exception:
                # пользователь мог не открыть бота/запретить сообщения — в MVP просто игнорируем
                pass

        STATE.pop(uid, None)
        is_admin = uid in ADMIN_IDS
        return await m.answer(
            f"✅ Вопрос #{qid} закрыт, факт сохранён, уведомления участникам отправлены.",
            reply_markup=kb_main(is_admin)
        )

# ---------------- MAIN ----------------
async def main():
    await db_init()
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

