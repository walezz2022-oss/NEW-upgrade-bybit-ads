import asyncio
import os
import random
import json
import re
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import logging
from decimal import Decimal, ROUND_HALF_UP, ROUND_FLOOR, ROUND_CEILING, InvalidOperation
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.error import RetryAfter, Forbidden
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)
from config import TELEGRAM_TOKEN, ADMIN_IDS
import bybit
from bybit import (
    get_ad_details, get_my_ads, modify_ad, get_market_ads,
    get_btc_usdt_price, get_eth_usdt_price, get_token_usdt_price,
    get_max_float_pct, get_min_float_pct, currency_needs_ref,
    get_pending_orders, get_sell_orders, get_incoming_sell_orders, get_order_detail,
    get_counterparty_info, mark_order_paid,
    send_chat_message, get_payment_name, release_assets,
    set_active_account, get_active_account, get_all_accounts,
    get_chat_messages, post_new_ad, remove_ad,
    take_ad_offline, put_ad_online,
    get_user_payment_list,
    review_seller_cancel,
    validate_interval, validate_interval_seconds, validate_float_pct, MAX_ADS_PER_USER,
    get_min_price_gap,
)
from market_collector import (
    get_market_snapshot, browserbase_pair_key, start_market_collector,
    register_demand as _bb_register_demand,
    unregister_demand as _bb_unregister_demand,
)
from direct_market import (
    get_direct_market_snapshot, direct_market_pair_key, start_direct_market_collector,
    register_demand as _dm_register_demand,
    unregister_demand as _dm_unregister_demand,
)
import db
import subscription as sub
from admin_commands import (
    cmd_upgrade, cmd_downgrade, cmd_requests, cmd_listusers, cmd_userdata,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# ⏳ Browserbase/Decodo Market modes — cold-start grace period
# ─────────────────────────────────────────
# market_collector.py closes its shared Browserbase session the instant
# demand hits zero (to conserve the free plan's hour budget), so every
# start/restart of a browserbase_market ad pays a real ~10-15s cold start:
# new session create + CDP connect + page nav + first Bybit XHR. That is
# normal, not an error, so we don't alarm the user with a Telegram warning
# during this window — instead we poll quickly and quietly until either
# the price is ready or the grace period itself runs out.
# direct_market.py's collector (decodo_market mode) is always-on rather
# than demand-gated, but still has a short cold start right after the bot
# boots (first fetch per pair hasn't completed yet) — these same
# constants cover that window too.
BB_WARMUP_GRACE_SECONDS = int(os.getenv("BB_WARMUP_GRACE_SECONDS", "20") or 20)
BB_WARMUP_POLL_SECONDS  = 2

# ─────────────────────────────────────────
# 🖼️ Welcome banner image
# ─────────────────────────────────────────
BANNER_URL = "https://raw.githubusercontent.com/akinrinadeakinniyi401-dot/bybit-p2p-telegram-bot/main/photo_6017280178934975538_x.jpg"


async def _get_current_ip() -> str:
    import requests as _r
    for svc in ["https://api.ipify.org", "https://ifconfig.me/ip"]:
        try:
            return _r.get(svc, timeout=4).text.strip()
        except Exception:
            continue
    return "unknown"


# ─────────────────────────────────────────
# 🧠 Per-user session state — replaces ALL globals
# ─────────────────────────────────────────
# All P2P state (settings, ad_data, orders, toggles, tasks) is now stored
# per user inside a SessionState object from user_session.py.
# Globals below are ONLY kept for:
#   - _current_user_id / _current_plan_badge (display-only, refreshed per request)
from user_session import get_session, clear_session, get_all_sessions, SessionState, _default_extra_ad_slot

# Dedicated thread pool for ad modification calls (modify_ad via run_in_executor).
# Isolated from the default executor so order/chat monitor threads can never starve
# ad-update threads (and vice versa), which was causing Telegram timeouts under load.
_ad_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="ad_modify")

# Display-only — refreshed at the top of every button/command handler
_current_user_id    = 0
_current_plan_badge = "⚪ Free"

# Legacy admin-scope globals still used by admin-only features (single admin session)
# These are ONLY read/written when is_admin(uid) is True.
user_state: dict = {}   # admin input action state (non-admins use context.user_data)

def _s(uid: int) -> SessionState:
    """Shorthand: get the per-user session for uid."""
    sess = get_session(uid)
    # Ensure per-user slot field exists (backfill for sessions created before this patch)
    if not hasattr(sess, "selected_slot"):
        sess.selected_slot = 0   # 0 = slot 1, 1 = slot 2 (matches bybit._active_index values)
    return sess


def _get_user_slot(uid: int) -> int:
    """Return the active account slot index (0-based) for this specific user."""
    return _s(uid).selected_slot


def _get_user_slot_str(uid: int) -> str:
    """Return slot as 1-based string: '1' or '2'."""
    return str(_s(uid).selected_slot + 1)












# ── Fixed 8-ad layout ───────────────────────────────────────────────────
# Ad 1..Ad 8 are always present. Each Ad is tied to ONE market pair:
#   Ad 1-4 → BTC/USD   (Ad 1 = senior; Ad 2/3/4 sit -9/-18/-27 below it)
#   Ad 5   → BTC/NGN   (Quick Market allowed)
#   Ad 6-7 → ETH/USD   (Ad 7 sits -9 below Ad 6)
#   Ad 8   → USDT/USD  (Quick Market allowed)
# Slot index convention: -1 = Ad 1, 0..6 = Ad 2..Ad 8 (extra_ad_slots).
TOTAL_AD_SLOTS = 8
AD_SLOT_PAIRS = {
    -1: ("BTC",  "USD"),
     0: ("BTC",  "USD"),
     1: ("BTC",  "USD"),
     2: ("BTC",  "USD"),
     3: ("BTC",  "NGN"),
     4: ("ETH",  "USD"),
     5: ("ETH",  "USD"),
     6: ("USDT", "USD"),
}
QUICK_MARKET_SLOTS = {3, 6}                     # Ad 5 (BTC/NGN) and Ad 8 (USDT/USD)
NUDGE_BUTTON_SLOTS = {-1, 0, 1, 2, 4, 5}        # all BTC/USD and ETH/USD ads
QUICK_MARKET_PAIRS = {("BTC", "NGN"), ("USDT", "USD")}


def _slot_pair(slot_idx: int) -> tuple:
    """(token, currency) this Ad slot is restricted to."""
    return AD_SLOT_PAIRS.get(slot_idx, ("BTC", "USD"))


def _slot_pair_label(slot_idx: int) -> str:
    t, c = _slot_pair(slot_idx)
    return f"{t}/{c}"


def _ad_matches_slot_pair(slot_idx: int, ad_data: dict) -> bool:
    t, c = _slot_pair(slot_idx)
    return (ad_data.get("tokenId", "").upper() == t
            and ad_data.get("currencyId", "").upper() == c)


def _multi_ad(sess) -> bool:
    """True when more than one of the 8 ads is configured (has an Ad ID) —
    replaces the old 'user added extra slots' check."""
    _ensure_ad_slots(sess)
    return sum(1 for i in range(-1, TOTAL_AD_SLOTS - 1) if _ad_settings(sess, i).get("ad_id")) > 1


def _ensure_ad_slots(sess):
    """Guarantee all 7 extra slots (Ad 2..Ad 8) exist."""
    while len(sess.extra_ad_slots) < TOTAL_AD_SLOTS - 1:
        sess.extra_ad_slots.append(_default_extra_ad_slot())


def _valid_slot(sess, slot_idx: int) -> int:
    """Clamp a stale/out-of-range slot index back to -1 (Ad 1)."""
    _ensure_ad_slots(sess)
    if slot_idx != -1 and not (0 <= slot_idx < len(sess.extra_ad_slots)):
        return -1
    return slot_idx

def _ad_settings(sess, slot_idx: int) -> dict:
    slot_idx = _valid_slot(sess, slot_idx)
    return sess.settings if slot_idx == -1 else sess.extra_ad_slots[slot_idx]["settings"]

def _ad_data_of(sess, slot_idx: int) -> dict:
    slot_idx = _valid_slot(sess, slot_idx)
    return sess.ad_data if slot_idx == -1 else sess.extra_ad_slots[slot_idx]["ad_data"]

def _ad_running(sess, slot_idx: int) -> bool:
    slot_idx = _valid_slot(sess, slot_idx)
    if slot_idx == -1:
        return sess.refresh_running
    return sess.extra_ad_slots[slot_idx]["running"]

def _any_ad_running(sess) -> bool:
    """True if ANY of this user's 8 ads is running."""
    if sess.refresh_running:
        return True
    return any(slot.get("running") for slot in sess.extra_ad_slots)

def _set_ad_running(sess, slot_idx: int, val: bool):
    slot_idx = _valid_slot(sess, slot_idx)
    if slot_idx == -1:
        sess.refresh_running = val
    else:
        sess.extra_ad_slots[slot_idx]["running"] = val

def _set_ad_task(sess, slot_idx: int, task):
    slot_idx = _valid_slot(sess, slot_idx)
    if slot_idx == -1:
        sess.refresh_task = task
    else:
        sess.extra_ad_slots[slot_idx]["task"] = task


def _ip_error_already_notified(sess, slot_idx: int) -> bool:
    return getattr(sess, "ip_error_notified_by_slot", {}).get(slot_idx, False)

def _set_ip_error_notified(sess, slot_idx: int, val: bool):
    store = getattr(sess, "ip_error_notified_by_slot", None)
    if store is None:
        store = {}
        sess.ip_error_notified_by_slot = store
    store[slot_idx] = val

def _fiat_amount_error_already_notified(sess, slot_idx: int) -> bool:
    return getattr(sess, "fiat_amount_error_notified_by_slot", {}).get(slot_idx, False)

def _set_fiat_amount_error_notified(sess, slot_idx: int, val: bool):
    store = getattr(sess, "fiat_amount_error_notified_by_slot", None)
    if store is None:
        store = {}
        sess.fiat_amount_error_notified_by_slot = store
    store[slot_idx] = val

async def _handle_ad_ip_error(bot, chat_id: int, sess, slot_idx: int, ret_code, ret_msg) -> bool:
    """Detects Bybit's "Unmatched IP" rejection (10010) for the ad price
    bot — fast-chase and the scheduled cycle both hit this identically
    whenever the account's IP genuinely isn't whitelisted for this call,
    whether that's because it was never whitelisted, or because a
    Permanent IP approval just expired/got rejected and Render's IP was
    never re-added. Previously this was either silent (fast-chase just
    logged a warning and moved on) or simply never checked at all (the
    scheduled cycle kept retrying every interval forever with no
    detection whatsoever) — this brings both in line with order monitor's
    existing, working pattern: ONE notification, then stop this slot
    until the user manually restarts it, rather than silently burning
    budget/cycles against an IP problem that won't resolve itself.
    Returns True if this WAS an IP error (caller should stop processing
    immediately) — False otherwise (do nothing, not this kind of error).
    """
    if ret_code != 10010 and "IP" not in str(ret_msg).upper():
        return False
    if _ip_error_already_notified(sess, slot_idx):
        _set_ad_running(sess, slot_idx, False)
        return True
    _set_ip_error_notified(sess, slot_idx, True)
    label = _ad_slot_label(slot_idx)
    ip = await _get_current_ip()
    text = (
        f"🚫 <b>{label} — Bybit IP Whitelist Error (10010)</b>\n\n"
        f"Your API key isn't whitelisted for this server's IP.\n\n"
        f"👉 Add <code>{_esc(ip)}</code> to your Bybit API key's IP whitelist:\n"
        "Bybit → Account → API Management → Edit Key → Bind IP\n\n"
        f"⚠️ {label} has been <b>stopped</b> to avoid wasting requests. Restart it after whitelisting the IP."
    )
    try:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
    except Exception as e:
        logger.warning(f"[IPError] Failed to notify user {chat_id} for {label}: {e}")
    _set_ad_running(sess, slot_idx, False)
    return True

def _ad_current_price(sess, slot_idx: int) -> Decimal:
    slot_idx = _valid_slot(sess, slot_idx)
    return sess.current_price if slot_idx == -1 else sess.extra_ad_slots[slot_idx]["current_price"]

def _ceiling_ref(sess, slot_idx: int):
    """Last real ceiling Bybit has confirmed for THIS ad slot (see
    _set_ad_current_price, which keeps this in sync with reality)."""
    return getattr(sess, "last_known_ceiling_by_slot", {}).get(slot_idx)

def _set_ceiling_ref(sess, slot_idx: int, value):
    store = getattr(sess, "last_known_ceiling_by_slot", None)
    if store is None:
        store = {}
        sess.last_known_ceiling_by_slot = store
    store[slot_idx] = value

def _pending_ceiling(sess, slot_idx: int):
    """A discovered-but-not-yet-posted ceiling for THIS ad slot (ran out of
    budget, or the post itself was rejected) — worth a plain 1-call retry
    before ever spending 2 calls on a fresh probe."""
    return getattr(sess, "pending_ceiling_by_slot", {}).get(slot_idx)

def _set_pending_ceiling(sess, slot_idx: int, value):
    store = getattr(sess, "pending_ceiling_by_slot", None)
    if store is None:
        store = {}
        sess.pending_ceiling_by_slot = store
    store[slot_idx] = value

def _last_ceiling_attempt(sess, slot_idx: int):
    """The raw (pre-collision) ceiling value THIS slot last actually acted
    on — attempted a post against, whether it succeeded, got nudged, or
    was rejected. Used to tell "the raw ceiling genuinely moved higher
    since last time" apart from "my own posted price sits below the raw
    ceiling because collision-avoidance deliberately nudged it down" —
    the two look identical if you only compare last_known vs cur_p, which
    is what caused the endless-identical-repost bug: an ad nudged $10-17
    below the true ceiling for collision reasons would see that same
    static gap as "room to catch up" on EVERY poll forever, re-triggering
    a retry that just re-posts the exact same number again and again."""
    return getattr(sess, "last_ceiling_attempt_by_slot", {}).get(slot_idx)

def _set_last_ceiling_attempt(sess, slot_idx: int, value):
    store = getattr(sess, "last_ceiling_attempt_by_slot", None)
    if store is None:
        store = {}
        sess.last_ceiling_attempt_by_slot = store
    store[slot_idx] = value

def _required_fresh_move(sess, slot_idx: int, currency: str, token: str, reference_price, fallback_gap):
    """How much the underlying raw ceiling needs to have moved past what
    this slot already probed/resolved a post from before it's worth
    trying again — matched to THIS ad's own real spacing requirement
    instead of one flat number shared by every slot:
      - Senior-most ad (nothing to defer to, e.g. Ad 1): no extra
        requirement beyond the normal fast-chase reaction gap
        (fallback_gap, $0.50/₦1,500) — it can react to any real move.
      - One ad below it (e.g. Ad 2): needs its own real spacing to have
        opened up — $9/₦12,600 — before a re-probe can possibly produce
        a genuinely different final price.
      - Two ads below (e.g. Ad 3): $18/₦25,200.
    A flat number was wrong in both directions: too loose for Ad 1
    (which doesn't need this extra gate at all) and too tight for
    Ad 2/Ad 3, whose own required spacing is bigger than any single flat
    guess — confirmed in production by repeated wasted probe-and-repost
    cycles for a price that could never have been different once you
    account for the ad's own $9/$18 offset.
    """
    resolved, _ = _resolve_price_collision(sess, slot_idx, currency, token, reference_price)
    own_gap = reference_price - resolved
    return own_gap if own_gap > 0 else fallback_gap

# ─────────────────────────────────────────────────────────────────────────
# Cycle-restart-on-fast-chase (floating mode only)
# ─────────────────────────────────────────────────────────────────────────
# Bybit resets its own "5 min to auto-offline" countdown on a USD/NGN ad
# every time the ad is successfully modified — not just on the bot's own
# scheduled cycle. So whenever fast-chase pushes a price up between
# scheduled cycles, the bot's own next-scheduled-cycle timer for THAT slot
# should restart too, the same way Bybit's real countdown just did —
# otherwise the bot's schedule silently drifts out of sync with what
# Bybit is actually counting down, and a scheduled cycle can fire only a
# couple of minutes after a fast-chase post instead of a full interval
# later.
#
# Tracked PER SLOT (Ad 1/2/3 each have their own independent Bybit-side
# countdown, same convention as the modify budget / ceiling ref above) and
# uses the asyncio loop's own monotonic clock — the same clock
# auto_update_loop already schedules its waits against — so there's no
# wall-clock/monotonic-clock mismatch between the two tasks.
def _last_fast_modify_ts(sess, slot_idx: int):
    """Loop-clock timestamp of this slot's last successful fast-chase
    post, or None if fast-chase hasn't posted anything for it yet since
    this loop iteration's wait phase began watching."""
    return getattr(sess, "last_fast_modify_ts_by_slot", {}).get(slot_idx)

def _touch_last_fast_modify(sess, slot_idx: int):
    store = getattr(sess, "last_fast_modify_ts_by_slot", None)
    if store is None:
        store = {}
        sess.last_fast_modify_ts_by_slot = store
    store[slot_idx] = asyncio.get_event_loop().time()


async def _resync_price_from_bybit(ad_id: str, creds: dict, sess, slot_idx: int, fallback_price, tag: str):
    """
    A 90043 rejection ("price differs from your existing ad by less than
    0%") only tells us the price we just tried was too close to what's
    live — it does NOT confirm our guess IS what's live. The old recovery
    here just assumed the rejected submission was now the real price and
    wrote it straight into the tracked state. At the old $3/₦5,000 gaps
    that assumption was usually close enough not to matter; at tighter
    thresholds a wrong guess drifts the tracked price away from Bybit's
    actual value, and every later decision compounds the error — this is
    exactly what produced the Ad 2/Ad 3 stall in production logs (endless
    90043s, never an accepted modify, while Ad 1 kept succeeding).

    So instead of guessing, ask Bybit directly via get_ad_details — one
    extra read call, only on the rejection path (never on every poll),
    cheap insurance against that whole class of bug. Falls back to the
    old guess only if the lookup itself fails, so a transient API error
    doesn't leave the tracked price stale forever.
    """
    try:
        details = await asyncio.get_event_loop().run_in_executor(
            _ad_executor, partial(get_ad_details, ad_id, creds=creds)
        )
        if details.get("retCode", details.get("ret_code", -1)) == 0:
            real_price = details.get("result", {}).get("price")
            if real_price not in (None, ""):
                real_price = Decimal(str(real_price))
                _set_ad_current_price(sess, slot_idx, real_price)
                logger.info(f"{tag} confirmed real live price from Bybit directly: {real_price}")
                return real_price
    except Exception as e:
        logger.warning(f"{tag} get_ad_details lookup failed during 90043 resync — {e}")
    _set_ad_current_price(sess, slot_idx, fallback_price)
    logger.warning(f"{tag} get_ad_details lookup unavailable — falling back to guess-sync at {fallback_price}")
    return fallback_price


def _set_ad_current_price(sess, slot_idx: int, price, collision_adjusted: bool = False):
    slot_idx = _valid_slot(sess, slot_idx)
    if slot_idx == -1:
        sess.current_price = price
    else:
        sess.extra_ad_slots[slot_idx]["current_price"] = price
    # Keep the fast-chase chase-ceiling reference in sync with reality, for
    # ANY ad slot (Ad 1/2/3, not just Ad 1). This is the fix for a real
    # staleness bug: the scheduled cycle can push an ad's price up or down
    # independently of fast-chase, and if the ceiling reference weren't
    # refreshed here too, fast-chase would keep comparing fresh spot prices
    # against a stale number that no longer reflects what's actually live
    # on Bybit. Every confirmed price change, from ANY source, is new
    # information about reality and resets this baseline for that slot.
    #
    # EXCEPTION — collision_adjusted=True: this price was deliberately
    # nudged BELOW the real known ceiling to stay clear of a senior ad
    # (Ad 2 under Ad 1, Ad 3 under Ad 1/2), so `price` here is NOT a fresh
    # statement about where the true ceiling is — it's a intentionally
    # lower number. Overwriting ceiling_ref down to it was the actual bug
    # behind Ad 2/Ad 3 endlessly re-posting the exact same price every
    # poll: next poll would see (true ceiling we just threw away) minus
    # (the nudged price we kept) as if it were fresh room to catch up,
    # when it was really just the static collision gap, forever re-
    # triggering a retry that resolves to the same number every time.
    # Leave the real discovered ceiling in place instead so future polls
    # only re-trigger when it genuinely rises further.
    if not collision_adjusted:
        _set_ceiling_ref(sess, slot_idx, price)
    _set_pending_ceiling(sess, slot_idx, None)

def _pick_ad_copy_price(competing: list):
    """Selection rule for Ad Copy: within an already-filtered (self-ads
    removed, wrong token/currency removed), already-ordered window of
    candidates (Bybit's own page-1 listing order — never re-sorted or
    re-ranked by us), copy exactly the price of the FIRST ad in that
    window — i.e. whatever ad sits at position #1 on page 1 once your
    own ad(s) are excluded. Bybit's /v5/p2p/item/online has no sort
    parameter at all, so items[] on page 1 already comes back in the
    exact "price highest to lowest" order a person sees live on the
    site; the only thing we do on top of that is skip past any of your
    own ad IDs.

    Previously this picked whichever price was the MOST COMMON across
    the window, to guard against a single outlier sitting at #1. That
    "clustering" behaviour is intentionally removed — it could copy a
    different ad than the one actually in position #1 on the real page,
    which no longer matches what the market shows. Copy Range (1-10 /
    1-20) still controls how deep to look ONLY for the purpose of
    skipping past your own ad(s); it no longer affects price selection.

    Returns (chosen_price_str, chosen_item) or (None, None) if the
    window is empty.
    """
    if not competing:
        return None, None
    top = competing[0]
    return str(top.get("price", "")), top


def _pick_ad_copy_price_windowed(combined: list):
    """Selection rule for the USDT/USD deep-window Ad Copy comparison.

    Given a deep window of market ads (ranks 1-300), choose whichever
    price appears MOST OFTEN within it — a price several independent ads
    have converged on is treated as a more reliable signal of the "real"
    market price than whatever sits at position #1, which can be a single
    outlier or a boosted/stale listing.

    JUNK PRICES ARE SKIPPED — some ads park at an obviously-fake decoy
    price (1 or below, or exactly 1.2) just to sit in the listing; no
    matter how many ads happen to share one of those exact values, it's
    never treated as the real dominant price. Ranking instead moves on to the
    next most-common price down the list. E.g. if 1 appears 46 times,
    1.014 appears 30 times, and 1.017 appears 40 times, the bot skips the
    46-count junk price entirely and copies 1.017 (the next-highest
    genuine ranking, not just whatever beats the junk price).

    Ties among genuine (non-junk) prices sharing the top frequency are
    broken by whichever appears EARLIEST in `combined` — i.e. Bybit's own
    return order (closer to rank 1 wins ties over ranks closer to 300).

    Returns (chosen_price_str, chosen_item) or (None, None) if the
    combined set is empty or every distinct price found is junk.
    """
    if not combined:
        return None, None
    from collections import Counter
    prices = [str(it.get("price", "")) for it in combined]
    counts = Counter(prices)
    first_seen = {}
    for idx, p in enumerate(prices):
        first_seen.setdefault(p, idx)
    # Rank distinct prices by frequency (highest first), ties broken by
    # earliest occurrence in `combined`.
    ranked = sorted(counts.keys(), key=lambda p: (-counts[p], first_seen[p]))
    for p in ranked:
        if _is_ad_copy_junk_price(p):
            logger.info(
                f"[AdCopy] skipping junk price {p} ({counts[p]} occurrence(s)) — "
                f"moving to next-ranked price"
            )
            continue
        idx = first_seen[p]
        return p, combined[idx]
    return None, None   # every distinct price found was junk


# Decoy/junk prices some USDT/USD ads park at just to sit in the listing —
# never realistic values for this pair, so never eligible to be copied no
# matter how many ads share one of them. See _pick_ad_copy_price_windowed.
# Anything at or below 0.99 is junk (covers 1, 0.9, 0.8, and everything
# else down that far), plus 1.2 specifically as an extra known decoy.
# Applies to USDT AD1, USDT AD2, and USDT AD3 alike (same function used by
# both the single-ad picker and the 3-way rank-rotation engine). Anything
# at or below 1 is junk — no legitimate USDT/USD price sits at or under
# parity — plus 1.2 specifically as an extra known decoy above parity.
_AD_COPY_JUNK_THRESHOLD = Decimal("1")
_AD_COPY_JUNK_EXTRA_VALUES = {Decimal("1.2")}

def _is_ad_copy_junk_price(price_str: str) -> bool:
    try:
        val = Decimal(price_str)
    except Exception:
        return False
    return val <= _AD_COPY_JUNK_THRESHOLD or val in _AD_COPY_JUNK_EXTRA_VALUES


def _market_ads_query_side(ad_data: dict) -> str:
    """Which 'side' value to pass to /v5/p2p/item/online when looking for
    ads that actually compete with this one.

    REVERTED from the earlier "flip" logic. Live test (2026-09-13) showed
    querying the FLIPPED side returned a stale, wrong-looking cluster of
    prices (e.g. Adi_Melo at 0.800 when the live site showed ~0.98) —
    the flip was not correct in production despite the earlier note
    claiming it was confirmed empirically. Per Bybit's own docs, "side"
    on /v5/p2p/item/online is just 0=buy / 1=sell for the ads being
    RETURNED, with no reference to "your own ad" at all — so the ads
    that actually compete with yours are simply the ones posted with the
    SAME side value as your own ad. No flipping.
    """
    return str(ad_data.get("side", "0")).strip()


async def _fetch_market_ads_up_to(token: str, currency: str, side: str,
                                   total: int, creds) -> list:
    """Fetch up to `total` market ads for the "View Market Ads List"
    diagnostic, paging in chunks of 100 (Bybit's per-page cap) and
    concatenating in the SAME order Bybit returns them (page 1's order is
    the live "price highest to lowest" ordering — never re-sort here).
    Stops early if a page comes back short (no more ads available).
    """
    items: list = []
    page = 1
    PAGE_CAP = 100
    while len(items) < total:
        remaining  = total - len(items)
        fetch_size = min(PAGE_CAP, max(remaining, 10))
        resp = await asyncio.get_event_loop().run_in_executor(
            _ad_executor, get_market_ads, token, currency, side, page, fetch_size, creds
        )
        page_items = (resp.get("result") or {}).get("items", []) if isinstance(resp, dict) else []
        if not page_items:
            break
        items.extend(page_items)
        if len(page_items) < fetch_size:
            break  # Bybit ran out of ads before filling the page
        page += 1
    return items[:total]


async def _fetch_market_ads_range(token: str, currency: str, side: str,
                                   start_rank: int, end_rank: int, creds) -> list:
    """Fetch ONLY the market-ad ranks in [start_rank, end_rank] (1-based,
    inclusive), using Bybit's own page numbers DIRECTLY rather than always
    accumulating from page 1 — e.g. ranks 301-600 needs only pages 4-6
    (3 requests), not 6 requests starting from page 1.

    This is what makes Merchant Watch's sticky-range preference actually
    fast: once a cycle knows the merchant lives in 301-600, EVERY cycle
    after that fetches only those 3 pages, not both halves (6 pages)
    every time — cutting fetch time roughly in half for the common case
    where the merchant hasn't moved.
    """
    PAGE_CAP    = 100
    first_page  = (start_rank - 1) // PAGE_CAP + 1
    last_page   = (end_rank   - 1) // PAGE_CAP + 1
    items: list = []
    for page in range(first_page, last_page + 1):
        resp = await asyncio.get_event_loop().run_in_executor(
            _ad_executor, get_market_ads, token, currency, side, page, PAGE_CAP, creds
        )
        page_items = (resp.get("result") or {}).get("items", []) if isinstance(resp, dict) else []
        items.extend(page_items)
        if len(page_items) < PAGE_CAP:
            break   # Bybit ran out of ads before filling the page
    # `items` currently spans ranks [(first_page-1)*100+1 .. ...] — slice
    # down to the EXACT window the caller asked for.
    offset_start = start_rank - ((first_page - 1) * PAGE_CAP) - 1
    offset_end   = offset_start + (end_rank - start_rank + 1)
    return items[max(offset_start, 0):offset_end]


def _price_str_equal(a, b) -> bool:
    """True if two price-like values represent the SAME number, even if
    their exact text differs (trailing zeros, '1200' vs '1200.00', etc).
    Comparing prices as raw text is what caused USDT AD2/AD3 to get
    re-edited every single cycle even though the price hadn't actually
    changed: different competing ads (or the same ad reported with
    different formatting between polls) can list numerically-identical
    prices with cosmetically different strings, and a raw `==` treats
    those as a real change. Falls back to exact string equality only if
    either value truly isn't parseable as a number."""
    if a is None or b is None:
        return False
    try:
        return Decimal(str(a)) == Decimal(str(b))
    except (InvalidOperation, ValueError, TypeError):
        return str(a) == str(b)


def _rank_ad_copy_prices(combined: list, top_n: int) -> list:
    """Like _pick_ad_copy_price_windowed but returns up to `top_n` DISTINCT
    non-junk prices, ranked by occurrence count (highest first, ties
    broken by earliest occurrence in `combined`). Used by the USDT triad
    coordinator, which needs the whole top-N ranking at once (not just
    #1) to decide which participant holds which rank.

    Groups by NUMERIC VALUE (Decimal), not raw string — two ads listing
    the "same" price with different formatting (e.g. "1200" vs "1200.00")
    are the same price on Bybit and must be counted together, or a
    genuinely-leading price can get split across its cosmetic variants
    and under-counted/mis-ranked.
    """
    if not combined:
        return []
    from collections import Counter
    raw_prices  = [str(it.get("price", "")) for it in combined]
    decimal_of  = {}   # Decimal value -> first raw string seen for it (kept for display/posting)
    values      = []
    for p in raw_prices:
        try:
            d = Decimal(p)
        except (InvalidOperation, ValueError):
            continue
        values.append(d)
        decimal_of.setdefault(d, p)
    counts = Counter(values)
    first_seen = {}
    for idx, d in enumerate(values):
        first_seen.setdefault(d, idx)
    ranked = sorted(counts.keys(), key=lambda d: (-counts[d], first_seen[d]))
    result = []
    for d in ranked:
        p = decimal_of[d]
        if _is_ad_copy_junk_price(p):
            continue
        result.append(p)
        if len(result) >= top_n:
            break
    return result












def _is_usdt_usd_ad(ad_data: dict) -> bool:
    """True if this ad's pair is USDT/USD — the one pair allowed to run on
    a seconds-level interval (floor 5s, see MIN_USDT_INTERVAL_SECONDS)."""
    return (ad_data.get("tokenId", "").upper() == "USDT"
            and ad_data.get("currencyId", "").upper() == "USD")


def _interval_floor_secs(s: dict, ad_data: dict):
    """The seconds-level interval floor for this ad, or None if this ad
    uses the normal whole-minutes floor instead.
      • USDT/USD               → 5s
      • BTC/NGN in ad_copy mode → 10s
    Floating/fixed BTC/NGN ads are NOT included — they keep the 2-minute
    floor, since they submit an edit every cycle rather than only on a
    genuine price change.
    """
    if _is_usdt_usd_ad(ad_data):
        return bybit.MIN_USDT_INTERVAL_SECONDS
    if _is_btc_ngn_ad(ad_data) and s.get("mode") in ("ad_copy", "browserbase_market", "decodo_market"):
        return bybit.MIN_BTC_NGN_ADCOPY_INTERVAL_SECONDS
    return None


def _ad_interval_seconds(s: dict, ad_data: dict) -> int:
    """This ad's update interval in SECONDS.

    Most ads store `interval` as whole MINUTES (floor 2), so seconds =
    interval * 60. The copy modes (USDT/USD, and BTC/NGN in ad_copy) also
    support an `interval_secs` field holding a raw seconds value, which
    takes precedence when set and valid for that ad's floor. Keeping the
    old minutes field untouched means every other ad, and any copy-mode ad
    configured before this existed, keeps behaving exactly as before.
    """
    floor = _interval_floor_secs(s, ad_data)
    if floor is not None:
        raw = s.get("interval_secs")
        if raw:
            try:
                val = int(raw)
                if val >= floor:
                    return val
            except (TypeError, ValueError):
                pass
    try:
        return max(1, int(s.get("interval", 2))) * 60
    except (TypeError, ValueError):
        return 120


def _ad_interval_label(s: dict, ad_data: dict) -> str:
    """Human-readable interval for menus/status text."""
    secs = _ad_interval_seconds(s, ad_data)
    if secs % 60 == 0:
        return f"{secs // 60} min"
    return f"{secs} sec"


def _is_btc_ngn_ad(ad_data: dict) -> bool:
    """True if this ad's pair is BTC/NGN — the pair that uses prefix-based
    ("Close Price Range") Ad Copy rather than dominant-price Ad Copy."""
    return (ad_data.get("tokenId", "").upper() == "BTC"
            and ad_data.get("currencyId", "").upper() == "NGN")


def _find_btc_ngn_ad_copy_slot(sess, exclude_slot=None):
    """Slot index of the ONE ad already set to ad_copy mode on BTC/NGN, or
    None. Used to enforce the single-slot rule — two BTC/NGN ad_copy ads
    would chase the same prefix in the same market list and fight each
    other for the same price."""
    for i in range(-1, TOTAL_AD_SLOTS - 1):
        if exclude_slot is not None and i == exclude_slot:
            continue
        s = _ad_settings(sess, i)
        if s.get("mode") != "ad_copy":
            continue
        if _is_btc_ngn_ad(_ad_data_of(sess, i)):
            return i
    return None


def _pick_close_range_price(items: list, prefix: str, own_ids: set = None, top_range: int = 1):
    """BTC/NGN "Close Price Range" pick.

    The user saves a leading-digits prefix (e.g. 1047435). Among all
    fetched market ads whose price starts with those exact digits, this
    ranks them highest-to-lowest and returns the `top_range`'th one —
    1 = the leading (highest) price in that band, 2 = second-highest,
    3 = third-highest, and so on.

    e.g. prefix 100200 against 100200.9, 100200.5, 100200.3, 100200.03:
      top_range=1 -> 100200.9  (leading)
      top_range=2 -> 100200.5  (2nd highest)
      top_range=3 -> 100200.3  (3rd highest)

    Comparison is done on the digits of the price with any decimal point
    stripped, so "1047435" matches 104743520.00 regardless of where the
    decimal falls. Returns (price_str, item) or (None, None) if there
    aren't at least `top_range` matches in the band.
    """
    prefix = str(prefix or "").strip()
    if not prefix or not items:
        return None, None
    own_ids = own_ids or set()
    try:
        top_range = max(1, int(top_range))
    except (TypeError, ValueError):
        top_range = 1

    matches = []   # [(Decimal price, item), ...]
    for it in items:
        if str(it.get("id", "")) in own_ids:
            continue          # never match against the user's own ad
        price_str = str(it.get("price", "") or "").strip()
        if not price_str:
            continue
        digits = price_str.replace(".", "").replace(",", "")
        if not digits.startswith(prefix):
            continue
        try:
            val = Decimal(price_str)
        except Exception:
            continue
        matches.append((val, it))

    if len(matches) < top_range:
        return None, None

    matches.sort(key=lambda pair: pair[0], reverse=True)
    picked_val, picked_item = matches[top_range - 1]
    return str(picked_item.get("price")), picked_item


def _ad_copy_range_n(s: dict) -> int:
    """How many page-1 positions deep to look — ONLY to skip past your
    own ad id(s), never to change which price gets picked (that's always
    strictly position #1 of whatever remains after skipping your own
    ads — see _pick_ad_copy_price). Two choices: 1-10 or 1-20.

    Stored setting values are "top10" / "top20". Old saved values from
    before this change ("top5" / "top10") are migrated in place so
    existing users don't silently keep the old, narrower depth:
    legacy "top5" (was depth 5)  → depth 10 (new first option)
    legacy "top10" (was depth 10) → depth 20 (new second option)
    """
    v = s.get("ad_copy_range", "top10")
    if v == "top20":
        return 20
    if v == "top10":
        return 10
    if v == "top5":       # legacy value
        return 10
    return 10             # unknown/legacy fallback


def _ad_copy_range_label(v: str) -> str:
    return "1 - 20" if v in ("top20",) else "1 - 10"


def _ad_slot_label(slot_idx: int) -> str:
    return "Ad 1" if slot_idx == -1 else f"Ad {slot_idx + 2}"

def _increment_ad_failures(sess, slot_idx: int) -> int:
    slot_idx = _valid_slot(sess, slot_idx)
    if slot_idx == -1:
        sess.consecutive_failures += 1
        return sess.consecutive_failures
    slot = sess.extra_ad_slots[slot_idx]
    slot["consecutive_failures"] += 1
    return slot["consecutive_failures"]

def _reset_ad_failures(sess, slot_idx: int):
    slot_idx = _valid_slot(sess, slot_idx)
    if slot_idx == -1:
        sess.consecutive_failures = 0
    else:
        sess.extra_ad_slots[slot_idx]["consecutive_failures"] = 0
    # A successful post or a manual (re)start both mean whatever IP issue
    # existed before is either resolved or the user is trying again —
    # either way, a FUTURE ip error deserves a fresh notification, not
    # permanent silence from a stale flag set the last time this happened.
    _set_ip_error_notified(sess, slot_idx, False)
    _set_fiat_amount_error_notified(sess, slot_idx, False)


# ─────────────────────────────────────────
# Fast-chase modify budget (Ad 1, single-ad, floating mode only)
# Bybit's own DOCUMENTED limit is "a single advertisement can be modified
# no more than 10 times within 5 minutes." This is set to 60 — above that
# documented limit — by explicit request (raised 10 → 20 → 40 → 60 over
# time (now 100 per ad), each time by request, with no observed real Bybit-side rate-limit
# rejection at the previous value). This constant no longer acts as real
# protection against Bybit's own rate limiting: calls beyond whatever
# Bybit actually enforces will simply get rejected by Bybit itself (handled
# as an ordinary failure, same as any other rejection). It still exists so
# every part of the codebase shares one consistent, adjustable number.
_FAST_CHASE_BUDGET       = 100
_FAST_CHASE_WINDOW_SECS  = 300

# Sentinel ret_code used ONLY internally when a scheduled cycle skips its
# own post because the shared modify budget is exhausted (protecting
# Bybit's real 10-per-5-minutes limit). _handle_ad_cycle_failure treats
# this as a soft skip, never as a real Bybit-side failure.
_BUDGET_COOLDOWN = "BUDGET_COOLDOWN"

# How often the fast-chase price check runs while waiting out the rest of
# the scheduled interval. Was implicitly 10s (tick counter % 10), then 8s.
# Dropped to 5s for faster reaction to price moves — this only affects how
# often we CHECK, not how many edits we're allowed (still capped by
# _FAST_CHASE_BUDGET/_FAST_CHASE_WINDOW_SECS above, shared with the
# scheduled cycle, so the 8-edits-per-5-minutes ceiling is unchanged).
# Safe at current scale: this poll only drives calls to Bybit's PUBLIC,
# unauthenticated price endpoint (get_token_usdt_price), shared by IP
# across all users, limited to 600 req/5s (120 req/s) there. Worst case
# today — under 20 users, all 3 ad slots each, all floating — is ~60
# requests per tick, i.e. ~12 req/s at 5s polling: comfortably inside the
# limit even accounting for ticks landing in sync across users (bursty
# rather than smoothed). Re-check this math before dropping it further or
# if concurrent active floating ad slots grow well past ~100-150.
_FAST_CHASE_POLL_SECS    = 5

# ─────────────────────────────────────────────────────────────────────────
# Shared per-user fast-chase coordinator
# ─────────────────────────────────────────────────────────────────────────
# Previously each ad slot's auto_update_loop ran its OWN independent
# fast-chase timer, started whenever THAT specific ad was activated. With
# 2-3 ads on the same pair, those timers drifted out of phase with each
# other — Ad 1 might be 3 seconds into its own 8-second window while Ad 2
# was 7 seconds in, so a market move could get picked up by one ad several
# seconds (or, if a user activated them minutes apart, much longer) before
# a sibling even looked. That's what read as the bot "forgetting" to
# fast-chase Ad 2/3 and only reacting for Ad 1.
#
# One coordinator task per user now owns ALL of that user's fast-chase
# polling: a single timer, one tick every _FAST_CHASE_POLL_SECS, checking
# every currently-active floating-mode ad slot for that user IN THE SAME
# TICK, always in Ad 1 → Ad 2 → Ad 3 order. A price move now reaches every
# active ad within the same few-second window instead of drifting apart,
# and there's no separate per-ad clock that can silently fall out of sync.
_fast_chase_coordinators: dict = {}   # chat_id -> asyncio.Task


def _ensure_fast_chase_coordinator(bot, chat_id):
    """Start the shared fast-chase coordinator for this user if it isn't
    already running. Cheap and safe to call every time any ad slot for
    this user (re)activates — a coordinator already running for this
    chat_id is left alone."""
    existing = _fast_chase_coordinators.get(chat_id)
    if existing is not None and not existing.done():
        return
    task = asyncio.create_task(_fast_chase_coordinator(bot, chat_id))
    _fast_chase_coordinators[chat_id] = task


async def _fast_chase_coordinator(bot, chat_id):
    """
    One shared timer, all of this user's active floating-mode ad slots
    checked on every tick, always Ad 1 → Ad 2 → Ad 3. Exits once none of
    this user's ad slots are running; a later ad activation starts a fresh
    coordinator via _ensure_fast_chase_coordinator.
    """
    sess = _s(chat_id)
    logger.info(f"[FastChase] coordinator started for user {chat_id}")
    try:
        while True:
            any_running = False
            for slot_idx in range(-1, TOTAL_AD_SLOTS - 1):
                # Only ever look at a slot index that actually exists.
                # _ad_running/_ad_settings/_ad_data_of all route through
                # _valid_slot(), which — for a DIFFERENT, legitimate reason
                # (gracefully handling a stale UI reference to a slot that
                # was just removed) — silently CLAMPS an out-of-range index
                # back down to -1 (Ad 1) rather than signalling "doesn't
                # exist". That's fine for a one-off UI lookup, but here it
                # meant a user with only Ad 1 + Ad 2 running still had
                # slot_idx=1 ("Ad 3") checked every tick, silently aliased
                # to Ad 1's own settings/ad_id/running-flag — so the
                # coordinator ran a SECOND, fully independent fast-chase
                # process against Ad 1's own physical ad, mislabeled "Ad 3",
                # each treating the other as a sibling to dodge. That's what
                # was crashing Ad 1's price against itself. A slot index
                # only really exists if it's -1 (always) or strictly less
                # than how many extra slots this user has actually added.
                if slot_idx != -1 and slot_idx >= len(sess.extra_ad_slots):
                    continue
                if not _ad_running(sess, slot_idx):
                    continue
                any_running = True
                s = _ad_settings(sess, slot_idx)
                if s.get("mode") != "floating":
                    continue   # fixed-mode ads don't fast-chase
                ad_data = _ad_data_of(sess, slot_idx)
                if not ad_data:
                    continue
                try:
                    float_pct = float(s.get("float_pct") or 0)
                except (TypeError, ValueError):
                    float_pct = 0
                if float_pct <= 0:
                    continue
                creds = get_user_creds(chat_id)
                if not creds or not creds.get("key"):
                    continue
                _quant = Decimal("0.01")   # floating mode's precision, matches auto_update_loop
                try:
                    await _try_fast_chase(bot, chat_id, sess, slot_idx, ad_data, s, float_pct, creds, _quant)
                except asyncio.CancelledError:
                    raise
                except Exception as _fce:
                    # Belt-and-braces — _try_fast_chase already catches
                    # everything internally, but this guarantees one slot's
                    # failure can never take down the coordinator (and with
                    # it, every OTHER active ad's fast-chase) for this user.
                    logger.error(
                        f"[FastChase] coordinator uncaught error for user {chat_id} slot {slot_idx}: {_fce}",
                        exc_info=True
                    )
            if not any_running:
                break
            await asyncio.sleep(_FAST_CHASE_POLL_SECS)
    except asyncio.CancelledError:
        logger.info(f"[FastChase] coordinator cancelled for user {chat_id}")
        raise
    finally:
        _fast_chase_coordinators.pop(chat_id, None)
        logger.info(f"[FastChase] coordinator stopped for user {chat_id}")

def _modify_times(sess, slot_idx: int) -> list:
    """
    Per-ad-slot rolling list of recent modify_ad() call timestamps. Bybit
    enforces its 10-per-5-minutes limit PER AD (per ad_id), not per user —
    so each ad slot (-1/0/1 = Ad 1/2/3) needs its own independent budget.
    A single shared counter across all of a user's ads (the old design)
    would throttle Ad 2 and Ad 3 based on Ad 1's activity for no real
    reason, and vice versa.

    Stored as a dynamic dict attribute rather than something declared in
    SessionState (user_session.py) — Python objects allow this as long as
    they don't use __slots__, and it means this works without needing to
    touch that file at all.
    """
    store = getattr(sess, "modify_times_by_slot", None)
    if store is None:
        store = {}
        sess.modify_times_by_slot = store
    return store.setdefault(slot_idx, [])

def _can_modify_slot(sess, slot_idx: int, need: int = 1) -> bool:
    now   = datetime.now().timestamp()
    times = _modify_times(sess, slot_idx)
    times[:] = [t for t in times if now - t < _FAST_CHASE_WINDOW_SECS]
    return len(times) <= _FAST_CHASE_BUDGET - need

def _record_modify_slot(sess, slot_idx: int):
    _modify_times(sess, slot_idx).append(datetime.now().timestamp())

# Back-compat aliases — Ad 1 (slot -1) used to be the only ad allowed to
# fast-chase, so every existing call site uses these names. They're now
# thin wrappers over the general per-slot functions above, hardcoded to
# slot -1, so nothing else needs to change.
def _can_modify_ad1(sess, need: int = 1) -> bool:
    return _can_modify_slot(sess, -1, need)

def _record_modify_ad1(sess):
    _record_modify_slot(sess, -1)

def _fast_chase_lock(sess, slot_idx: int) -> asyncio.Lock:
    """
    Defense-in-depth: even though the real cause of overlapping runs on the
    same ad was a task-duplication race in the start/stop toggle (now
    fixed — see toggle_refresh), this guarantees two fast-chase checks for
    the SAME ad slot can never execute concurrently regardless of cause.
    Without it, two overlapping runs read/write the same cur_p and ceiling
    state with no ordering guarantee — exactly what produced near-
    simultaneous MODIFY calls with drifting internal price state and
    repeated 90043 "price unchanged" rejections in production logs.
    """
    store = getattr(sess, "fast_chase_locks", None)
    if store is None:
        store = {}
        sess.fast_chase_locks = store
    lock = store.get(slot_idx)
    if lock is None:
        lock = asyncio.Lock()
        store[slot_idx] = lock
    return lock


# Fast-chase-only gap thresholds. This is separate from get_min_price_gap
# (used everywhere else — collision avoidance between ads, retry nudges)
# because that gap is sized to keep ads safely apart, not to decide
# "was this worth an early post". BTC/ETH move in much smaller increments
# than a $1/₦1,500 swing most 10-second windows, so using the same gap
# here meant fast-chase rarely found a move big enough to act on. Only
# applies inside _try_fast_chase — the scheduled cycle and multi-ad
# collision logic are untouched.
_FAST_CHASE_GAP_OVERRIDE = {
    ("NGN", "BTC"): Decimal("1500"),
    ("NGN", "ETH"): Decimal("1500"),
    ("USD", "BTC"): Decimal("0.5"),
    ("USD", "ETH"): Decimal("0.5"),
}

# Ad 1 (BTC/USD) — NO reaction threshold at all: any upward move in the
# spot-derived price, however tiny, triggers a fast update. (0.005 here
# only exists so a strict "<" comparison never blocks a real move; the
# posted price is still rounded to 0.01 like every other ad.)
_AD1_FAST_CHASE_GAP = Decimal("0.005")


def _fast_chase_gap(currency_id: str, token_id: str, reference_price=None, slot_idx=None) -> Decimal:
    if slot_idx == -1 and currency_id.upper() == "USD" and token_id.upper() == "BTC":
        return _AD1_FAST_CHASE_GAP
    override = _FAST_CHASE_GAP_OVERRIDE.get((currency_id.upper(), token_id.upper()))
    if override is not None:
        return override
    return get_min_price_gap(currency_id, token_id, reference_price)


# Pairs eligible for the manual "Nudge Amount" override (Ad 2/Ad 3 only —
# see _MANUAL_NUDGE_ELIGIBLE_PAIRS usage in ads_section_keyboard and
# _resolve_price_collision). These are exactly the 4 pairs that currently
# get an automatic fixed-amount nudge ($9/₦12,600 per gap level) rather
# than the stablecoin 1%-of-price gap, so a manual override actually makes
# sense as a flat currency amount here.
_MANUAL_NUDGE_ELIGIBLE_PAIRS = {("USD", "BTC"), ("USD", "ETH"), ("NGN", "BTC"), ("NGN", "ETH")}


def _manual_nudge_for_slot(sess, slot_idx: int, currency_id: str, token_id: str):
    """Return this slot's manually-set nudge amount (Decimal) if the user
    has set one for an eligible pair, else None (meaning: fall back to the
    automatic get_min_price_gap()-derived amount)."""
    if slot_idx == -1:
        return None
    if (currency_id.upper(), token_id.upper()) not in _MANUAL_NUDGE_ELIGIBLE_PAIRS:
        return None
    raw = (_ad_settings(sess, slot_idx) or {}).get("manual_nudge", "")
    if not raw:
        return None
    try:
        val = Decimal(str(raw))
        return val if val > 0 else None
    except Exception:
        return None


# How far to nudge a price off of ITS OWN last posted value when Bybit
# rejects it as an exact duplicate (90043 — "differs from your existing ad
# by less than 0%"). Deliberately tiny, and shared by every place that
# handles a self-duplicate rejection: that 90043 check is Bybit comparing
# an ad against ONLY its own previous price, it has nothing to do with the
# real inter-ad collision gap (get_min_price_gap / _fast_chase_gap), which
# exists to keep DIFFERENT ads apart. Using the full inter-ad gap (or worse,
# an exponentially growing multiple of it) here used to stack an unearned
# extra $3/$6/$9+ on top of a price that may already have correctly cleared
# every real ad-vs-ad collision.
_SELF_DUPLICATE_EPSILON = Decimal("0.01")


def _resolve_price_collision(sess, slot_idx: int, currency_id: str, token_id: str, natural_price: Decimal):
    """
    Multiple ads on the SAME (currency, coin) pair are allowed to use the
    same floating % (or fixed base) — Bybit doesn't reject on the % or the
    starting price, it rejects when the actual POSTED prices land too
    close together. So: if this ad's naturally-computed price is within
    get_min_price_gap() of another active ad's currently-known price on
    the same pair, step this one down below the lowest conflicting price
    by that gap — e.g. Ad 1 posts ₦84,908,465.23, Ad 2 (same pair, same
    float) would naturally compute the same number, so it gets pushed to
    ₦84,903,465.23 or lower instead.

    Fixed-amount gap for BTC/ETH pairs (₦5,000 / $3 etc — see
    MIN_PRICE_GAP in bybit.py). For USDT/USDC specifically, the gap is 1%
    of the actual price instead, since a flat amount would be the wrong
    scale for a stablecoin price (see get_min_price_gap in bybit.py).

    This only ever looks at OTHER slots (plus this ad's own last-posted
    price, to avoid a 90043 "unchanged" rejection) — never changes which
    ad "wins" the natural price, it just moves the others out of the way.
    Resolves iteratively so a 3-way collision clears every conflict, not
    just the nearest one.

    Returns (resolved_price, collided_with) — collided_with is a list of
    human-readable labels (e.g. ["Ad 1"], or ["its own last posted price"])
    this price was adjusted away from, empty if no adjustment was needed.
    Callers use this to tell the user WHY a price isn't at the natural
    maximum, instead of leaving it unexplained.

    IMPORTANT — single pass, never re-checks a conflict it already
    resolved. This used to be an iterative "keep looping until nothing
    changes" loop that re-scanned the FULL conflict list on every pass.
    That looks safe but isn't: once a conflict is resolved the running
    price sits EXACTLY `gap` away from it, and on the very next pass that
    same conflict (or a different one at the same price) could still read
    as "too close" and get subtracted a second time — silently doubling
    (or worse) the actual nudge while the notification only ever recorded
    the label once, so users saw "nudged by $3" while the real drop was
    $6, $9, etc. Processing conflicts exactly once, highest price first,
    gives the same correct multi-way-collision behavior (a price that's
    too close to two different ads still clears both) without ever
    touching the same conflict twice.

    IMPORTANT — the self-conflict ("its own last posted price") uses a
    tiny epsilon, NOT the full inter-ad gap. Bybit's 90043 rejection
    ("price differs from your existing ad by less than 0%") only fires on
    an EXACT duplicate — it has nothing to do with staying $3/₦5,000 away
    from your own old price, that requirement only applies BETWEEN
    different ads. Treating the self-check with the full gap used to
    stack an extra, unearned full-gap deduction on top of any real ad-vs-
    ad collision (natural price clears Ad 2's gap fine, but then lands
    close enough to this ad's OWN prior price — itself only ever gap-
    distant from Ad 2's PREVIOUS price — to trip a second full
    deduction), which is what produced $6/$9 nudges for what was really
    only ever one $3 collision.
    """
    gap = get_min_price_gap(currency_id, token_id, natural_price)
    _manual = _manual_nudge_for_slot(sess, slot_idx, currency_id, token_id)
    if _manual is not None:
        gap = _manual
    conflicts = []   # list of (price, label, required_gap)
    for i in range(-1, len(sess.extra_ad_slots)):
        if i == slot_idx:
            continue
        # SENIORITY: only ever defer to a slot that's senior to this one —
        # Ad 1 (-1) is senior to Ad 2 (0) and Ad 3 (1); Ad 2 is senior to
        # Ad 3. Slot indices already sort this way (-1 < 0 < 1), so a plain
        # i < slot_idx check IS the seniority check. Without this, Ad 1
        # could get pushed DOWN below Ad 2/3's price — backwards, since
        # Ad 1 is supposed to be free to hold the top of the market and
        # everyone else steps around IT, never the other way.
        if i > slot_idx:
            continue
        # ACTIVE ONLY: a slot that isn't actually running can still have
        # cached ad_data + a stale current_price sitting in session state
        # (e.g. it was configured once, then stopped, or never fully
        # removed) — that's not a live competing ad, it's leftover memory.
        # Treating it as a real conflict is exactly what caused "Ad 3"
        # collisions to show up for users who only ever had Ad 1 and Ad 2
        # actually running.
        if not _ad_running(sess, i):
            continue
        other_ad_data = _ad_data_of(sess, i)
        if not other_ad_data:
            continue
        if (other_ad_data.get("currencyId","").upper() != currency_id.upper()
                or other_ad_data.get("tokenId","").upper() != token_id.upper()):
            continue
        other_price = _ad_current_price(sess, i)
        if other_price and other_price > 0:
            conflicts.append((other_price, _ad_slot_label(i), gap))

    # NOTE — deliberately no "own last posted price" entry here anymore.
    # An earlier version preventatively avoided this ad's own tracked price
    # too, on the theory that it was cheap insurance against a 90043
    # self-duplicate. In practice that made results UNPREDICTABLE instead:
    # a price that already cleared every real sibling conflict could still
    # get an extra, hard-to-predict cascade tacked on just because it
    # happened to land near this ad's own price history — e.g. a discovered
    # boundary $3.02 clear of every real conflict getting knocked down a
    # further $3 anyway, for no reason a user watching the logs could
    # anticipate. Sibling conflicts are the only thing this function
    # protects against now — fully deterministic (N other ads on the same
    # pair means at most N applications of `gap`, cascading from whichever
    # sibling price is highest). A genuine self-duplicate (this ad's new
    # price rounding to exactly what's already live) is comparatively rare
    # and is caught REACTIVELY instead — every call site posts, and if
    # Bybit comes back with 90043, retries once with a single real-gap
    # nudge. That's simpler to reason about and matches how Bybit itself
    # actually enforces it (an exact-match check on ITS side, not a
    # $3-radius exclusion zone).

    # Process highest conflicting price first. Each conflict is visited
    # EXACTLY ONCE: we check it against whatever the running price is at
    # that point (so a genuine multi-way collision still cascades
    # correctly — clearing the top ad can bring you within range of a
    # lower one) but we never go back and re-check a conflict already
    # handled, which is what caused the double-subtraction bug.
    price = natural_price
    collided_with = []
    for cp, label, req_gap in sorted(conflicts, key=lambda c: c[0], reverse=True):
        if abs(price - cp) < req_gap:
            candidate = cp - req_gap
            if candidate < price:
                price = candidate
            if label not in collided_with:
                collided_with.append(label)
    return price, collided_with


def _settings(uid: int) -> dict:
    """Shorthand: get the mutable settings dict for uid."""
    return get_session(uid).settings

def _save_settings(uid: int):
    """Persist the user's current session settings to disk — Ad 1's fields
    AND any extra ad slots (Ad 2/Ad 3), so a redeploy doesn't silently
    delete multi-ad configuration that was never being saved before.
    Also persists the dedicated USDT AD2/USDT AD3 slots when present."""
    sess = get_session(uid)

    def _persistable(d: dict) -> dict:
        # `_notify_last_*` are runtime-only notification throttles (see
        # _should_notify_now) — no reason to write them to disk, and keeping
        # them out guarantees a stray non-JSON value there can never block
        # a save again.
        return {k: v for k, v in d.items() if not str(k).startswith(("_notify_last_", "_notify_count_"))}

    db.save_settings(uid, _persistable(sess.settings))
    db.save_extra_slots(uid, [_persistable(slot["settings"]) for slot in sess.extra_ad_slots])

def _load_settings_from_disk(uid: int):
    """Load persisted settings from disk into the user's session on first access.

    Also back-populates slot-keyed keys from generic keys (and vice versa)
    so that both ad_id_1/bybit_uid_1 and ad_id/bybit_uid are always in sync.
    This ensures UID and Ad ID survive /start, restarts, and slot switches.

    AD BOT settings (mode, increment, float_pct, local_usdt_ref, interval) are
    also stored per-slot and restored here for the user's active slot.
    """
    saved = db.load_settings(uid)
    if saved:
        sess = get_session(uid)
        for k, v in saved.items():
            sess.settings[k] = v
        # Back-fill: if only generic key exists, populate slot-keyed keys (slots 1 & 2)
        for field in ("ad_id", "bybit_uid"):
            generic_val = sess.settings.get(field, "")
            for slot in ("1", "2"):
                slot_key = f"{field}_{slot}"
                if not sess.settings.get(slot_key) and generic_val:
                    sess.settings[slot_key] = generic_val
        # Back-fill: if only slot-keyed keys exist, populate generic key from slot 1
        for field in ("ad_id", "bybit_uid"):
            if not sess.settings.get(field):
                slot1_val = sess.settings.get(f"{field}_1", "")
                if slot1_val:
                    sess.settings[field] = slot1_val

        # ── Restore active slot's AD BOT settings into generic keys ──
        # This ensures the correct slot's config is active after /start or restart.
        # Ensure selected_slot is set first (default to 0)
        if not hasattr(sess, "selected_slot"):
            sess.selected_slot = 0
        active_slot_str = str(sess.selected_slot + 1)
        for field, default in [("mode", "fixed"), ("increment", "0.05"),
                                ("float_pct", ""), ("local_usdt_ref", ""), ("interval", 2)]:
            slot_val = sess.settings.get(f"{field}_{active_slot_str}")
            if slot_val is not None and slot_val != "":
                sess.settings[field] = slot_val
            elif not sess.settings.get(field):
                sess.settings[field] = default

        logger.debug(f"[Settings] Loaded for user={uid}: ad_id={sess.settings.get('ad_id')!r} "
                     f"bybit_uid={sess.settings.get('bybit_uid')!r} "
                     f"ad_id_1={sess.settings.get('ad_id_1')!r} bybit_uid_1={sess.settings.get('bybit_uid_1')!r} "
                     f"mode={sess.settings.get('mode')!r} slot={active_slot_str}")

    # ── Restore Ad 2 / Ad 3 (extra ad slots) ──
    # These previously had NO persistence at all (see save_extra_slots'
    # docstring in db.py) — a redeploy deleted Ad 2/Ad 3's configuration
    # outright, with nothing to recover. Idempotent: only ever CREATES a
    # slot here if it doesn't already exist in this session, so calling
    # this repeatedly (e.g. every time the menu loads) never duplicates
    # or overwrites a slot the user is actively editing right now.
    saved_extra = db.load_extra_slots(uid)
    if saved_extra:
        sess = get_session(uid)
        for i, extra_settings in enumerate(saved_extra):
            if not extra_settings:
                continue
            _ensure_ad_slots(sess)
            if i >= len(sess.extra_ad_slots):
                break
            if not sess.extra_ad_slots[i]["settings"].get("ad_id"):
                sess.extra_ad_slots[i]["settings"].update(extra_settings)
        logger.debug(f"[Settings] Restored {len(saved_extra)} extra ad slot(s) for user={uid}")

    _ensure_ad_slots(get_session(uid))

def is_admin(uid): return uid in ADMIN_IDS

def _get_or_register_user(telegram_user):
    """Register user in DB on first access and update last_active. Returns (user_dict, is_new)."""
    uid   = telegram_user.id
    uname = telegram_user.username or ""
    dname = telegram_user.full_name or ""
    # get_or_create_user already writes last_active on every call (db.py line 160)
    return db.get_or_create_user(uid, uname, dname)

# Pre-populate admin chat IDs from environment config so upgrade notifications
# work even before the admin has sent /start in this deploy session.
_admin_chat_ids: set = set(ADMIN_IDS)  # seeded from config; updated on /start

def _get_admin_chat_ids() -> set:
    return _admin_chat_ids


# ─────────────────────────────────────────
# 📊 Setup progress checker (per-user)
# ─────────────────────────────────────────
def setup_progress(uid: int) -> tuple:
    s     = _settings(uid)
    sess  = _s(uid)
    slot  = _get_user_slot_str(uid)   # per-user slot — NOT global
    steps = [
        bool(s.get(f"ad_id_{slot}") or s.get("ad_id")),
        bool(s.get(f"bybit_uid_{slot}") or s.get("bybit_uid")),
        bool(sess.ad_data),
        bool(s.get("increment") or s.get("float_pct")),
        bool(s.get("interval")),
    ]
    done  = sum(steps)
    total = len(steps)
    bar   = "".join("✅" if s else "⬜" for s in steps)
    return done, total, bar


def next_setup_hint(uid: int) -> str:
    s    = _settings(uid)
    sess = _s(uid)
    slot = _get_user_slot_str(uid)   # per-user slot — NOT global
    ad_id    = s.get(f"ad_id_{slot}") or s.get("ad_id","")
    bybit_uid = s.get(f"bybit_uid_{slot}") or s.get("bybit_uid","")
    if not ad_id:
        return "👉 Start by tapping *🆔 Set Ad ID*"
    if not bybit_uid:
        return "👉 Next: tap *👤 Set UID* to set your Bybit user ID"
    if not sess.ad_data:
        return "👉 Next: tap *📋 Fetch Ad Details* to load your ad from Bybit"
    mode = s.get("mode", "fixed")
    if mode == "fixed" and not s.get("increment"):
        return "👉 Next: tap *➕ Set Increment* to set your price step"
    if mode == "floating" and not s.get("float_pct"):
        return "👉 Next: tap *📊 Set Float %* to set your market percentage"
    currency_upper = sess.ad_data.get("currencyId","").upper()
    needs_ref_cur  = currency_needs_ref(currency_upper) or currency_upper == "NGN"
    if mode == "floating" and needs_ref_cur and not s.get("local_usdt_ref"):
        return f"👉 Next: tap *💱 Set {currency_upper}/USDT Ref* to set the reference rate"
    return "✅ *All set!* Tap *🟢 Start Auto-Update* to begin"


# ─────────────────────────────────────────
# 🔑 Per-user credential helper
# ─────────────────────────────────────────
def get_user_creds(user_id: int, slot: int | None = None) -> dict | None:
    """
    Load Bybit credentials for a user using THEIR OWN per-user slot from DB.

    CRITICAL: Uses _s(user_id).selected_slot — NOT the global bybit._active_index.
    This ensures User A switching slots never affects User B.

    ALL users — including admins — now load from DB first.
    Admins fall back to env account ONLY if no DB key is saved for their slot,
    so the bot works with or without Render env keys.

    Args:
        user_id: Telegram user ID
        slot: Optional override (0-based index). If None, uses user's own selected_slot.

    Return values:
      - User/admin w/ DB key  → {"key": ..., "secret": ...}
      - Admin w/ no DB key    → None  (bybit._resolve_creds(None) uses env account if set)
      - Non-admin no DB key   → {"key": "", "secret": ""}  ← SENTINEL: no key saved
    """
    user_slot = slot if slot is not None else _get_user_slot(user_id)
    slot_str  = str(user_slot + 1)   # "1" or "2"

    key    = db.get_api(user_id, f"bybit_key_{slot_str}")
    secret = db.get_api(user_id, f"bybit_secret_{slot_str}")

    if key and secret:
        logger.debug(f"[Creds] User {user_id} slot {slot_str} — DB key found")
        return {"key": key, "secret": secret}

    # No DB key for this user/slot
    if is_admin(user_id):
        # Admin fallback: use env account (may also be empty if no env keys set)
        logger.info(f"[Creds] Admin {user_id} slot {slot_str} — no DB key, falling back to env account")
        return None   # bybit._resolve_creds(None) uses BYBIT_ACCOUNTS[_active_index] if available

    # Non-admin: return sentinel (empty strings) — callers show "No API set" error
    logger.info(f"[Creds] User {user_id} slot {slot_str} — NO API KEY SAVED")
    return {"key": "", "secret": ""}


# ─────────────────────────────────────────
# 🏠 MAIN MENU
# ─────────────────────────────────────────
def main_menu_keyboard(uid: int = 0):
    sess   = _s(uid) if uid else None
    r_icon = "🟢" if (sess and _any_ad_running(sess)) else "📊"
    ap_on  = bool(sess and sess.auto_pay_enabled)
    ap_lbl = "✅ AUTO-PAY: ON" if ap_on else "💳 AUTO-PAY: OFF"
    all_ac = get_all_accounts()

    # ── Account slot buttons (always at least 2 slots) ──
    _user_slot_idx = _s(uid).selected_slot if uid else 0
    _num_slots = max(len(all_ac), 2)
    _slot_row = []
    for i in range(_num_slots):
        label = all_ac[i]["label"] if i < len(all_ac) else f"Account {i + 1}"
        tick  = "✅ " if i == _user_slot_idx else ""
        _slot_row.append(InlineKeyboardButton(f"{tick}{label}", callback_data=f"switch_account_{i}"))
    kb = [_slot_row]

    kb += [
        [InlineKeyboardButton(f"{r_icon} AD PRICE BOT",  callback_data="section_ads"),
         InlineKeyboardButton(ap_lbl,                    callback_data="toggle_auto_pay")],
        [InlineKeyboardButton("🔑 Set APIs",             callback_data="section_apis"),
         InlineKeyboardButton("⬆️ Upgrade Plan",         callback_data="upgrade_plan")],
        [InlineKeyboardButton("📡 Bot Status",           callback_data="bot_status"),
         InlineKeyboardButton("🌍 Get My IP",            callback_data="get_my_ip")],
        [InlineKeyboardButton("🔁 Reset Session",        callback_data="reset_confirm")],
    ]
    return InlineKeyboardMarkup(kb)


def main_menu_text(uid: int = 0) -> str:
    uid      = uid or _current_user_id
    sess     = _s(uid)
    done, total, bar = setup_progress(uid)
    p_status = "✅ ON — auto marking new BUY orders paid" if sess.auto_pay_enabled else "❌ OFF"
    r_status = "🟢 Running" if _any_ad_running(sess) else "🔴 Off"
    _uid_slot = _s(uid).selected_slot
    _all_ac   = get_all_accounts()
    if _all_ac and _uid_slot < len(_all_ac):
        acct = _all_ac[_uid_slot]
    elif _all_ac:
        acct = _all_ac[0]
    else:
        acct = {"label": f"Account {_uid_slot + 1}"}
    badge = sub.plan_badge(uid) if uid else _current_plan_badge
    return (
        "🤖 *P2P Auto Bot — Control Panel*\n\n"
        f"🆔 Your ID: <code>{uid}</code> | {badge}\n"
        f"🔑 Active Account: <b>{acct['label']}</b>\n"
        f"📋 Setup: {bar} <code>{done}/{total}</code>\n\n"
        f"┌ 📊 Price Bot: {r_status}\n"
        f"└ 💳 Auto-Pay: {p_status}\n\n"
        "_Select a section below to get started:_"
    )


def back_main():
    return [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]


def back_section(section: str):
    labels = {"section_ads": "📊 AD Price Bot"}
    return [[InlineKeyboardButton(f"⬅️ Back — {labels.get(section,'Back')}", callback_data=section)]]


def back_prev(prev: str):
    """Back to previous section button — used after text input success."""
    labels = {"section_ads": "📊 AD Price Bot", "main_menu": "🏠 Main Menu"}
    label = labels.get(prev, "⬅️ Back")
    return InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ Back to {label}", callback_data=prev)]])




_flood_controlled_until: dict = {}   # chat_id -> datetime after which sends may be attempted again

def _mark_flood_controlled(chat_id, retry_after_seconds):
    _flood_controlled_until[chat_id] = datetime.now() + timedelta(seconds=(retry_after_seconds or 0) + 5)

def _is_flood_controlled(chat_id) -> bool:
    until = _flood_controlled_until.get(chat_id)
    return until is not None and datetime.now() < until


async def _safe_send(bot, **kwargs) -> bool:
    """Drop-in replacement for `bot.send_message(...)` that NEVER raises.

    This exists because of a real incident: auto_update_loop and the USDT
    triad loop call bot.send_message on almost every cycle — every 5s in
    Quick Market mode, across up to 3 ads. Once Telegram's flood control
    kicks in for a chat (telegram.error.RetryAfter), EVERY subsequent
    send_message to that chat raises the same exception until the
    cooldown expires — observed in production at ~3997 seconds (over an
    hour). Since none of these call sites were wrapped, that exception
    propagated straight up and silently killed the whole asyncio task —
    which is to say, that user's ad just stopped auto-updating with
    nothing in the chat to explain why ("the bot suddenly stopped
    working"). This wrapper catches that (and any other send failure),
    logs it, and lets the loop continue normally on its next cycle
    instead of dying.

    Also remembers which chats are currently flood-controlled and skips
    even ATTEMPTING a send during that window, rather than hitting the
    exact same RetryAfter every single cycle for the next hour — that
    both wastes an outbound call every cycle and spams the log.

    Returns True if the message was actually sent, False otherwise —
    callers that don't care can just ignore the return value, exactly
    like the `await bot.send_message(...)` statements this replaces.
    """
    chat_id = kwargs.get("chat_id")
    if chat_id is not None and _is_flood_controlled(chat_id):
        return False
    try:
        await bot.send_message(**kwargs)
        return True
    except RetryAfter as e:
        logger.warning(
            f"[TelegramFlood] chat={chat_id} flood-controlled — "
            f"retry_after={e.retry_after}s; suppressing further sends to "
            f"this chat until then instead of hitting this on every cycle"
        )
        if chat_id is not None:
            _mark_flood_controlled(chat_id, e.retry_after)
        return False
    except Forbidden:
        logger.info(f"[Telegram] chat={chat_id} has blocked the bot or is unreachable — message dropped")
        return False
    except Exception as e:
        logger.warning(f"[Telegram] send_message to {chat_id} failed: {e} — message dropped, loop continues")
        return False


_NOTIFY_COOLDOWN_SECONDS = int(os.getenv("AD_EDIT_NOTIFY_COOLDOWN_SECONDS", "300") or 300)

def _should_notify_now(s: dict, key: str, cooldown_secs: int = None) -> bool:
    """Throttles a repetitive per-cycle notification (e.g. "price
    unchanged, skipping") to at most once per `cooldown_secs` (default 5
    minutes) PER AD SLOT, regardless of how fast that ad's own interval
    is. Without this, a fast-polling mode like Quick Market (5s interval)
    sends the exact same "no change" message every single cycle — up to
    720/hour per ad — which is what triggered Telegram's flood control in
    production (a reported >3900 second, i.e. over an hour, lockout for
    that chat). Always returns True the first time a given `key` is seen
    for this ad, so the user still gets immediate confirmation the first
    time something happens; only REPEATS of the same state are throttled.
    The underlying event is still logged to Render's own logs every
    cycle regardless — this only throttles the Telegram message."""
    if cooldown_secs is None:
        cooldown_secs = _NOTIFY_COOLDOWN_SECONDS
    # IMPORTANT: `s` is the ad's SETTINGS dict, which db.save_settings()
    # writes straight to disk as JSON. This used to store a datetime object
    # here, and json.dump can't serialize one — so every save for that user
    # failed with "Object of type datetime is not JSON serializable" (logged
    # by db._write_json, but never raised), silently freezing ALL of that
    # user's persisted settings (ad_id, mode, interval, slots...) at whatever
    # was last saved before the first throttled notification fired. A plain
    # epoch float is JSON-safe. Legacy/odd values (an old datetime, or a
    # string left behind by a previous version) are treated as "never sent".
    now_ts = datetime.now().timestamp()
    field = f"_notify_last_{key}"
    last = s.get(field)
    if isinstance(last, (int, float)) and not isinstance(last, bool):
        if now_ts - last < cooldown_secs:
            return False
    s[field] = now_ts
    return True


# Successful-edit notices ("✅ Cycle N 💲 price", "⚡ Fast update") are the
# highest-volume Telegram messages the bot sends. Quick Market polls every
# 5s and USDT/USD ads can run at similar fine intervals, so a market that
# keeps moving would otherwise produce up to 12 messages/minute per ad —
# the same pattern that got a chat flood-locked for over an hour in
# production. Cap them to one per AD_EDIT_SUCCESS_NOTIFY_SECONDS per ad
# slot. Ads whose own interval is already >= that cap are NOT affected
# (they naturally send at most one per cycle, exactly as before). The edit
# itself is never skipped — only the Telegram message — and the next
# message that does go out says how many edits it is summarising.
_EDIT_NOTIFY_SECONDS = int(os.getenv("AD_EDIT_SUCCESS_NOTIFY_SECONDS", "60") or 60)

def _edit_notice(s: dict, interval_secs: int):
    """Decide whether a successful-edit notice should be sent now.
    Returns (send, suppressed): `suppressed` is how many edit notices were
    held back since the last one that was sent (0 if none)."""
    count_key = "_notify_count_edit_ok"
    try:
        interval_secs = int(interval_secs)
    except (TypeError, ValueError):
        interval_secs = _EDIT_NOTIFY_SECONDS
    if interval_secs >= _EDIT_NOTIFY_SECONDS:
        return True, 0                       # slow ad — one per cycle, as always
    if _should_notify_now(s, "edit_ok", _EDIT_NOTIFY_SECONDS):
        held = int(s.get(count_key, 0) or 0)
        s[count_key] = 0
        return True, held
    s[count_key] = int(s.get(count_key, 0) or 0) + 1
    return False, 0

def _edit_notice_suffix(suppressed: int) -> str:
    if not suppressed:
        return ""
    return (f"\n<i>(+{suppressed} more edit{'s' if suppressed != 1 else ''} since the last "
            f"message — notices are limited to one per {_EDIT_NOTIFY_SECONDS}s to protect your "
            f"Telegram account from rate limits)</i>")


async def _safe_reply(message, text: str, **kwargs) -> bool:
    """Drop-in replacement for `message.reply_text(...)` that never raises
    — same reasoning as _safe_send above, applied to direct command
    replies (ping_bybit_command, etc.) rather than the per-cycle loops.
    A chat already flood-controlled (see _is_flood_controlled) would
    otherwise make a command's FINAL reply vanish with no error shown —
    the command actually ran and got a real result, but the user sees
    nothing, indistinguishable from the bot hanging."""
    chat_id = getattr(getattr(message, "chat", None), "id", None)
    if chat_id is not None and _is_flood_controlled(chat_id):
        return False
    try:
        await message.reply_text(text, **kwargs)
        return True
    except RetryAfter as e:
        logger.warning(f"[TelegramFlood] chat={chat_id} flood-controlled on reply — retry_after={e.retry_after}s")
        if chat_id is not None:
            _mark_flood_controlled(chat_id, e.retry_after)
        return False
    except Forbidden:
        logger.info(f"[Telegram] chat={chat_id} has blocked the bot or is unreachable — reply dropped")
        return False
    except Exception as e:
        logger.warning(f"[Telegram] reply_text to {chat_id} failed: {e} — reply dropped")
        return False


def _mode_display_label(mode: str) -> str:
    """User-facing mode name for Telegram messages. Deliberately generic
    for decodo_market — its DISPLAY name is "QUICK MARKET" everywhere the
    user sees it, even though the setting is still stored internally as
    "decodo_market" (so nothing about already-saved settings needs to
    change, and the Auto Resume Agent / disk persistence keep working
    exactly as before). Never call mode.upper() directly in a string that
    reaches the user — always go through this."""
    return {"decodo_market": "QUICK MARKET"}.get(mode, (mode or "").upper())


def ads_section_keyboard(uid: int = 0):
    sess       = _s(uid) if uid else None
    if sess:
        _ensure_ad_slots(sess)
    slot_idx   = sess.editing_slot if sess else -1
    s          = _ad_settings(sess, slot_idx) if sess else {}
    ad_data    = _ad_data_of(sess, slot_idx) if sess else {}
    mode       = s.get("mode", "fixed")
    mode_icon  = {"fixed": "💲", "floating": "📈", "ad_copy": "🪞", "browserbase_market": "🌐", "decodo_market": "⚡"}.get(mode, "💲")
    mode_label = f"{mode_icon} Mode: {_mode_display_label(mode).replace('_',' ')}"
    ad_loaded  = bool(ad_data)
    running    = _ad_running(sess, slot_idx) if sess else False
    status     = "🟢 Stop Auto-Update" if running else "▶️ Start Auto-Update"

    rows = []

    # ── Ad slot switcher — Ad 1..Ad 8, 4 per row ──
    switch_row = []
    for i in range(-1, TOTAL_AD_SLOTS - 1):
        label = _ad_slot_label(i)
        if _ad_running(sess, i) if sess else False:
            label = f"🟢{label}"
        if i == slot_idx:
            label = f"• {label} •"
        switch_row.append(InlineKeyboardButton(label, callback_data=f"edit_ad_{i+2}"))
        if len(switch_row) == 4:
            rows.append(switch_row)
            switch_row = []
    if switch_row:
        rows.append(switch_row)
    rows.append([InlineKeyboardButton("🗂 All My Ads (Dashboard)", callback_data="ads_dashboard")])

    if slot_idx == -1:
        rows.append([
            InlineKeyboardButton("🆔 Set Ad ID",    callback_data="set_ad_id"),
            InlineKeyboardButton("👤 Set UID",      callback_data="set_uid"),
            InlineKeyboardButton("🗑 Del UID",      callback_data="delete_uid"),
        ])
    else:
        # Ads 2-8 share Ad 1's Bybit account + UID — only the Ad ID differs.
        rows.append([InlineKeyboardButton("🆔 Set Ad ID", callback_data="set_ad_id")])

    rows.append([InlineKeyboardButton("📋 Fetch Ad Details", callback_data="fetch_ad")])
    rows.append([InlineKeyboardButton("📃 My Ads List", callback_data="fetch_my_ads")])
    rows.append([
        InlineKeyboardButton(mode_label,        callback_data="mode_menu"),
        InlineKeyboardButton("⏱ Set Interval", callback_data="set_interval"),
    ])

    if mode == "fixed":
        rows.append([InlineKeyboardButton("➕ Set Increment", callback_data="set_increment")])
    elif mode == "ad_copy":
        if _is_btc_ngn_ad(ad_data):
            _cpr = (s.get("close_price_range") or "").strip()
            rows.append([InlineKeyboardButton(
                f"🎯 Close Price Range: {_cpr}" if _cpr else "🎯 Set Close Price Range",
                callback_data="set_close_price_range"
            )])
            _top_range = int(s.get("top_range", 1) or 1)
            rows.append([InlineKeyboardButton(
                f"🏆 Top Range: {_top_range}", callback_data="set_top_range"
            )])
            _merchant = (s.get("merchant_username") or "").strip()
            rows.append([InlineKeyboardButton(
                f"👤 Merchant Watch: {_merchant}" if _merchant else "👤 Set Merchant Watch",
                callback_data="set_merchant_watch"
            )])
            rows.append([InlineKeyboardButton("🔍 View Market Ads List (BTC/NGN)", callback_data="view_market_ads")])
        else:
            _range = s.get("ad_copy_range", "top10")
            _range_label = _ad_copy_range_label(_range)
            rows.append([InlineKeyboardButton(f"🔝 Copy Range: {_range_label}", callback_data="set_ad_copy_range")])
            rows.append([InlineKeyboardButton("🔍 View Market Ads List", callback_data="view_market_ads")])
    elif mode in ("browserbase_market", "decodo_market"):
        # Copy Bybit's live rank-1 price — no settings of their own.
        pass
    else:
        rows.append([InlineKeyboardButton("📊 Set Float %",   callback_data="set_float_pct")])
        _tok, _cur = _slot_pair(slot_idx)
        if currency_needs_ref(_cur) or _cur == "NGN":
            rows.append([InlineKeyboardButton(f"💱 Set {_cur}/USDT Ref", callback_data="set_ngn_ref")])
        # Manual nudge amount — every BTC/USD ad (Ad 1-4) and every ETH/USD
        # ad (Ad 6-7). Overrides the automatic -$9 per level gap.
        if slot_idx in NUDGE_BUTTON_SLOTS and (_cur, _tok) in _MANUAL_NUDGE_ELIGIBLE_PAIRS:
            _nudge_val = (s.get("manual_nudge") or "").strip()
            _nudge_label = f"↔️ Nudge Amount: {_nudge_val}" if _nudge_val else "↔️ Set Nudge Amount"
            rows.append([InlineKeyboardButton(_nudge_label, callback_data="set_manual_nudge")])

    # Dedicated BTC/NGN market list (Ad 5), any mode except ad_copy.
    if (ad_data.get("tokenId","").upper() == "BTC"
            and ad_data.get("currencyId","").upper() == "NGN"
            and mode != "ad_copy"):
        rows.append([InlineKeyboardButton("🔍 View Market Ads List (BTC/NGN)",
                                          callback_data="view_market_ads")])

    # Update Once Now is only offered when no OTHER ad is currently running.
    _others_running = any(_ad_running(sess, i) for i in range(-1, TOTAL_AD_SLOTS - 1) if i != slot_idx) if sess else False
    if ad_loaded and slot_idx == -1 and not _others_running:
        rows.append([InlineKeyboardButton("🔄 Update Once Now", callback_data="update_now")])

    rows.append([InlineKeyboardButton(status, callback_data="toggle_refresh")])

    rows += back_main()
    return InlineKeyboardMarkup(rows)


def ads_section_text(uid: int = 0) -> str:
    uid      = uid or _current_user_id
    sess     = _s(uid)
    slot_idx = sess.editing_slot
    s        = _ad_settings(sess, slot_idx)
    ad_data  = _ad_data_of(sess, slot_idx)
    acct_slot = _get_user_slot_str(uid)   # per-user BYBIT ACCOUNT slot — NOT the ad slot

    if slot_idx == -1:
        ad_id     = s.get(f"ad_id_{acct_slot}") or s.get("ad_id","") or "❗ Not set"
        bybit_uid = s.get(f"bybit_uid_{acct_slot}") or s.get("bybit_uid","") or "❗ Not set"
    else:
        ad_id     = s.get("ad_id","") or "❗ Not set"
        # Ads 2/3 always use the same UID as the active account (Ad 1's UID).
        bybit_uid = (
            sess.settings.get(f"bybit_uid_{acct_slot}")
            or sess.settings.get("bybit_uid","")
            or "❗ Not set"
        )

    mode      = s.get("mode",           "fixed")
    interval  = s.get("interval",       2)
    increment = s.get("increment",      "0.05")
    float_pct = s.get("float_pct",     "") or "❗ Not set"
    local_ref = sess.shared_local_usdt_ref or s.get("local_usdt_ref","") or "❗ Not set"
    cur_label = ad_data.get("currencyId","NGN").upper() if ad_data else "NGN"
    cur       = str(_ad_current_price(sess, slot_idx)) if _ad_current_price(sess, slot_idx) else "—"
    status    = "🟢 Running" if _ad_running(sess, slot_idx) else "🔴 Stopped"

    if ad_data:
        price    = ad_data.get("price",        "—")
        min_amt  = ad_data.get("minAmount",    "—")
        max_amt  = ad_data.get("maxAmount",    "—")
        qty      = ad_data.get("lastQuantity", ad_data.get("quantity","—"))
        token    = ad_data.get("tokenId",      "—")
        currency = ad_data.get("currencyId",   "—")
        ad_stat  = {10:"🟢 Online",20:"🔴 Offline",30:"✅ Done"}.get(ad_data.get("status"),"?")
        max_pct  = get_max_float_pct(currency, token)
        ad_info  = (
            f"\n📋 <b>Loaded Ad:</b>\n"
            f"  💱 <code>{token}/{currency}</code> | 💲 <code>{price}</code>\n"
            f"  Min: <code>{min_amt}</code> | Max: <code>{max_amt}</code> | Qty: <code>{qty}</code>\n"
            f"  Status: {ad_stat} | Max float: <code>{max_pct}%</code>\n"
        )
    else:
        ad_info = "\n  ⚠️ No ad loaded yet\n"

    if mode == "fixed":
        mode_info = f"  ➕ Increment: `+{increment}` per cycle"
    elif mode == "browserbase_market":
        mode_info = "  🌐 Copies Bybit's live Rank #1 price via Browserbase"
    elif mode == "decodo_market":
        mode_info = "  ⚡ Copies Bybit's live Rank #1 price (Quick Market)"
    else:
        mode_info = f"  📊 Float: `{float_pct}%`"
        if ad_data.get("currencyId","").upper() == "NGN":
            mode_info += f" | 💱 {cur_label}/USDT: `{local_ref}`"

    hint = next_setup_hint(uid) if slot_idx == -1 else "Set this ad's Ad ID, fetch its details, then set its mode."
    user_slot_idx = _get_user_slot(uid)
    acct_label = bybit.BYBIT_ACCOUNTS[user_slot_idx]["label"] if (bybit.BYBIT_ACCOUNTS and user_slot_idx < len(bybit.BYBIT_ACCOUNTS)) else f"Account {acct_slot}"
    slot_header = _ad_slot_label(slot_idx)
    multi_note = f" — {_slot_pair_label(slot_idx)} only"

    return (
        f"📊 <b>AD PRICE BOT — {slot_header}{multi_note}</b>\n"
        f"<i>{acct_label}</i>\n\n"
        f"🆔 Ad ID: <code>{ad_id}</code>\n"
        f"👤 UID (Acct {acct_slot}): <code>{bybit_uid}</code>\n"
        f"🔀 Mode: <code>{mode.replace('_',' ').upper()}</code> | ⏱ Every <code>{_ad_interval_label(s, ad_data)}</code>\n"
        f"{mode_info}\n"
        f"{ad_info}\n"
        f"📈 Session price: <code>{cur}</code> | {status}\n\n"
        f"<i>{hint}</i>"
    )


def ads_dashboard_text(uid: int) -> str:
    """All-ads-at-a-glance view — coin/pair + status per ad, per user's request."""
    sess = _s(uid)
    _ensure_ad_slots(sess)
    lines = ["🗂 <b>All My Ads (Ad 1 – Ad 8)</b>\n"]
    for i in range(-1, len(sess.extra_ad_slots)):
        ad_data = _ad_data_of(sess, i)
        running = _ad_running(sess, i)
        s       = _ad_settings(sess, i)
        icon    = "🟢" if running else "🔴"
        if ad_data:
            pair = f"{ad_data.get('tokenId','?')}/{ad_data.get('currencyId','?')}"
            price = ad_data.get("price", "—")
        else:
            pair, price = f"{_slot_pair_label(i)} · not loaded yet", "—"
        mode = _mode_display_label(s.get("mode", "fixed"))
        lines.append(f"{icon} <b>{_ad_slot_label(i)}</b> — {pair} | 💲{price} | {mode} | {'Running' if running else 'Stopped'}")
    return "\n".join(lines)


def ads_dashboard_keyboard(uid: int) -> InlineKeyboardMarkup:
    sess = _s(uid)
    rows = []
    stop_row = []
    for i in range(-1, len(sess.extra_ad_slots)):
        if _ad_running(sess, i):
            stop_row.append(InlineKeyboardButton(f"⏹ Stop {_ad_slot_label(i)}", callback_data=f"stop_ad_{i+2}"))
    if stop_row:
        rows.append(stop_row)
    if sum(1 for i in range(-1, len(sess.extra_ad_slots)) if _ad_running(sess, i)) > 1:
        rows.append([InlineKeyboardButton("⏹ Stop All Ads", callback_data="stop_all_ads")])
    edit_btns = [InlineKeyboardButton(f"✏️ Edit {_ad_slot_label(i)}", callback_data=f"edit_ad_{i+2}") for i in range(-1, len(sess.extra_ad_slots))]
    for k in range(0, len(edit_btns), 4):
        rows.append(edit_btns[k:k+4])
    rows += back_main()
    return InlineKeyboardMarkup(rows)
















# ─────────────────────────────────────────
# 💳 Payment helpers
# ─────────────────────────────────────────

# Per-user payment method name cache: {user_id: {paymentType_str: paymentName_str}}
# Populated lazily on first use; survives the session but is lightweight (just strings).
_payment_name_cache: dict = {}






def _resolve_pay_term(order_detail: dict) -> dict:
    """
    Return the best available pay_term dict from an order.
    Merges confirmedPayTerm + paymentTermList[0] so missing fields in one
    can be filled from the other.
    """
    confirmed = order_detail.get("confirmedPayTerm", {}) or {}
    terms     = order_detail.get("paymentTermList",   []) or []
    fallback  = terms[0] if terms else {}
    if not confirmed:
        return dict(fallback)
    if not fallback:
        return dict(confirmed)
    # Merge: confirmed takes priority; fallback fills any blank fields
    merged = dict(fallback)
    merged.update({k: v for k, v in confirmed.items() if v not in (None, "", {}, [])})
    return merged


























# ─────────────────────────────────────────
# 🔒 ORDER LOCK + STATE HELPERS
# ─────────────────────────────────────────



















# ─────────────────────────────────────────
# 💬 CHAT MONITOR — Poll Bybit order chats
# Fetches new messages every 12 seconds for all active orders.
# Forwards new messages to Telegram with a Reply button.
# ─────────────────────────────────────────









# ─────────────────────────────────────────
# 🚫 SELLER CANCEL REQUEST HANDLER
# ─────────────────────────────────────────

















# ─────────────────────────────────────────
# 💲 Float price calc
# ─────────────────────────────────────────
def _extract_bybit_max(error_msg: str) -> str | None:
    import re
    match = re.search(r'higher than ([\d.]+)', error_msg)
    if match:
        return match.group(1).rstrip(".")
    return None


def _extract_bybit_bounds(error_msg: str):
    """
    Parse BOTH bounds from Bybit's out-of-range message, e.g.
    'The fixed price set is lower than X or higher than Y.'
    Returns (min_str, max_str) — either may be None if not present in
    this particular message.

    Matches optional thousands-separator commas too (e.g. "79,126,068.63")
    and strips them before returning — NGN bounds are large enough that if
    Bybit ever formats them with commas, a comma-blind regex would match
    only the digits up to the first comma (e.g. "79" instead of
    "79126068.63") and silently post a price a thousand times too small.
    """
    import re
    min_match = re.search(r'lower than ([\d,]*\d(?:\.\d+)?)',  error_msg or "")
    max_match = re.search(r'higher than ([\d,]*\d(?:\.\d+)?)', error_msg or "")
    min_val = min_match.group(1).replace(",", "").rstrip(".") if min_match else None
    max_val = max_match.group(1).replace(",", "").rstrip(".") if max_match else None
    return min_val, max_val


def _safety_margin(price: Decimal) -> Decimal:
    """
    Small buffer used when retrying at a Bybit-stated boundary, so we land
    safely INSIDE the valid range instead of riding exactly on the edge.
    Posting the exact boundary value sometimes gets rejected again if
    Bybit's live min/max shifted a hair between it telling us the number
    and us submitting it (normal market movement over the round-trip).
    0.02% of price, with a small currency-agnostic floor so it's never
    effectively zero on cheap-priced pairs.
    """
    margin = price * Decimal("0.0002")
    return margin if margin > Decimal("0.01") else Decimal("0.01")


def _wants_live_ceiling(currency_id: str, token_id: str, float_pct) -> bool:
    """
    True when the floating % is set to the very top of what this bot
    allows for this pair, or one point below it (e.g. 110% or 111% for
    NGN/BTC, 130% or 131% for USD/BTC — get_max_float_pct(NGN,BTC) is 111,
    get_max_float_pct(USD,BTC) is 131).

    At that setting, the user's real intent is "give me the highest price
    Bybit will currently let me list at" — not literally "market price
    times this exact multiplier". A fixed multiplier can drift from what
    Bybit will actually accept as the live order book moves, so instead of
    trusting the formula number, the caller (auto_update_loop) deliberately
    forces an out-of-range rejection every cycle so Bybit hands back its
    OWN live ceiling in the error message, then posts that instead. See
    the 912120022 handling in auto_update_loop for where that gets used.
    """
    try:
        pct = float(float_pct)
    except (TypeError, ValueError):
        return False
    hi = get_max_float_pct(currency_id, token_id)
    return hi - 1 <= pct <= hi


def calc_floating_price(ad_data, float_pct, local_usdt_ref):
    """
    Calculate floating price for any supported currency/token pair.

    Formula:
      NGN/USD:  token_usdt_price × local_usdt_ref × float_pct / 100
      GHS/GBP/EUR/RUB/KES:  token_usdt_price × local_usdt_ref × float_pct / 100
      (same formula — local_usdt_ref is the local currency per 1 USDT rate)

    For USDT/USDC pairs the ref is not needed (token IS the dollar).
    """
    currency = ad_data.get("currencyId", "").upper()
    token    = ad_data.get("tokenId",   "").upper()

    token_price = get_token_usdt_price(token)
    if token_price <= 0:
        return None, f"Failed to fetch {token}/USDT price from Bybit"

    # Currencies that need a local/USDT reference rate
    needs_ref = currency_needs_ref(currency) or currency == "NGN"

    if needs_ref:
        if local_usdt_ref <= 0:
            return None, f"{currency}/USDT reference price not set — tap 💱 Set {currency}/USDT Ref"
        raw = token_price * local_usdt_ref * float_pct / 100
    elif currency == "USD":
        # USD: token_price already in USD
        raw = token_price * float_pct / 100
    else:
        # Unknown currency — treat as direct
        raw = token_price * float_pct / 100

    return str(Decimal(str(raw)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)), None


async def _try_fast_chase(bot, chat_id, sess, slot_idx, ad_data, s, float_pct, creds, _quant):
    """
    Called once per tick, for this ad slot, by the shared per-user
    _fast_chase_coordinator (see _FAST_CHASE_POLL_SECS) — every active
    floating-mode ad slot for a user is checked on the SAME tick, in
    Ad 1 → Ad 2 → Ad 3 order, rather than each slot running its own
    independent timer. Budget (_can_modify_slot/_record_modify_slot) and
    the chase-ceiling reference (_ceiling_ref/_pending_ceiling) are still
    tracked PER SLOT, since Bybit enforces its modify-rate limit per
    ad_id, not per user — Ad 2's fast-chase doesn't eat into Ad 1's budget
    or vice versa. Where two ads share the same (currency, coin) pair and
    could otherwise land on the same computed/discovered price,
    _resolve_price_collision() is applied before every submission here,
    the same guard the scheduled cycle already uses.

    Pulls from the SAME rolling modify budget as this ad's own scheduled
    cycle (_can_modify_slot/_record_modify_slot, keyed by slot_idx) so the
    two together can never exceed Bybit's modifies-per-5-minutes limit on
    this one ad.

    Two different modes, handled separately:

    PLAIN FLOATING (not at this pair's max %): the formula price IS the
    real candidate. Only bother Bybit if it's risen by at least the
    fast-chase gap since the last post.

    CHASE-CEILING (float % at this pair's max): the formula price is a
    deliberate overshoot — the only way to learn Bybit's real current
    boundary is to probe and read the rejection. That boundary can
    legitimately move DOWN as the order book shifts, not just up, so it
    gets checked against the ad's own current price AFTER being
    discovered, before ever being submitted as a real update. This is the
    fix for a real bug: it used to trust whatever boundary Bybit reported
    and repost it unconditionally — which meant reposting the exact same
    price twice in a row, then even a LOWER price, every ~10-40 seconds,
    each one logged as if it were a genuine rise. Now a discovered
    boundary is only ever posted if it's at least one gap ABOVE what's
    already live; otherwise nothing is sent and the ad is left exactly
    where it is — the whole point of chase mode is to keep the ad on the
    highest price seen so far, never to drift it down.

    RELIABILITY: every skip/decision point below is logged (why it did or
    didn't act), and the whole function is wrapped in try/except so a
    single bad response, timeout, or bug here can NEVER kill the parent
    auto_update_loop task — it used to be able to, silently, since nothing
    upstream caught exceptions raised from here. A failure past the budget
    check is still treated as "skip, try again next cycle" rather than
    counted towards the scheduled-cycle auto-stop counter — this is a
    bonus opportunistic update, not a scheduled cycle.
    """
    lock = _fast_chase_lock(sess, slot_idx)
    if lock.locked():
        logger.info(f"[FastChase u={chat_id} ad={s.get('ad_id')}] skipped — a check is already in progress for this ad (defense-in-depth against overlapping runs)")
        return
    async with lock:
        tag = f"[FastChase u={chat_id} ad={s.get('ad_id')}]"
        try:
            logger.info(f"{tag} poll start")

            if not _can_modify_slot(sess, slot_idx):
                logger.info(
                    f"{tag} skipped — modify budget exhausted "
                    f"({len(_modify_times(sess, slot_idx))}/{_FAST_CHASE_BUDGET} used in the last "
                    f"{_FAST_CHASE_WINDOW_SECS}s window, shared with this ad's scheduled cycle)"
                )
                return

            local_usdt_ref = float(sess.shared_local_usdt_ref or s.get("local_usdt_ref") or 0)

            # calc_floating_price() ultimately calls requests.get() against Bybit's
            # public ticker endpoint — a BLOCKING network call. Calling it directly
            # on the event loop (as before) stalls the entire bot — every other
            # user's monitor, every Telegram reply — for however long that HTTP
            # call takes, with no exception and therefore no log line. Running it
            # in the executor is what actually makes "the loop never stops
            # unless disabled" true.
            new_p_str, err = await asyncio.get_event_loop().run_in_executor(
                _ad_executor, calc_floating_price, ad_data, float_pct, local_usdt_ref
            )
            if err:
                logger.warning(f"{tag} skipped — spot price fetch failed: {err}")
                return

            new_p = Decimal(new_p_str)
            cur_p = _ad_current_price(sess, slot_idx)
            logger.info(f"{tag} spot-derived price={new_p} | current ad price={cur_p}")
            if cur_p <= 0:
                logger.warning(f"{tag} skipped — no known current ad price yet")
                return

            currency = ad_data.get("currencyId", "")
            token    = ad_data.get("tokenId", "")
            gap      = _fast_chase_gap(currency, token, new_p, slot_idx)

            chase_ceiling = _wants_live_ceiling(currency, token, float_pct)
            posted_price  = None
            collided_with = []   # which other ad(s)/self this price had to dodge, for the user-facing notice
            # The pre-collision "natural" price, captured at whichever call
            # site actually invokes _resolve_price_collision. Used only to
            # compute the REAL total nudge (natural - final) for the
            # user-facing message, instead of re-deriving a nominal gap
            # from the already-adjusted price (see message block below).
            natural_before_collision = None

            if not chase_ceiling:
                # Plain floating mode: the formula number is the real candidate.
                diff = new_p - cur_p
                logger.info(f"{tag} plain-floating | diff={diff} threshold={gap}")
                if diff < gap:
                    logger.info(f"{tag} skipped — diff {diff} below the {gap} threshold, not worth a post yet")
                    return
                if new_p.quantize(_quant, rounding=ROUND_HALF_UP) <= cur_p.quantize(_quant, rounding=ROUND_HALF_UP):
                    logger.info(f"{tag} skipped — move rounds to the price already live ({cur_p})")
                    return
                # Collision guard — with fast-chase now able to run on Ad 2/3 as
                # well as Ad 1, two ads on the same pair can independently see
                # the same threshold-clearing spot move at nearly the same
                # moment and compute the same candidate price. This steps this
                # ad's price below any other active ad on the same pair before
                # ever submitting — same guard, same convention, as the
                # scheduled cycle already uses.
                resolved_p = new_p
                if _multi_ad(sess):
                    natural_before_collision = new_p
                    resolved_p, collided_with = _resolve_price_collision(sess, slot_idx, currency, token, new_p)
                submit_price = resolved_p.quantize(_quant, rounding=ROUND_HALF_UP)
                _record_modify_slot(sess, slot_idx)
                logger.info(f"{tag} threshold met — submitting {submit_price}")
                result = await asyncio.get_event_loop().run_in_executor(
                    _ad_executor, modify_ad, s["ad_id"], str(submit_price), ad_data, creds
                )
                code = result.get("retCode", result.get("ret_code", -1))
                if await _handle_ad_ip_error(bot, chat_id, sess, slot_idx, code, result.get("retMsg", result.get("ret_msg", ""))):
                    return
                if code == 0:
                    posted_price = submit_price
                    logger.info(f"{tag} modify accepted — new ad price {posted_price}")
                elif code == 90043 and _can_modify_slot(sess, slot_idx):
                    if collided_with:
                        # This price was ALREADY pushed down to sit correctly
                        # below a senior ad (the $5/₦7,000 — or $10/₦14,000 for
                        # Ad 3 — spacing that's confirmed to work against
                        # Bybit's own real minimum). That offset IS the
                        # intended gap. Applying a SECOND, independent
                        # real_gap nudge on top of it here would double-
                        # deduct — Ad 2 landing $10 below Ad 1 instead of the
                        # intended $5, which is exactly the bug this guards
                        # against. If even the correctly-spaced price still
                        # looks like a duplicate to Bybit, the tracked price
                        # is just stale — resync directly instead of
                        # squeezing further.
                        logger.info(f"{tag} collision-adjusted price also hit 90043 — resyncing instead of stacking a second nudge")
                        await _resync_price_from_bybit(s["ad_id"], creds, sess, slot_idx, submit_price, tag)
                    else:
                        # Self-duplicate — this ad's own new price rounds to
                        # what's already live (can happen if the market barely
                        # moved since the last successful post). Nudge by the
                        # REAL minimum gap Bybit actually enforces (get_min_price_gap
                        # — $5/₦7,000, confirmed by manual testing), NOT `gap`
                        # (the $0.50/₦1,500 fast-chase reaction threshold used
                        # just above). Those answer different questions — "was
                        # this market move worth trying" vs "how far do I
                        # actually have to move to stop looking like a
                        # duplicate to Bybit" — and nudging by the tiny
                        # reaction threshold here was just retrying a value
                        # barely different from the one that already got
                        # rejected. Only reached when collided_with is empty
                        # (no senior ad involved), so there's no risk of
                        # stacking this on top of a collision offset.
                        real_gap = get_min_price_gap(currency, token, submit_price)
                        nudged = (submit_price - real_gap).quantize(_quant, rounding=ROUND_HALF_UP)
                        logger.info(f"{tag} modify got 90043 (self-duplicate) — retrying at {nudged}")
                        _record_modify_slot(sess, slot_idx)
                        nudge_result = await asyncio.get_event_loop().run_in_executor(
                            _ad_executor, modify_ad, s["ad_id"], str(nudged), ad_data, creds
                        )
                        if nudge_result.get("retCode", nudge_result.get("ret_code", -1)) == 0:
                            posted_price = nudged
                            logger.info(f"{tag} nudged modify accepted — new ad price {posted_price}")
                        else:
                            logger.warning(
                                f"{tag} nudged modify also rejected — code="
                                f"{nudge_result.get('retCode', nudge_result.get('ret_code', -1))} "
                                f"msg={nudge_result.get('retMsg', nudge_result.get('ret_msg',''))!r}"
                            )
                            if nudge_result.get("retCode", nudge_result.get("ret_code", -1)) == 90043:
                                await _resync_price_from_bybit(s["ad_id"], creds, sess, slot_idx, nudged, tag)
                else:
                    logger.warning(
                        f"{tag} modify rejected — code={code} "
                        f"msg={result.get('retMsg', result.get('ret_msg',''))!r}"
                    )

            else:
                # Chase-ceiling needs TWO calls to ever actually change anything:
                # the probe (guaranteed to be rejected, purely to learn Bybit's
                # current boundary) and the follow-up post of that boundary. If
                # only one slot is left in the shared budget, spending it on the
                # probe burns the last slot on a call that can NEVER succeed by
                # itself — the follow-up that would have actually raised the
                # price then gets silently skipped for lack of budget. That's
                # exactly what was happening: a single "MODIFY... 912120022"
                # logged, then nothing — the discovered boundary was a real $21+
                # improvement, but there was no budget left to post it. Wait for
                # 2 free slots instead of spending on a probe we can't follow up.
                #
                # THE ACTUAL FIX: unlike plain-floating mode (which only spends
                # budget once diff >= gap against the ad's own current price),
                # chase-ceiling had NO local gate at all — it asked Bybit
                # (spending 1-2 of the 8 shared slots) on every single poll,
                # whether or not the market had moved at all. That's what burns
                # the budget down to nothing within the first minute, timer-
                # throttled or not.
                #
                # get_token_usdt_price()/calc_floating_price() hit Bybit's PUBLIC
                # market endpoint (no API key, no signature) — it does not touch
                # the account's rate limit at all, only modify_ad() does. So we
                # can fetch that price as often as we like for free, and use it
                # to decide WHETHER to spend a Bybit call, the same way plain-
                # floating mode already does.
                #
                # Two pieces of memory, kept deliberately separate:
                #   last_known_ceiling  — a boundary Bybit ITSELF has told us,
                #                         via a real rejection message. This is
                #                         confirmed information regardless of
                #                         whether we managed to post it.
                #   pending_ceiling     — set only when we discovered a real
                #                         improvement but couldn't post it (ran
                #                         out of budget, or the post itself was
                #                         rejected) — something worth trying
                #                         again with a plain 1-call post before
                #                         ever spending 2 calls on a fresh probe.
                # cur_p (the ad's actual live price on Bybit, only ever updated
                # on a CONFIRMED successful post) is never touched by either of
                # these — that bug (comparing against an attempted-but-unposted
                # price as if it were live) is exactly what made the bot think
                # nothing had changed while the real Bybit ad sat at its old,
                # lower, unconfirmed price.
                pending_ceiling = _pending_ceiling(sess, slot_idx)
                last_known      = _ceiling_ref(sess, slot_idx)

                # Prefer an explicit pending ceiling, but ALSO fall back to the
                # last CONFIRMED ceiling if the ad's own live price has simply
                # fallen behind it. This is the fix for a real bug: pending_
                # ceiling only gets set when a discovered boundary was actually
                # attempted and rejected/skipped in the SAME cycle it was
                # found. If that attempt was never made at all (e.g. the
                # candidate cleared the gap against cur_p at discovery time but
                # something else short-circuited before the post), last_known
                # still holds a real, Bybit-confirmed number the ad has never
                # caught up to — and the old code only ever compared FRESH
                # spot movement against last_known, never the ad's own current
                # price against it. That let an ad sit $9+ below a ceiling
                # Bybit had already told it was valid, waiting for the market
                # to rise even further before trying again.
                actionable_ceiling = None
                if pending_ceiling is not None and pending_ceiling - cur_p >= gap:
                    actionable_ceiling = pending_ceiling
                elif last_known is not None and last_known - cur_p >= gap:
                    _prior_attempt = _last_ceiling_attempt(sess, slot_idx)
                    if _prior_attempt is not None and _prior_attempt > last_known:
                        # STALE HIGH-WATER MARK — _last_ceiling_attempt only
                        # ever gets written when we act on a discovery, so it
                        # can end up recording a HIGHER historical peak than
                        # the CURRENT last_known if the real ceiling has since
                        # legitimately dropped (a market dip, or a corrective
                        # post landing lower) — last_known correctly tracks
                        # that drop, but this high-water mark doesn't. Left
                        # unclamped, the gate below compares fresh movement
                        # against that stale, artificially-high peak forever,
                        # permanently refusing to probe again until the
                        # market claws all the way back past a number that's
                        # no longer the real ceiling — confirmed in
                        # production: an ad stuck skipping for minutes while
                        # sitting genuinely far below its OWN current,
                        # legitimate last_known. Once last_known itself has
                        # moved past the old high-water mark, that mark is
                        # simply irrelevant history — drop it.
                        _prior_attempt = None
                    _required = _required_fresh_move(sess, slot_idx, currency, token, last_known, gap)
                    if _prior_attempt is not None and last_known - _prior_attempt < _required:
                        # Already tried this raw ceiling recently. With
                        # _set_ad_current_price now leaving ceiling_ref alone
                        # on a collision-adjusted post, last_known correctly
                        # keeps holding the TRUE higher ceiling instead of
                        # getting dragged down to this ad's own nudged
                        # price — but that means last_known - cur_p is now
                        # PERMANENTLY >= gap for any ad sitting below a
                        # senior sibling by design, not just when the market
                        # actually moves. Gating on "any improvement at all"
                        # wasn't enough either — ordinary tick jitter clears
                        # that easily. Only act again once the raw ceiling
                        # has risen past what we already tried by THIS ad's
                        # own real spacing requirement ($9/$18, or the tiny
                        # reaction gap for the senior-most ad).
                        logger.info(
                            f"{tag} chase-ceiling | {last_known - cur_p} below known ceiling {last_known}, "
                            f"but that's this ad's own collision-avoidance gap (already tried {last_known}) "
                            f"— not fresh movement (need {_required} more), skipping until it rises further"
                        )
                    else:
                        actionable_ceiling = last_known
                        logger.info(
                            f"{tag} chase-ceiling | live price {cur_p} has room to catch up to the last "
                            f"confirmed ceiling {last_known} (diff={last_known - cur_p} >= {gap}) — "
                            f"retrying it directly instead of waiting on fresh spot movement"
                        )

                if actionable_ceiling is not None:
                    if not _can_modify_slot(sess, slot_idx):
                        logger.info(f"{tag} skipped — have a known unposted ceiling {actionable_ceiling} but no budget yet")
                        return
                    _set_last_ceiling_attempt(sess, slot_idx, actionable_ceiling)
                    # Same collision guard the scheduled cycle uses — with fast-
                    # chase now running on Ad 2/3 too, two ads on the same pair
                    # could otherwise land on the same discovered ceiling at
                    # nearly the same moment and one of them gets rejected.
                    natural_before_collision = actionable_ceiling
                    resolved_ceiling, collided_with = _resolve_price_collision(sess, slot_idx, currency, token, actionable_ceiling)
                    logger.info(f"{tag} retrying known ceiling {resolved_ceiling} directly (1 call, no re-probe)")
                    _record_modify_slot(sess, slot_idx)
                    retry_result = await asyncio.get_event_loop().run_in_executor(
                        _ad_executor, modify_ad, s["ad_id"], str(resolved_ceiling), ad_data, creds
                    )
                    retry_code = retry_result.get("retCode", retry_result.get("ret_code", -1))
                    if retry_code == 0:
                        posted_price = resolved_ceiling
                        logger.info(f"{tag} pending ceiling post accepted — new ad price {posted_price}")
                    elif retry_code == 90043:
                        # Bybit says this price "differs from your existing
                        # ad by less than 0%" — our tracked cur_p disagreed
                        # (that's WHY we thought this was worth posting), so
                        # it's fallen out of sync with reality. But the
                        # rejection alone doesn't PROVE resolved_ceiling is
                        # what's actually live — ask Bybit directly instead
                        # of guessing (see _resync_price_from_bybit; this is
                        # the exact spot that produced the Ad 2/Ad 3 stall —
                        # repeated 90043s while the tracked price silently
                        # drifted from a chain of unconfirmed guesses).
                        await _resync_price_from_bybit(s["ad_id"], creds, sess, slot_idx, resolved_ceiling, tag)
                    else:
                        # Still couldn't post it (budget/collision/transient) —
                        # remember it so the next check retries directly again.
                        _set_pending_ceiling(sess, slot_idx, actionable_ceiling)
                        logger.warning(f"{tag} pending ceiling post still rejected — code={retry_code} — will keep retrying as budget allows")

                else:
                    if last_known is not None:
                        formula_diff = new_p - last_known
                        logger.info(
                            f"{tag} chase-ceiling | fetched price={new_p} vs last known real ceiling={last_known} "
                            f"diff={formula_diff} threshold={gap}"
                        )
                        if formula_diff < gap:
                            logger.info(
                                f"{tag} skipped — underlying price hasn't moved enough above the last "
                                f"confirmed Bybit ceiling to be worth spending a call (no API request made)"
                            )
                            return
                        _prior_attempt = _last_ceiling_attempt(sess, slot_idx)
                        if _prior_attempt is not None and _prior_attempt > last_known:
                            # STALE HIGH-WATER MARK — same issue as the
                            # actionable_ceiling branch above: this can hold
                            # a HIGHER historical peak than the CURRENT
                            # last_known if the real ceiling has since
                            # legitimately dropped. Confirmed as the exact
                            # cause of an ad sitting stuck for minutes,
                            # perpetually "not fresh enough", while genuinely
                            # $20-30+ behind its own current last_known —
                            # new_p was being compared against an old peak
                            # (from before a market dip) that was HIGHER
                            # than new_p itself, so no amount of real
                            # movement could ever clear it. Once last_known
                            # itself has moved past the old high-water mark,
                            # that mark is irrelevant history — drop it.
                            _prior_attempt = None
                        if _prior_attempt is not None:
                            _required = _required_fresh_move(sess, slot_idx, currency, token, new_p, gap)
                            if new_p - _prior_attempt < _required:
                                # We already ran a full probe off essentially this
                                # same underlying ceiling recently. Gated on THIS
                                # ad's own real spacing requirement, not one flat
                                # number for every slot — ordinary BTC tick jitter
                                # between polls routinely exceeds a few dollars on
                                # its own, with the REAL account-wide ceiling not
                                # having moved at all, which is exactly what let
                                # 2-3 wasted probe-and-repost cycles through in a
                                # row for the identical final price (confirmed in
                                # production logs). A one-gap-junior (Ad 2) needs
                                # its own $9/₦12,600 of real movement before a
                                # re-probe could possibly land somewhere different;
                                # a two-gap-junior (Ad 3) needs $18/₦25,200; the
                                # senior-most ad needs none of this extra gating.
                                logger.info(
                                    f"{tag} skipped — still within {_required} of the ceiling we already probed and "
                                    f"resolved a post from ({_prior_attempt}), not fresh enough for another probe"
                                )
                                return
                    if not _can_modify_slot(sess, slot_idx, need=2):
                        logger.info(f"{tag} skipped — chase-ceiling needs 2 free budget slots, don't have them")
                        return
                    # Probe first (always — the natural formula number tells us
                    # nothing about where Bybit's real boundary currently sits),
                    # then decide whether the DISCOVERED boundary is worth
                    # posting — never the probe itself.
                    # 50x (not 5x) — confirmed in production that 5x wasn't
                    # always enough to force Bybit's out-of-range rejection:
                    # some ads' real accepted price band is wider than a flat
                    # 5x multiplier assumes, so an inflated-but-not-inflated-
                    # ENOUGH probe was briefly accepted outright and went
                    # live at ~5x its real price before the correction below
                    # caught it. 50x pushes the probe far beyond any
                    # realistic accepted band so that acceptance case should
                    # no longer happen at all — the correction logic stays
                    # in place regardless, as a second layer of defense.
                    probe_price = (new_p * Decimal("50")).quantize(_quant, rounding=ROUND_HALF_UP)
                    _record_modify_slot(sess, slot_idx)
                    logger.info(f"{tag} chase-ceiling probe — submitting {probe_price}")
                    result = await asyncio.get_event_loop().run_in_executor(
                        _ad_executor, modify_ad, s["ad_id"], str(probe_price), ad_data, creds
                    )
                    last_code = result.get("retCode", result.get("ret_code", -1))
                    last_msg  = result.get("retMsg",  result.get("ret_msg","Unknown"))
                    logger.info(f"{tag} probe result code={last_code} msg={last_msg!r}")
                    if await _handle_ad_ip_error(bot, chat_id, sess, slot_idx, last_code, last_msg):
                        return

                    if last_code == 0:
                        # Extremely unlikely — the 50x probe was accepted
                        # outright instead of triggering the out-of-range
                        # rejection it's designed to guarantee. A 50x-inflated
                        # number is NEVER a real, usable price under any
                        # circumstance — it exists purely to be rejected.
                        # Leaving it live actually mispriced real ads on
                        # Bybit in production (confirmed: an ad stuck at
                        # several times its real value, permanently, since
                        # fast-chase then compares future spot prices against
                        # this nonsense number and never finds room to act
                        # again). Correct it back to the real target
                        # immediately instead of accepting it.
                        logger.warning(f"{tag} 50x probe price {probe_price} was UNEXPECTEDLY accepted — correcting immediately to the real target {new_p}, never leaving an inflated price live")
                        if _can_modify_slot(sess, slot_idx):
                            _record_modify_slot(sess, slot_idx)
                            correction = await asyncio.get_event_loop().run_in_executor(
                                _ad_executor, modify_ad, s["ad_id"], str(new_p), ad_data, creds
                            )
                            if correction.get("retCode", correction.get("ret_code", -1)) == 0:
                                posted_price = new_p
                                logger.info(f"{tag} corrected back to real target {new_p} after unexpected probe acceptance")
                            else:
                                # Correction failed too — do NOT record the
                                # inflated price as current either way, and
                                # do NOT claim a successful post. Loudly flag
                                # it so a human checks Bybit directly rather
                                # than silently leaving a wrong live price.
                                await bot.send_message(chat_id=chat_id,
                                    text=(
                                        f"🚨 <b>{_ad_slot_label(slot_idx)}</b>\n"
                                        f"Fast-chase's ceiling probe was unexpectedly accepted at <code>{probe_price}</code> "
                                        f"and the automatic correction back to <code>{new_p}</code> ALSO failed. "
                                        f"This ad may be live on Bybit at the wrong price right now — please check it manually."
                                    ),
                                    parse_mode="HTML")
                        else:
                            await bot.send_message(chat_id=chat_id,
                                text=(
                                    f"🚨 <b>{_ad_slot_label(slot_idx)}</b>\n"
                                    f"Fast-chase's ceiling probe was unexpectedly accepted at <code>{probe_price}</code> "
                                    f"but there's no modify budget left to correct it this instant. "
                                    f"This ad is live on Bybit at the wrong price right now — please check it manually."
                                ),
                                parse_mode="HTML")
                    elif last_code == 912120022:
                        min_str, max_str = _extract_bybit_bounds(last_msg)
                        bound_str = max_str if max_str else min_str
                        if not bound_str:
                            logger.warning(f"{tag} could not parse a boundary out of msg={last_msg!r}")
                        else:
                            candidate = Decimal(bound_str)
                            # Confirmed information from Bybit itself — record it
                            # regardless of what happens with the follow-up post.
                            _set_ceiling_ref(sess, slot_idx, candidate)
                            logger.info(f"{tag} discovered boundary={candidate} vs current={cur_p} threshold={gap}")
                            if candidate - cur_p >= gap and _can_modify_slot(sess, slot_idx):
                                # Same collision guard as above — with 2+ ads on
                                # the same pair fast-chasing independently, the
                                # discovered boundary can coincide with another
                                # ad's live price even though it clears the gap
                                # against THIS ad's own last price.
                                natural_before_collision = candidate
                                _set_last_ceiling_attempt(sess, slot_idx, candidate)
                                post_candidate, collided_with = _resolve_price_collision(sess, slot_idx, currency, token, candidate)
                                _record_modify_slot(sess, slot_idx)
                                retry_result = await asyncio.get_event_loop().run_in_executor(
                                    _ad_executor, modify_ad, s["ad_id"], str(post_candidate), ad_data, creds
                                )
                                retry_code = retry_result.get("retCode", retry_result.get("ret_code", -1))
                                if retry_code == 0:
                                    posted_price = post_candidate
                                    logger.info(f"{tag} boundary post accepted — new ad price {posted_price}")
                                elif retry_code == 90043 and _can_modify_slot(sess, slot_idx):
                                    if collided_with:
                                        # This candidate was ALREADY pushed down to sit
                                        # correctly below a senior ad — that offset IS
                                        # the intended, Bybit-tested $5/₦7,000 (or
                                        # $10/₦14,000 for Ad 3) spacing. A second,
                                        # independent real_gap nudge on top of it here
                                        # is exactly what compounded Ad 2's spacing
                                        # into $10 below Ad 1 instead of the intended
                                        # $5 — confirmed in production logs. If even
                                        # the correctly-spaced candidate still looks
                                        # like a duplicate to Bybit, the tracked price
                                        # is just stale — resync instead of squeezing
                                        # further.
                                        logger.info(f"{tag} collision-adjusted boundary also hit 90043 — resyncing instead of stacking a second nudge")
                                        await _resync_price_from_bybit(s["ad_id"], creds, sess, slot_idx, post_candidate, tag)
                                    else:
                                        # Discovered boundary matches what's live after all —
                                        # nudge once, using the REAL minimum gap Bybit
                                        # enforces (get_min_price_gap — $5/₦7,000), not
                                        # `gap` (the $0.50/₦1,500 fast-chase reaction
                                        # threshold) — same distinction as the
                                        # plain-floating branch above. Still gated by
                                        # the same "is it actually an improvement"
                                        # check so this can't sneak in a downgrade.
                                        # Only reached when collided_with is empty (no
                                        # senior ad involved), so no risk of stacking
                                        # this on top of a collision offset.
                                        real_gap = get_min_price_gap(currency, token, post_candidate)
                                        nudged = (post_candidate - real_gap).quantize(_quant, rounding=ROUND_HALF_UP)
                                        if nudged - cur_p >= gap:
                                            _record_modify_slot(sess, slot_idx)
                                            nudge_result = await asyncio.get_event_loop().run_in_executor(
                                                _ad_executor, modify_ad, s["ad_id"], str(nudged), ad_data, creds
                                            )
                                            nudge_code = nudge_result.get("retCode", nudge_result.get("ret_code", -1))
                                            if nudge_code == 0:
                                                posted_price = nudged
                                                logger.info(f"{tag} nudged post accepted — new ad price {posted_price}")
                                            else:
                                                logger.warning(f"{tag} nudged post also rejected — code={nudge_code}")
                                                if nudge_code == 90043:
                                                    await _resync_price_from_bybit(s["ad_id"], creds, sess, slot_idx, nudged, tag)
                                else:
                                    # Discovered a real improvement but couldn't post it
                                    # right now (out of budget, or a transient rejection).
                                    # Remember it so the NEXT check can try posting it
                                    # directly for 1 call instead of re-probing for 2.
                                    _set_pending_ceiling(sess, slot_idx, candidate)
                                    logger.warning(
                                        f"{tag} boundary post rejected/skipped — code={retry_code} — "
                                        f"remembered {candidate} as a pending ceiling to retry"
                                    )
                            else:
                                logger.info(
                                    f"{tag} skipped — Bybit's real ceiling hasn't risen enough above what's "
                                    f"already live (candidate-cur={candidate - cur_p} < {gap}, or budget exhausted)"
                                )
                    elif last_code == 912300014:
                        # "Error retrieving fiat amount parameter" — seen on
                        # the 50x-inflated PROBE price itself, not on a real
                        # target price. This isn't Bybit telling us the price
                        # is out of range (that's 912120022, handled above);
                        # it's Bybit's backend failing to compute this ad's
                        # min/maxAmount fiat bounds against such an extreme
                        # price at all, which points at the ad's own
                        # minAmount/maxAmount/quantity combination being at
                        # or near Bybit's own limits — the 50x probe just
                        # happens to be what exposes it. No boundary can be
                        # discovered from this response, so nothing is
                        # posted this cycle, but it's surfaced to the user
                        # (once, not every poll) since it otherwise looks
                        # identical to "nothing happened" in the logs while
                        # actually meaning fast-chase can't probe this ad at
                        # all until its amount range is fixed on Bybit.
                        logger.warning(
                            f"{tag} probe hit fiat-amount-parameter error (912300014) at probe price "
                            f"{probe_price} — likely this ad's minAmount/maxAmount/quantity can't be "
                            f"validated against such an inflated price; no boundary discovered this cycle"
                        )
                        if not _fiat_amount_error_already_notified(sess, slot_idx):
                            _set_fiat_amount_error_notified(sess, slot_idx, True)
                            await bot.send_message(chat_id=chat_id,
                                text=(
                                    f"⚠️ <b>{_ad_slot_label(slot_idx)} — fast-chase probe error (912300014)</b>\n\n"
                                    f"Bybit rejected fast-chase's price probe with "
                                    f"<code>Error retrieving fiat amount parameter</code>, not a normal "
                                    f"out-of-range rejection. This usually means this ad's Min/Max "
                                    f"transaction amount (or quantity) is set in a way Bybit can't validate "
                                    f"against fast-chase's probe price.\n\n"
                                    f"👉 Check/adjust this ad's Min/Max Amount directly on Bybit — fast-chase "
                                    f"will keep skipping this ceiling-discovery step until it's resolved.\n"
                                    f"<i>(This notice only shows once per run for this ad.)</i>"
                                ),
                                parse_mode="HTML")
                    else:
                        logger.warning(f"{tag} probe returned an unexpected code={last_code} msg={last_msg!r}")

            if posted_price is not None:
                _set_ad_current_price(sess, slot_idx, posted_price, collision_adjusted=bool(collided_with))
                # A real, confirmed modify just happened for this slot — this
                # is what Bybit itself would restart its own 5-min countdown
                # on. Mirror that by restarting this slot's own scheduled-
                # cycle timer too (see auto_update_loop's wait phase).
                _touch_last_fast_modify(sess, slot_idx)
                now   = datetime.now().strftime("%H:%M:%S")
                label = _ad_slot_label(slot_idx)
                if collided_with:
                    # Explain the deduction instead of leaving a lower-than-expected
                    # price unexplained — e.g. "Fast update for Ad 2 matched Ad 1's
                    # price — nudged by $3 to post 84276.08 for Ad 2".
                    #
                    # This reports the ACTUAL total amount subtracted
                    # (natural price computed before collision resolution
                    # minus what was actually posted), not a nominal gap
                    # re-derived from the final price. The two used to be
                    # able to disagree — e.g. the message would say "nudged
                    # by 3" while the real drop was 6 or more — because the
                    # old code recalculated a fresh "gap" for display
                    # instead of reporting what collision resolution
                    # actually did.
                    if natural_before_collision is not None:
                        gap_amt = natural_before_collision - posted_price
                    else:
                        gap_amt = get_min_price_gap(currency, token, posted_price)
                    reason  = " and ".join(collided_with)
                    text = (
                        f"⚡ <b>Fast update — {label}</b> <code>{now}</code>\n"
                        f"⚠️ Matched {reason} — nudged by {gap_amt} to stay clear\n"
                        f"💲 <code>{posted_price}</code> posted for {label}"
                    )
                else:
                    text = (
                        f"⚡ <b>Fast update — {label}</b> <code>{now}</code>\n"
                        f"💲 <code>{posted_price}</code> — price rose before the next scheduled cycle"
                    )
                _send_ok, _held = _edit_notice(s, _ad_interval_seconds(s, ad_data))
                if _send_ok:
                    await _safe_send(bot, chat_id=chat_id, text=text + _edit_notice_suffix(_held), parse_mode="HTML")
            else:
                logger.info(f"{tag} poll complete — no update posted")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            # This is the critical guarantee: nothing raised in this function can
            # ever escape and take the parent auto_update_loop task down with it.
            logger.error(f"{tag} unhandled exception: {e}", exc_info=True)


# ─────────────────────────────────────────
# 💳 AUTO-PAY (silent background worker)
# ─────────────────────────────────────────
# Polls Bybit's pending-orders endpoint every 60s. Any NEW pending BUY order
# is marked as paid automatically. Nothing is sent to the user's chat for
# normal orders — only a one-off notice if the API key is missing or the
# server IP is not whitelisted. Runs until the user switches it off.
_AUTOPAY_POLL_SECS = 60
_AUTOPAY_MAX_ATTEMPTS = 3


async def auto_pay_loop(bot, chat_id: int):
    sess = _s(chat_id)
    logger.info(f"💳 AUTO-PAY STARTED for user {chat_id}")
    loop = asyncio.get_event_loop()
    ip_notified = False
    attempts: dict = {}   # order_id -> failed mark-paid attempts

    while sess.auto_pay_enabled:
        try:
            creds = get_user_creds(chat_id)
            if not is_admin(chat_id) and not (creds and creds.get("key")):
                sess.auto_pay_enabled = False
                await _safe_send(bot, chat_id=chat_id, parse_mode="HTML", text=(
                    "❌ <b>Auto-Pay stopped.</b>\n\n"
                    f"No Bybit API key found for Account {_get_user_slot_str(chat_id)}.\n"
                    "Add it in 🔑 <b>Set APIs</b>, then switch Auto-Pay on again."
                ))
                break

            res = await loop.run_in_executor(None, partial(get_pending_orders, creds=creds))
            rc  = res.get("retCode", res.get("ret_code", -1))
            msg = res.get("retMsg",  res.get("ret_msg", ""))
            if rc != 0:
                if rc == 10010 or "IP" in str(msg).upper():
                    if not ip_notified:
                        ip_notified = True
                        ip = await _get_current_ip()
                        await _safe_send(bot, chat_id=chat_id, parse_mode="HTML", text=(
                            "🚫 <b>Auto-Pay: Bybit IP Whitelist Error (10010)</b>\n\n"
                            f"Add <code>{_esc(ip)}</code> to your Bybit API key's IP whitelist.\n"
                            "Auto-Pay stays ON and will keep retrying every 60s."
                        ))
                else:
                    logger.warning(f"[AutoPay] pending-orders failed for {chat_id}: {rc} {msg}")
                await asyncio.sleep(_AUTOPAY_POLL_SECS)
                continue
            ip_notified = False

            items = res.get("result", {}).get("items", []) or []
            new_ids = [i.get("id") for i in items
                       if i.get("id") and i.get("id") not in sess.seen_order_ids]
            for n, oid in enumerate(new_ids):
                if not sess.auto_pay_enabled:
                    break
                try:
                    det = await loop.run_in_executor(None, partial(get_order_detail, oid, creds=creds))
                    if det.get("retCode", -1) != 0:
                        raise RuntimeError(f"order detail: {det.get('retMsg')}")
                    od = det.get("result", {}) or {}
                    if str(od.get("status", "")) != "10":      # only orders still waiting for payment
                        sess.seen_order_ids.add(oid)
                        continue
                    pt  = _resolve_pay_term(od)
                    ptype, pid = str(pt.get("paymentType", "")), str(pt.get("id", ""))
                    if not (ptype and pid):
                        raise RuntimeError("no payment term on order")
                    pr = await loop.run_in_executor(
                        None, partial(mark_order_paid, oid, ptype, pid, creds=creds))
                    if pr.get("retCode", -1) == 0:
                        sess.seen_order_ids.add(oid)
                        sess.paid_order_ids.add(oid)
                        logger.info(f"[AutoPay] user {chat_id} order {oid} marked paid")
                    else:
                        raise RuntimeError(f"mark paid: {pr.get('retMsg')}")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    attempts[oid] = attempts.get(oid, 0) + 1
                    logger.warning(f"[AutoPay] user {chat_id} order {oid} attempt {attempts[oid]} failed: {e}")
                    if attempts[oid] >= _AUTOPAY_MAX_ATTEMPTS:
                        sess.seen_order_ids.add(oid)   # give up; stop retrying this order
                if n < len(new_ids) - 1:
                    await asyncio.sleep(1.2)           # stay under Bybit's read rate limit
        except asyncio.CancelledError:
            logger.info(f"[AutoPay] cancelled for user {chat_id}")
            break
        except Exception as e:
            logger.error(f"[AutoPay] loop error for user {chat_id}: {e}")
        await asyncio.sleep(_AUTOPAY_POLL_SECS)

    logger.info(f"💳 AUTO-PAY STOPPED for user {chat_id}")


# ─────────────────────────────────────────
# 🔄 PRICE UPDATE LOOP
# ─────────────────────────────────────────
async def auto_update_loop(bot, chat_id, slot_idx: int = -1):
    """
    Runs one ad's price-update cycle. slot_idx=-1 is Ad 1 (the original
    single-ad behavior, completely unchanged); 0/1 are Ad 2/Ad 3.

    Auto-stop-on-failure: when a user has MORE THAN ONE ad active, two
    consecutive failed updates on this specific ad auto-stops just this
    slot and notifies the user to fix it on Bybit directly. This is now
    only a fallback for genuinely unexpected errors — the two known,
    recoverable cases are handled directly instead of counting as
    failures: 912120022 (out-of-range — retried with Bybit's own stated
    max) and 90043 (new price rounds to the same value the ad already has
    live — retried with a small nudge). Single-ad users keep the original
    behavior on any other error: it keeps retrying forever, since there's
    no other ad's price at risk for them.
    """
    sess = _s(chat_id)
    _set_ad_running(sess, slot_idx, True)
    _ensure_fast_chase_coordinator(bot, chat_id)   # shared timer covers this slot now — see note above
    label     = _ad_slot_label(slot_idx)
    s         = _ad_settings(sess, slot_idx)
    ad_data   = _ad_data_of(sess, slot_idx)
    interval  = s.get("interval", 2)
    # Seconds-resolution interval. Non-USDT ads resolve to interval*60
    # exactly as before; USDT/USD ads may carry a finer interval_secs
    # (floor 5s). Every wait below uses this instead of interval*60.
    interval_secs = _ad_interval_seconds(s, ad_data)
    increment = Decimal(str(s.get("increment","0.05")))
    # Sync our tracked state to Bybit's REAL live price for this ad the
    # moment the loop (re)starts — for BOTH modes, not just "fixed". This
    # used to only run for fixed mode, which meant every restart of a
    # FLOATING-mode ad kept whatever current_price/ceiling_ref/
    # pending_ceiling were left over from before it was last stopped —
    # completely untethered from reality. That stale number then fed every
    # downstream check: fast-chase's "has the market moved enough" gate,
    # and sibling-collision math for OTHER ads reading this ad's price via
    # _ad_current_price. A newly (re)started ad's first cycle could get an
    # unexplained deduction purely because of a leftover number from a
    # previous run, and any sibling ad's collision math against it would be
    # wrong too. _set_ad_current_price also resets ceiling_ref to match and
    # clears any stale pending_ceiling, so this brings the whole tracked
    # state for this slot back in sync with Bybit in one call.
    _set_ad_current_price(sess, slot_idx, Decimal(str(ad_data.get("price","0"))))

    # ── Load this user's credentials ONCE at loop start ──
    # Re-read from DB so any key updates take effect on next loop restart.
    creds = get_user_creds(chat_id)
    if not creds or not creds.get("key"):
        await _safe_send(bot, chat_id=chat_id,
            text=(
                f"❌ <b>{label} Auto-Update stopped</b>\n\n"
                "No Bybit API key found for your account.\n"
                "Go to 🔑 <b>Set APIs</b> → <b>Set Bybit API</b> first."
            ),
            parse_mode="HTML")
        _set_ad_running(sess, slot_idx, False)
        return

    cycle = 0
    try:
      while _ad_running(sess, slot_idx):
        try:
            cycle += 1
            now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            mode = s.get("mode","fixed")
            prefix = f"[{label}] " if _multi_ad(sess) else ""

            # Demand for the shared Browserbase collector is driven purely
            # off this registry (see market_collector.py) — default to
            # "not currently wanting it" every cycle; the browserbase_market
            # branch below re-registers it the moment it knows this cycle's
            # pair. This keeps the collector idle (zero Browserbase usage)
            # the instant every ad using this mode is stopped or switched
            # to something else, without needing to hook every stop path.
            _bb_unregister_demand(chat_id, slot_idx)
            # Same idea for the Quick Market (direct_market.py/Decodo)
            # collector — default to "not wanting it" every cycle too, so
            # it makes zero requests the instant nothing needs this pair
            # anymore (see direct_market.py's IDLE_GRACE_SECONDS for the
            # short buffer before it actually treats a pair as idle).
            _dm_unregister_demand(chat_id, slot_idx)

            if mode == "fixed":
                new_p    = _ad_current_price(sess, slot_idx) + increment
                _quant   = Decimal("0.00000001")   # unchanged — fixed mode's original precision
                chase_ceiling = False   # live-ceiling chase only applies to floating mode
            elif mode == "ad_copy":
                # USD/USDT only. Doesn't compute a price at all — copies
                # whatever a real competing ad is CURRENTLY charging,
                # straight from Bybit's own live market listing. Exists
                # because this specific market is thin/volatile enough
                # that Bybit itself reportedly reshuffles ad positions
                # unpredictably — a formula-based price can't track that,
                # but mirroring the real current top-of-market price can.
                _quant = Decimal("0.0001")
                chase_ceiling = False   # not applicable — this mode never probes for a ceiling
                _range_n = _ad_copy_range_n(s)
                _own_ad_ids = {
                    (_ad_settings(sess, i) or {}).get("ad_id","")
                    for i in range(-1, TOTAL_AD_SLOTS - 1)
                    if (_ad_settings(sess, i) or {}).get("ad_id")
                }
                _want_token    = ad_data.get("tokenId","").upper()
                _want_currency = ad_data.get("currencyId","").upper()
                _side = _market_ads_query_side(ad_data)

                if _want_token == "BTC" and _want_currency == "NGN":
                    # ── BTC/NGN "Close Price Range" copy ──
                    # Completely different rule from USDT/USD. The user
                    # saves a leading-digit prefix (the price band they
                    # want to sit in); each cycle we pull ranks 1-300 and
                    # copy the HIGHEST market price whose digits start
                    # with that prefix — i.e. whoever currently leads that
                    # band. If no new leader has appeared since last
                    # cycle, we skip the edit entirely rather than
                    # re-posting the same number.
                    _prefix = (s.get("close_price_range") or "").strip()
                    if not _prefix:
                        await _safe_send(bot, chat_id=chat_id,
                            text=(f"⚠️ {prefix}<b>Cycle {cycle}</b> — No Close Price Range set for "
                                  f"{label}. Set one in the ad menu to start copying. Skipping this cycle."),
                            parse_mode="HTML")
                        for _ in range(interval_secs):
                            if not _ad_running(sess, slot_idx): break
                            await asyncio.sleep(1)
                        continue

                    _merchant = (s.get("merchant_username") or "").strip()
                    _merchant_range_used = "1-300"

                    def _match_merchant(pool):
                        return [
                            it for it in pool
                            if str(it.get("nickName", "")).strip().lower() == _merchant.lower()
                        ]

                    if _merchant:
                        # Sticky range preference: once we know which half
                        # (1-300 or 301-600) the merchant is CURRENTLY
                        # sitting in, fetch and check ONLY that half every
                        # cycle — via _fetch_market_ads_range, which hits
                        # Bybit's page numbers directly (e.g. pages 4-6 for
                        # 301-600) instead of always re-fetching both
                        # halves (6 pages) every single cycle. Only falls
                        # back to fetching the OTHER half when the merchant
                        # isn't in the preferred one any more.
                        _preferred = s.get("merchant_last_range", "1-300")
                        _tok, _cur = ad_data.get("tokenId",""), ad_data.get("currencyId","")

                        if _preferred == "301-600":
                            _win_items = await _fetch_market_ads_range(_tok, _cur, _side, 301, 600, creds)
                            _search_items = _match_merchant(_win_items)
                            if _search_items:
                                _merchant_range_used = "301-600"
                            else:
                                _win_items_1_300 = await _fetch_market_ads_range(_tok, _cur, _side, 1, 300, creds)
                                _search_items = _match_merchant(_win_items_1_300)
                                if _search_items:
                                    _merchant_range_used = "1-300"
                                    _win_items = _win_items_1_300
                        else:
                            _win_items = await _fetch_market_ads_range(_tok, _cur, _side, 1, 300, creds)
                            _search_items = _match_merchant(_win_items)
                            if _search_items:
                                _merchant_range_used = "1-300"
                            else:
                                # Bybit's own ranking rotates constantly — a
                                # merchant sitting around rank 249 one moment
                                # can be past 300 the next. Fall back to
                                # ranks 301-600 within the SAME cycle before
                                # concluding they're not there at all.
                                logger.info(f"[{label}] Ad Copy (BTC/NGN) Merchant Watch — '{_merchant}' not in ranks 1-300, trying 301-600")
                                _win_items_301_600 = await _fetch_market_ads_range(_tok, _cur, _side, 301, 600, creds)
                                _search_items = _match_merchant(_win_items_301_600)
                                if _search_items:
                                    _merchant_range_used = "301-600"
                                    _win_items = _win_items_301_600

                        if not _search_items:
                            # Not found in EITHER half this cycle — leave the
                            # sticky preference untouched so next cycle
                            # retries the same (still most-likely) range
                            # first, rather than resetting back to 1-300.
                            logger.info(f"[{label}] Ad Copy (BTC/NGN) Merchant Watch — '{_merchant}' not found in ranks 1-300 or 301-600 this cycle")
                            await _safe_send(bot, chat_id=chat_id,
                                text=(f"⚠️ {prefix}<b>Cycle {cycle}</b> — Merchant <code>{_esc(_merchant)}</code> "
                                      f"not found in ranks 1-300 or 301-600 right now. Skipping this cycle."),
                                parse_mode="HTML")
                            for _ in range(interval_secs):
                                if not _ad_running(sess, slot_idx): break
                                await asyncio.sleep(1)
                            continue

                        if s.get("merchant_last_range") != _merchant_range_used:
                            logger.info(f"[{label}] Ad Copy (BTC/NGN) Merchant Watch — '{_merchant}' shifted to ranks {_merchant_range_used}, now focusing there")
                        s["merchant_last_range"] = _merchant_range_used
                    else:
                        _win_items = await _fetch_market_ads_up_to(
                            ad_data.get("tokenId",""), ad_data.get("currencyId",""), _side, 300, creds
                        )
                        _search_items = _win_items
                    _top_range = int(s.get("top_range", 1) or 1)
                    _btc_match_price, _btc_match_item = _pick_close_range_price(
                        _search_items, _prefix, _own_ad_ids, top_range=_top_range
                    )
                    logger.info(
                        f"[{label}] Ad Copy (BTC/NGN) prefix={_prefix} top_range={_top_range}"
                        + (f" merchant={_merchant} (checked ranks {_merchant_range_used})" if _merchant else "")
                        + f" — fetched {len(_win_items)} item(s) in that range"
                        + (f", {len(_search_items)} from merchant" if _merchant else "")
                        + f", matched price: {_btc_match_price}"
                    )

                    if not _btc_match_price:
                        if _merchant:
                            _scope_msg = (
                                f"Fewer than {_top_range} ad(s) from merchant <code>{_esc(_merchant)}</code> "
                                f"match prefix <code>{_esc(_prefix)}</code> right now"
                            )
                        else:
                            _scope_msg = (
                                f"Fewer than {_top_range} BTC/NGN ad(s) in ranks 1-300 "
                                f"match <code>{_esc(_prefix)}</code> right now"
                            )
                        await _safe_send(bot, chat_id=chat_id,
                            text=(f"⚠️ {prefix}<b>Cycle {cycle}</b> — {_scope_msg} "
                                  f"(Top Range {_top_range} needs at least that many matches). Skipping this cycle."),
                            parse_mode="HTML")
                        for _ in range(interval_secs):
                            if not _ad_running(sess, slot_idx): break
                            await asyncio.sleep(1)
                        continue

                    # Only move when a genuinely NEW price appears at this
                    # rank — re-posting an unchanged price would just reset
                    # this ad's own position for nothing.
                    #
                    # Compares against the ad's ACTUAL live price
                    # (ad_data["price"], kept fresh below on every
                    # successful post AND refreshed by "Fetch Ad Details")
                    # rather than only our own memory of what we last
                    # posted — that memory alone goes stale the moment the
                    # ad is edited some other way (manually on Bybit, or
                    # from a previous bot run) while this bot wasn't the
                    # one doing it, which used to make the bot wrongly
                    # "confirm" no change was needed against a price the
                    # ad was never actually at.
                    _prev = ad_data.get("price")
                    if _prev is not None and _price_str_equal(_prev, _btc_match_price):
                        logger.info(
                            f"[{label}] Ad Copy (BTC/NGN) rank {_top_range} price unchanged "
                            f"({_btc_match_price}) — skipping edit this cycle"
                        )
                        if _should_notify_now(s, "ad_copy_btc_unchanged"):
                            await _safe_send(bot, chat_id=chat_id,
                                text=(f"⏭ {prefix}<b>Cycle {cycle}</b> — Band <code>{_esc(_prefix)}</code> "
                                      f"rank {_top_range} still <code>{_esc(str(_btc_match_price))}</code> "
                                      f"(no change) — skipping edit."),
                                parse_mode="HTML")
                        for _ in range(interval_secs):
                            if not _ad_running(sess, slot_idx): break
                            await asyncio.sleep(1)
                        continue

                    try:
                        new_p = Decimal(_btc_match_price)
                    except Exception:
                        await _safe_send(bot, chat_id=chat_id,
                            text=f"⚠️ {prefix}<b>Cycle {cycle}</b> — Unreadable price from the market listing. Skipping this cycle.",
                            parse_mode="HTML")
                        for _ in range(interval_secs):
                            if not _ad_running(sess, slot_idx): break
                            await asyncio.sleep(1)
                        continue
                    _quant = Decimal("0.01")   # NGN prices are 2dp, not USDT's 4dp
                    logger.info(
                        f"[{label}] Ad Copy (BTC/NGN) copying rank {_top_range} price {new_p} "
                        f"from {(_btc_match_item or {}).get('nickName','?')}"
                    )

                elif _want_token == "USDT" and _want_currency == "USD":
                    # ── USDT/USD deep-window comparison ──
                    # Copy Range (1-10 / 1-20) no longer picks a shallow
                    # page-1 depth for this pair. Both settings now do the
                    # SAME thing: fetch ONE window — ranks 1-300 — and copy
                    # whichever single price shows up most often (the
                    # "dominant" price) within it. A price several
                    # independent ads have converged on is treated as the
                    # more reliable "real" market price than whatever sits
                    # at position #1 alone, which can be a single outlier
                    # or a stale/boosted listing.
                    _win_items = await _fetch_market_ads_up_to(
                        ad_data.get("tokenId",""), ad_data.get("currencyId",""), _side, 300, creds
                    )
                    _window = _win_items[0:300]   # ranks 1-300
                    _competing = [
                        it for it in _window
                        if str(it.get("id","")) not in _own_ad_ids
                        and it.get("tokenId","").upper()    == _want_token
                        and it.get("currencyId","").upper() == _want_currency
                    ]
                    logger.info(
                        f"[{label}] Ad Copy (USDT/USD) fetched {len(_win_items)} item(s), "
                        f"{len(_competing)} in ranks 1-300."
                    )

                    if not _competing:
                        await _safe_send(bot, chat_id=chat_id,
                            text=(
                                f"⚠️ {prefix}<b>Cycle {cycle}</b> — Ad Copy found no other "
                                f"{_want_currency}/{_want_token} ads in ranks 1-300 "
                                f"of the live market right now. Skipping this cycle."
                            ), parse_mode="HTML")
                        for _ in range(interval_secs):
                            if not _ad_running(sess, slot_idx): break
                            await asyncio.sleep(1)
                        continue

                    try:
                        _chosen_price, _chosen_item = _pick_ad_copy_price_windowed(_competing)
                    except Exception:
                        await _safe_send(bot, chat_id=chat_id,
                            text=f"⚠️ {prefix}<b>Cycle {cycle}</b> — Ad Copy got an unreadable price from the market listing. Skipping this cycle.",
                            parse_mode="HTML")
                        for _ in range(interval_secs):
                            if not _ad_running(sess, slot_idx): break
                            await asyncio.sleep(1)
                        continue
                    _match_count = sum(1 for it in _competing if str(it.get("price","")) == _chosen_price)
                    logger.info(
                        f"[{label}] Ad Copy (USDT/USD) dominant price {_chosen_price} "
                        f"({_match_count} match(es) in ranks 1-300) from "
                        f"{_chosen_item.get('nickName','?') if _chosen_item else '?'}"
                    )

                    # ── Skip-until-new-highest-common-price gate ──
                    # Bybit's USDT/USD ranking is first-come-first-served:
                    # among ads sitting at the identical price, whichever
                    # posted that price EARLIEST ranks higher. Blindly
                    # re-submitting the same price every scheduled cycle
                    # (even though nothing actually changed) resets this
                    # ad's own "posted at" timestamp for that price and
                    # pushes it to the BACK of that first-come-first-served
                    # queue — actively hurting ranking despite the price
                    # itself being correct. So once a price has been
                    # copied, do nothing more cycle after cycle until the
                    # highest-common price this window computes actually
                    # CHANGES to something new — only then is there a real
                    # reason to move and it's worth re-claiming a fresh
                    # "first" position at the new price.
                    # Compares against the ad's ACTUAL live price
                    # (ad_data["price"]) rather than only our own memory of
                    # what we last posted — see the identical fix and full
                    # explanation on the BTC/NGN Ad Copy check above.
                    _prev_price = ad_data.get("price")
                    if _prev_price is not None and _price_str_equal(_prev_price, _chosen_price):
                        logger.info(
                            f"[{label}] Ad Copy (USDT/USD) highest-common price unchanged "
                            f"({_chosen_price}) since last copy — skipping edit this cycle "
                            f"to avoid resetting this ad's first-come-first-served queue position."
                        )
                        if _should_notify_now(s, "ad_copy_usdt_unchanged"):
                            await _safe_send(bot, chat_id=chat_id,
                                text=(
                                    f"⏭ {prefix}<b>Cycle {cycle}</b> — Highest-common price is still "
                                    f"<code>{_esc(str(_chosen_price))}</code> (no change since last copy) — "
                                    f"skipping this edit. USDT/USD ranking is first-come-first-served, so "
                                    f"re-posting an unchanged price would only push {label} to the back of the queue."
                                ), parse_mode="HTML")
                        for _ in range(interval_secs):
                            if not _ad_running(sess, slot_idx): break
                            await asyncio.sleep(1)
                        continue
                    # (ad_copy_last_price is intentionally NOT set here — only
                    # committed downstream once modify_ad actually confirms
                    # success. Setting it here unconditionally was the bug:
                    # a failed post still "remembered" the new price as if it
                    # had gone live, so every later cycle compared the real
                    # market price against a price the ad was never actually
                    # moved to, and kept "confirming" no change was needed
                    # forever — even though the ad was frozen at its old
                    # price on Bybit the whole time.)
                    try:
                        new_p = Decimal(_chosen_price)
                    except Exception:
                        await _safe_send(bot, chat_id=chat_id,
                            text=f"⚠️ {prefix}<b>Cycle {cycle}</b> — Ad Copy got an unreadable price from the market listing. Skipping this cycle.",
                            parse_mode="HTML")
                        for _ in range(interval_secs):
                            if not _ad_running(sess, slot_idx): break
                            await asyncio.sleep(1)
                        continue
                else:
                    # ── Fallback for any other pair: original shallow
                    # page-1, position-#1 behaviour (unchanged). ──
                    # Fetch a full 100-item page-1 window regardless of the
                    # self-exclusion range setting — gives us more raw data to
                    # look at / potentially filter on later, since Bybit's own
                    # support confirmed the API's page-1 order doesn't match
                    # what's actually visible on the site (server-side hidden
                    # ads + boosted visibility that the API doesn't expose).
                    # _range_n still controls how deep we search for the first
                    # non-self ad — this only widens what we fetch.
                    _market = await asyncio.get_event_loop().run_in_executor(
                        _ad_executor, get_market_ads,
                        ad_data.get("tokenId",""), ad_data.get("currencyId",""),
                        _side, 1, 100, creds
                    )
                    _items = (_market.get("result") or {}).get("items", []) if isinstance(_market, dict) else []
                    logger.info(
                        f"[{label}] Ad Copy fetched {len(_items)} item(s) for requested "
                        f"tokenId={_want_token} currencyId={_want_currency} — "
                        f"sample: {[(it.get('tokenId'), it.get('currencyId'), it.get('price')) for it in _items[:3]]}"
                    )
                    # EXPLICIT verification — don't trust the API's own filtering
                    # blindly. Confirmed in production: a request for USDT/USD
                    # returned (or was matched against) a USDC price, which got
                    # copied as if it were USDT — a real, meaningful pricing
                    # error since USDC/USD and USDT/USD traded at genuinely
                    # different rates (1.20 vs 1.015). Every candidate must
                    # match the ad's own tokenId AND currencyId exactly before
                    # it's ever eligible to be copied.
                    _competing = [
                        it for it in _items
                        if str(it.get("id","")) not in _own_ad_ids
                        and it.get("tokenId","").upper()    == _want_token
                        and it.get("currencyId","").upper() == _want_currency
                    ][:_range_n]
                    if not _competing:
                        await _safe_send(bot, chat_id=chat_id,
                            text=(
                                f"⚠️ {prefix}<b>Cycle {cycle}</b> — Ad Copy found no other "
                                f"{_want_currency}/{_want_token} ads to copy "
                                f"in the top {_range_n} of the live market right now. Skipping this cycle."
                            ), parse_mode="HTML")
                        for _ in range(interval_secs):
                            if not _ad_running(sess, slot_idx): break
                            await asyncio.sleep(1)
                        continue
                    try:
                        _chosen_price, _chosen_item = _pick_ad_copy_price(_competing)
                        new_p = Decimal(_chosen_price)
                        logger.info(
                            f"[{label}] Ad Copy window (top {_range_n}): "
                            f"{[it.get('price') for it in _competing]} — chose {_chosen_price} "
                            f"from {_chosen_item.get('nickName','?')}"
                        )
                    except Exception:
                        await _safe_send(bot, chat_id=chat_id,
                            text=f"⚠️ {prefix}<b>Cycle {cycle}</b> — Ad Copy got an unreadable price from the market listing. Skipping this cycle.",
                            parse_mode="HTML")
                        for _ in range(interval_secs):
                            if not _ad_running(sess, slot_idx): break
                            await asyncio.sleep(1)
                        continue

            elif mode == "browserbase_market":
                # ── Browserbase Market Price mode (BTC/NGN + USDT/USD only) ──
                # Every edit reads the shared Rank #1 snapshot that
                # market_collector.py's background loop refreshes — this
                # loop never talks to Browserbase itself, never scrapes
                # Bybit's website, and never touches bybit.get_market_ads.
                # It only reads the cache and, further below, calls the
                # SAME existing modify_ad() path every other mode uses.
                _pair_key = browserbase_pair_key(ad_data.get("tokenId",""), ad_data.get("currencyId",""))
                if _pair_key:
                    # This is what actually wakes the shared collector up
                    # (or keeps it awake) — see market_collector.py's
                    # demand registry. Registered BEFORE the "not ready
                    # yet" check below so the very first cycle after a
                    # user starts this mode already triggers the
                    # collector to open a session, instead of waiting a
                    # full extra cycle.
                    _bb_register_demand(chat_id, slot_idx, _pair_key)
                if not _pair_key:
                    await _safe_send(bot, chat_id=chat_id,
                        text=(
                            f"❌ <b>{label} Browserbase Market mode stopped</b>\n\n"
                            "This mode only supports BTC/NGN and USDT/USD ads.\n"
                            "Switch this ad to a different mode, or point it at one of those pairs."
                        ),
                        parse_mode="HTML")
                    _set_ad_running(sess, slot_idx, False)
                    _set_ad_task(sess, slot_idx, None)
                    return

                snap = get_market_snapshot(_pair_key)
                if snap["status"] != "ok" or snap["latest_price"] is None:
                    # Cold start is normal here: a fresh Browserbase session +
                    # page nav + first Bybit response genuinely takes a few
                    # seconds, and market_collector.py only just opened one
                    # because register_demand() was called above. Within the
                    # grace window we poll quietly and quickly instead of
                    # sending a Telegram warning for something that's about
                    # to resolve on its own.
                    _elapsed = snap.get("starting_elapsed_secs")
                    if snap["status"] == "starting" and _elapsed is not None \
                            and _elapsed < BB_WARMUP_GRACE_SECONDS:
                        for _ in range(BB_WARMUP_POLL_SECONDS):
                            if not _ad_running(sess, slot_idx): break
                            await asyncio.sleep(1)
                        continue

                    _err_note = f" ({_esc(str(snap['last_error']))})" if snap.get("last_error") else ""
                    if _should_notify_now(s, "browserbase_not_ready"):
                        await _safe_send(bot, chat_id=chat_id,
                            text=(f"⚠️ {prefix}<b>Cycle {cycle}</b> — Browserbase market price not "
                                  f"ready yet (status: {snap['status']}){_err_note}. Skipping this cycle."),
                            parse_mode="HTML")
                    for _ in range(interval_secs):
                        if not _ad_running(sess, slot_idx): break
                        await asyncio.sleep(1)
                    continue

                new_p  = snap["latest_price"]
                _quant = Decimal("0.01") if _pair_key == "USDT_USD" else Decimal("0.01")
                chase_ceiling = False   # not applicable — this mode always targets a specific real price
                logger.info(
                    f"[{label}] Browserbase Market ({_pair_key}) rank #1 price {new_p} "
                    f"from {snap.get('latest_nickname','?')} (fetched_at={snap.get('fetched_at')})"
                )

            elif mode == "decodo_market":
                # ── Quick Market mode (BTC/NGN + USDT/USD only) ──
                # Same idea as Browserbase Market mode — every edit copies
                # Bybit's live Rank #1 price — but backed by
                # direct_market.py's lightweight collector (plain requests
                # through a proxy) instead of a Browserbase/Playwright
                # session. Demand-driven exactly like Browserbase Market:
                # register BEFORE checking the snapshot so the very first
                # cycle after this mode starts already wakes the collector
                # up, instead of waiting a full extra cycle.
                _dm_pair_key = direct_market_pair_key(ad_data.get("tokenId",""), ad_data.get("currencyId",""))
                if not _dm_pair_key:
                    await _safe_send(bot, chat_id=chat_id,
                        text=(
                            f"❌ <b>{label} Quick Market mode stopped</b>\n\n"
                            "This mode only supports BTC/NGN and USDT/USD ads.\n"
                            "Switch this ad to a different mode, or point it at one of those pairs."
                        ),
                        parse_mode="HTML")
                    _set_ad_running(sess, slot_idx, False)
                    _set_ad_task(sess, slot_idx, None)
                    return
                _dm_register_demand(chat_id, slot_idx, _dm_pair_key)

                dm_snap = get_direct_market_snapshot(_dm_pair_key)
                if dm_snap["status"] != "ok" or dm_snap["latest_price"] is None:
                    # Same cold-start reasoning as Browserbase Market: the
                    # collector was idle (zero requests) until the
                    # register_demand() call just above woke it up, and its
                    # very first fetch for this pair genuinely takes a
                    # moment. Poll quietly instead of alarming the user
                    # during that normal startup window — by the NEXT
                    # scheduled cycle it's already caught up and fast.
                    _dm_elapsed = dm_snap.get("starting_elapsed_secs")
                    if dm_snap["status"] == "starting" and _dm_elapsed is not None \
                            and _dm_elapsed < BB_WARMUP_GRACE_SECONDS:
                        for _ in range(BB_WARMUP_POLL_SECONDS):
                            if not _ad_running(sess, slot_idx): break
                            await asyncio.sleep(1)
                        continue

                    _dm_err_note = f" ({_esc(str(dm_snap['last_error']))})" if dm_snap.get("last_error") else ""
                    if _should_notify_now(s, "quick_market_not_ready"):
                        await _safe_send(bot, chat_id=chat_id,
                            text=(f"⚠️ {prefix}<b>Cycle {cycle}</b> — Quick Market price not "
                                  f"ready yet (status: {dm_snap['status']}){_dm_err_note}. Skipping this cycle."),
                            parse_mode="HTML")
                    for _ in range(interval_secs):
                        if not _ad_running(sess, slot_idx): break
                        await asyncio.sleep(1)
                    continue

                # ── Skip-until-price-actually-changes gate ──
                # Same reasoning as Ad Copy's own gate a bit further up:
                # Bybit's ranking rewards whichever ad edited to a given
                # price FIRST. Re-submitting the identical price every
                # scheduled cycle — even though nothing changed — still
                # counts as a fresh edit, which resets THIS ad's own
                # position and can push it down behind ads that were
                # genuinely edited more recently. So once a price has been
                # posted, do nothing more until the snapshot's rank #1
                # price actually changes to something new.
                #
                # Compares against the ad's ACTUAL live price
                # (ad_data["price"]) rather than only the bot's own memory
                # of what it last posted — that memory alone goes stale
                # the instant the ad is changed some other way (manually
                # on Bybit's app, or a previous bot run) while auto-update
                # is stopped: the bot would otherwise still think "already
                # there" against a price the ad was never actually
                # returned to, and silently never re-sync it. ad_data
                # itself is refreshed either by "Fetch Ad Details" or by
                # this same mode's own successful posts below, so it's
                # always the true current state, not just this bot's
                # memory of its own actions.
                _dm_new_p = dm_snap["latest_price"]
                _dm_prev_price = ad_data.get("price")
                if _dm_prev_price is not None and _price_str_equal(_dm_prev_price, _dm_new_p):
                    logger.info(
                        f"[{label}] Quick Market ({_dm_pair_key}) price unchanged "
                        f"({_dm_new_p}) since last copy — skipping edit this cycle"
                    )
                    if _should_notify_now(s, "quick_market_unchanged"):
                        await _safe_send(bot, chat_id=chat_id,
                            text=(
                                f"⏭ {prefix}<b>Cycle {cycle}</b> — Rank #1 price is still "
                                f"<code>{_esc(str(_dm_new_p))}</code> (no change since last copy) — "
                                f"skipping this edit to avoid resetting {label}'s ranking position."
                            ), parse_mode="HTML")
                    for _ in range(interval_secs):
                        if not _ad_running(sess, slot_idx): break
                        await asyncio.sleep(1)
                    continue

                new_p  = _dm_new_p
                _quant = Decimal("0.01")
                chase_ceiling = False   # not applicable — this mode always targets a specific real price
                logger.info(
                    f"[{label}] Quick Market ({_dm_pair_key}) rank #1 price {new_p} "
                    f"from {dm_snap.get('latest_nickname','?')} (fetched_at={dm_snap.get('fetched_at')})"
                )

            else:
                try:
                    float_pct = float(s.get("float_pct") or 0)
                except (TypeError, ValueError):
                    float_pct = 0
                if float_pct <= 0:
                    await _safe_send(bot, chat_id=chat_id,
                        text=(
                            f"⚠️ {prefix}<b>Cycle {cycle}</b> — Float % isn't set yet.\n"
                            f"Set it from the AD Price Bot menu, then restart {label}."
                        ), parse_mode="HTML")
                    _set_ad_running(sess, slot_idx, False)
                    _set_ad_task(sess, slot_idx, None)
                    return
                local_usdt_ref = float(sess.shared_local_usdt_ref or s.get("local_usdt_ref") or 0)
                # Offload — calc_floating_price() blocks on a real HTTP call
                # (Bybit's public ticker endpoint). Never call it directly on
                # the event loop thread; see the matching fix in
                # _try_fast_chase for why that silently stalls the whole bot.
                new_p_str, err = await asyncio.get_event_loop().run_in_executor(
                    _ad_executor, calc_floating_price, ad_data, float_pct, local_usdt_ref
                )
                if err:
                    await _safe_send(bot, chat_id=chat_id,
                        text=f"⚠️ {prefix}<b>Cycle {cycle} float error</b>\n<code>{_esc(str(err))}</code>", parse_mode="HTML")
                    for _ in range(interval_secs):
                        if not _ad_running(sess, slot_idx): break
                        await asyncio.sleep(1)
                    continue
                new_p  = Decimal(new_p_str)   # calc_floating_price's own 0.01 precision
                _quant = Decimal("0.01")
                chase_ceiling = _wants_live_ceiling(
                    ad_data.get("currencyId",""), ad_data.get("tokenId",""), float_pct
                )

            # ── Same-float multi-ad support: same % is allowed across ads on
            # the same pair now — this nudges the actual PRICE apart instead,
            # only when another active ad on the identical pair would
            # otherwise land within the minimum gap. ──
            # Ad Copy is deliberately EXCLUDED: it already targets a specific,
            # real price it discovered on the live market (the dominant
            # USDT/USD price, or the leading price in a BTC/NGN band) —
            # nudging it away from that price for sibling separation defeats
            # the entire point of copying it, and the extra unplanned edits
            # were silently burning through this ad's modify-rate budget,
            # which then made LATER cycles appear to "do nothing" while they
            # were actually just waiting for that budget to free up.
            if _multi_ad(sess) and mode not in ("ad_copy", "browserbase_market", "decodo_market"):
                new_p, _cycle_collided = _resolve_price_collision(
                    sess, slot_idx,
                    ad_data.get("currencyId",""), ad_data.get("tokenId",""),
                    new_p
                )
            if mode in ("ad_copy", "browserbase_market", "decodo_market"):
                # Copy modes post EXACTLY what the market/snapshot gave —
                # no rounding. Forcing these through the same 2-decimal
                # quantize() used by fixed/floating mode was silently
                # turning e.g. "1.015" into "1.02", which isn't the price
                # that was actually copied.
                new_p_str = str(new_p)
            else:
                new_p_str = str(new_p.quantize(_quant, rounding=ROUND_HALF_UP))

            # ── Live-ceiling chase (max floating %) ──
            # At the top float % for this pair, submit a price deliberately
            # far beyond anything Bybit would ever accept, guaranteeing an
            # out-of-range (912120022) rejection. Bybit's error message always
            # includes its OWN current real max — the 912120022 handler below
            # then posts exactly that. This makes the ad always track Bybit's
            # true live ceiling every cycle instead of a fixed formula number
            # that can drift from it as the order book moves.
            # 50x (not 5x) — confirmed in production that 5x wasn't always
            # enough to force the rejection: some ads' real accepted price
            # band is wider than a flat 5x assumes, so the probe was briefly
            # accepted outright at ~5x the real price before the correction
            # further below caught it. 50x pushes it far beyond any realistic
            # accepted band.
            if chase_ceiling:
                probe_price = (new_p * Decimal("50")).quantize(_quant, rounding=ROUND_HALF_UP)
                submit_price, submit_str = probe_price, str(probe_price)
            else:
                submit_price, submit_str = new_p, new_p_str

            if not _can_modify_slot(sess, slot_idx, need=2 if chase_ceiling else 1):
                # RETRY-UNTIL-SUCCESS: the modify budget is a ROLLING 300s
                # window (40 edits/5min, shared with this ad's own fast-
                # chase) — "no budget right now" almost always clears up
                # within seconds as the oldest edit in the window ages out,
                # confirmed by the fact "Update Now" still works fine mid-
                # wait. Previously this gave up on the WHOLE cycle and made
                # the user wait a full scheduled interval (e.g. 5 more
                # minutes) before trying again, leaving the ad stuck at a
                # stale price that whole time unless fast-chase happened to
                # pick it back up. Now it waits out the budget and falls
                # through to the SAME unconditional post below the moment
                # it frees, instead of abandoning the cycle entirely.
                #
                # IMPORTANT: this block only WAITS — it must never itself
                # post or set ret_code. The actual post (and ret_code
                # assignment) lives once, unconditionally, right after this
                # whole if-block, so both "had to wait" and "budget was
                # already free" paths converge on exactly the same call.
                # An earlier version of this fix nested the post INSIDE
                # this branch instead, which meant the common case (budget
                # already free, no wait needed) skipped the post entirely
                # and left ret_code completely unset for the rest of the
                # cycle — an UnboundLocalError crash on every single cycle
                # that didn't need to wait.
                logger.info(f"[{label}] Cycle {cycle} waiting for modify budget to free up for user {chat_id} (retry-until-success, not skipping the cycle)")
                _budget_wait_notified = False
                while not _can_modify_slot(sess, slot_idx, need=2 if chase_ceiling else 1):
                    if not _ad_running(sess, slot_idx):
                        return
                    if not _budget_wait_notified and _should_notify_now(s, "budget_wait"):
                        await _safe_send(bot, chat_id=chat_id,
                            text=(
                                f"⏳ {prefix}<b>Cycle {cycle}</b> <code>{datetime.now().strftime('%H:%M:%S')}</code>\n"
                                f"Protecting Bybit's rate limit (too many recent edits) — retrying "
                                f"automatically as budget frees up, not waiting for the next full interval."
                            ),
                            parse_mode="HTML")
                        _budget_wait_notified = True
                    await asyncio.sleep(5)

            _record_modify_slot(sess, slot_idx)   # this ad's own budget, shared with its own fast-chase checks
            result   = await asyncio.get_event_loop().run_in_executor(
                _ad_executor, modify_ad, s["ad_id"], submit_str, ad_data, creds
            )
            ret_code = result.get("retCode", result.get("ret_code",-1))
            ret_msg  = result.get("retMsg",  result.get("ret_msg","Unknown"))
            if await _handle_ad_ip_error(bot, chat_id, sess, slot_idx, ret_code, ret_msg):
                return

            if ret_code == 912120022:
                # Out-of-range — Bybit tells us its own max/min. The FIRST
                # retry posts that exact number — it's what Bybit itself just
                # told us is valid, and with live BTC/ETH prices constantly
                # moving there's essentially no risk of it colliding with
                # anything. Only if that exact-boundary attempt ALSO fails
                # do later attempts back off with a small safety margin, each
                # time reading a FRESH boundary from the latest response.
                #
                # One specific failure mode handled here: if this ad already
                # sits EXACTLY at Bybit's stated boundary (e.g. a previous
                # cycle already parked it at the live max), resubmitting that
                # same exact number gets rejected with 90043 ("price differs
                # by less than 0%") instead of 912120022 — Bybit is telling us
                # the number is valid, just unchanged. That used to bubble up
                # as a totally different, unhandled error and get counted as
                # a cycle failure after only 2 occurrences, auto-stopping the
                # ad. Now it's treated the same way the top-level 90043 branch
                # handles it: nudge off it by this pair's minimum price gap,
                # in whichever direction we were already heading, and keep
                # trying — rather than stopping the ad over a price that's
                # perfectly fine, just identical to what's already live.
                last_code, last_msg = ret_code, ret_msg
                posted_price   = None
                was_too_high   = None
                candidate      = None
                for _attempt in range(4):
                    if last_code == 912120022:
                        min_str, max_str = _extract_bybit_bounds(last_msg)
                        was_too_high = max_str is not None
                        bound_str = max_str if was_too_high else min_str
                        if not bound_str:
                            break   # couldn't parse a boundary at all — nothing more we can do
                        bound_dec = Decimal(bound_str)
                        if _attempt < 2:
                            # Use Bybit's own string EXACTLY — no re-quantizing. Bybit
                            # doesn't always use the same decimal precision we do
                            # (e.g. 3 decimals on some USD pairs vs 2 on NGN), so
                            # rounding this to our own precision can round IT UP
                            # past Bybit's actual limit and fail again for a reason
                            # that has nothing to do with the price being wrong.
                            candidate = bound_dec
                        else:
                            # Last resort: nudge off the boundary. Round in the SAFE
                            # direction (down if we were too high, up if too low) so
                            # quantizing can never push us back out of range.
                            margin    = _safety_margin(bound_dec)
                            candidate = (bound_dec - margin) if was_too_high else (bound_dec + margin)
                            safe_rounding = ROUND_FLOOR if was_too_high else ROUND_CEILING
                            candidate     = candidate.quantize(_quant, rounding=safe_rounding)
                        # Sibling-collision guard — Bybit's stated boundary is a
                        # real market number, but with 2+ ads on the same pair
                        # it can still land within another active ad's gap.
                        # This loop used to post it straight to Bybit with no
                        # check at all (fast-chase's equivalent path already
                        # did this; the scheduled cycle didn't).
                        if _multi_ad(sess):
                            candidate, _cycle_collided3 = _resolve_price_collision(
                                sess, slot_idx,
                                ad_data.get("currencyId",""), ad_data.get("tokenId",""),
                                candidate
                            )
                        candidate_str = str(candidate)
                    elif last_code == 90043 and candidate is not None:
                        # Reactive recovery from an ACTUAL rejection — unlike
                        # the preventative self-check in
                        # _resolve_price_collision (which only needs to dodge
                        # an avoidable duplicate before ever submitting),
                        # real-world testing showed Bybit's own duplicate
                        # threshold for this is bigger than a cent: repeated
                        # $0.01 nudges (84789.161 → .15 → .14 → .13) all
                        # still came back 90043 here. Use the pair's real
                        # minimum gap instead — flat, not the old
                        # exponentially-doubling version, so a genuine retry
                        # sequence costs at most a few flat gaps, never 7x+.
                        gap = get_min_price_gap(ad_data.get("currencyId",""), ad_data.get("tokenId",""), candidate)
                        candidate = (candidate - gap) if was_too_high else (candidate + gap)
                        candidate = candidate.quantize(_quant, rounding=ROUND_HALF_UP)
                        candidate_str = str(candidate)
                    else:
                        break   # a genuinely different error — stop retrying, fall through to failure handling

                    if not _can_modify_slot(sess, slot_idx):
                        logger.warning(f"[{label}] Cycle {cycle} retry stopped for user {chat_id} — modify budget exhausted mid-retry")
                        break
                    _record_modify_slot(sess, slot_idx)
                    retry_result = await asyncio.get_event_loop().run_in_executor(
                        _ad_executor, modify_ad, s["ad_id"], candidate_str, ad_data, creds
                    )
                    last_code = retry_result.get("retCode", retry_result.get("ret_code",-1))
                    last_msg  = retry_result.get("retMsg",  retry_result.get("ret_msg","Unknown"))
                    if last_code == 0:
                        posted_price = candidate
                        break
                    if last_code not in (912120022, 90043):
                        break   # a different error now — stop retrying, fall through to failure handling

                if posted_price is not None:
                    _reset_ad_failures(sess, slot_idx)
                    _set_ad_current_price(sess, slot_idx, posted_price)
                    if mode in ("ad_copy", "decodo_market"):
                        # ad_data["price"] is the actual source of truth the
                        # skip-checks above compare against — keep it in
                        # sync immediately rather than waiting for the next
                        # manual "Fetch Ad Details".
                        ad_data["price"] = str(posted_price)
                    if mode == "ad_copy":
                        if _is_btc_ngn_ad(ad_data):
                            s["close_range_last_price"] = str(posted_price)
                        elif _is_usdt_usd_ad(ad_data):
                            s["ad_copy_last_price"] = str(posted_price)
                    elif mode == "decodo_market":
                        s["quick_market_last_price"] = str(posted_price)
                    if chase_ceiling:
                        await _safe_send(bot, chat_id=chat_id,
                            text=(
                                f"✅ {prefix}<b>Cycle {cycle}</b> <code>{now}</code>\n"
                                f"🏔 Chasing Bybit's live ceiling (max float {float_pct}%)\n"
                                f"💲 Posted at Bybit's real-time max: <code>{posted_price}</code> ({_mode_display_label(mode)})"
                            ),
                            parse_mode="HTML")
                    else:
                        await _safe_send(bot, chat_id=chat_id,
                            text=(
                                f"✅ {prefix}<b>Cycle {cycle}</b> <code>{now}</code>\n"
                                f"⚠️ Original <code>{new_p_str}</code> was out of range\n"
                                f"💲 Posted within Bybit's limit: <code>{posted_price}</code> ({_mode_display_label(mode)})"
                            ),
                            parse_mode="HTML")
                else:
                    if await _handle_ad_cycle_failure(bot, chat_id, sess, slot_idx, label, cycle, last_code, last_msg, ad_data):
                        return


            elif ret_code == 90043 and mode in ("ad_copy", "browserbase_market", "decodo_market"):
                # For every OTHER mode this means "the computed price
                # happens to round to what's already live — nudge off it".
                # For Ad Copy (and, for the same reason, Browserbase Market)
                # it means something different and much simpler: the ad is
                # ALREADY sitting at the exact market price we just
                # discovered and tried to (re)post. That's not a problem to
                # nudge away from — nudging here would actively move this ad
                # OFF the real market price it's supposed to be mirroring,
                # for no reason at all. Treat it as confirmation of success.
                _reset_ad_failures(sess, slot_idx)
                _set_ad_current_price(sess, slot_idx, new_p)
                if mode in ("ad_copy", "decodo_market"):
                    ad_data["price"] = str(new_p)
                if mode == "ad_copy":
                    if _is_btc_ngn_ad(ad_data):
                        s["close_range_last_price"] = str(new_p)
                    elif _is_usdt_usd_ad(ad_data):
                        s["ad_copy_last_price"] = str(new_p)
                elif mode == "decodo_market":
                    s["quick_market_last_price"] = str(new_p)
                logger.info(f"[{label}] Cycle {cycle} — 90043 (already at {new_p}) treated as success for {mode}, no nudge")
                await _safe_send(bot, chat_id=chat_id,
                    text=(
                        f"✅ {prefix}<b>Cycle {cycle}</b> <code>{now}</code>\n"
                        f"Already at the target price — no change needed\n"
                        f"💲 <code>{new_p}</code> ({_mode_display_label(mode)})"
                    ),
                    parse_mode="HTML")

            elif ret_code == 90043:
                # "The price of this P2P ad differs from your existing ad by
                # less than 0%." — this fires when the computed price rounds
                # to the SAME value the ad already has live on Bybit (happens
                # when the underlying market barely moves between cycles, or
                # when this ad hasn't changed since its last successful post).
                # This is fully recoverable — nudge the price by this pair's
                # real minimum gap and retry, instead of counting it as a
                # failure.
                #
                # This used to double on every attempt (1x, then 2x, then
                # 4x — up to 7x the gap across 3 attempts), which is what
                # produced unexplained -6/-7/-10 swings instead of -3. A
                # tiny epsilon nudge was tried in place of the real gap, but
                # real Bybit responses showed that's too small to register
                # as a genuine change at all — repeated $0.01 decrements
                # (84789.161 → .15 → .14 → .13) still came back 90043 every
                # time. So: flat, single gap per attempt — big enough to
                # actually register, never compounding.
                last_code, last_msg = ret_code, ret_msg
                candidate = new_p
                posted_price = None
                for _attempt in range(3):
                    if last_code == 90043:
                        gap = get_min_price_gap(ad_data.get("currencyId",""), ad_data.get("tokenId",""), candidate)
                        candidate = candidate - gap
                        candidate = candidate.quantize(_quant, rounding=ROUND_HALF_UP)
                    elif last_code == 912120022:
                        min_str, max_str = _extract_bybit_bounds(last_msg)
                        bound_str = max_str if max_str else min_str
                        if not bound_str:
                            break
                        candidate = Decimal(bound_str)
                    else:
                        break   # a genuinely different error — stop retrying, fall through to failure handling

                    # Sibling-collision guard — this loop previously posted
                    # candidate straight to Bybit with no check against other
                    # active ads on the same pair at all, unlike fast-chase's
                    # equivalent paths. A self-duplicate recovery here could
                    # land right on top of another ad's live price.
                    if _multi_ad(sess):
                        candidate, _cycle_collided2 = _resolve_price_collision(
                            sess, slot_idx,
                            ad_data.get("currencyId",""), ad_data.get("tokenId",""),
                            candidate
                        )

                    candidate_str = str(candidate)
                    if not _can_modify_slot(sess, slot_idx):
                        logger.warning(f"[{label}] Cycle {cycle} retry stopped for user {chat_id} — modify budget exhausted mid-retry")
                        break
                    _record_modify_slot(sess, slot_idx)
                    retry_result = await asyncio.get_event_loop().run_in_executor(
                        _ad_executor, modify_ad, s["ad_id"], candidate_str, ad_data, creds
                    )
                    last_code = retry_result.get("retCode", retry_result.get("ret_code",-1))
                    last_msg  = retry_result.get("retMsg",  retry_result.get("ret_msg","Unknown"))
                    if last_code == 0:
                        posted_price = candidate
                        break
                    if last_code not in (90043, 912120022):
                        break

                if posted_price is not None:
                    _reset_ad_failures(sess, slot_idx)
                    _set_ad_current_price(sess, slot_idx, posted_price)
                    _send_ok, _held = _edit_notice(s, _ad_interval_seconds(s, ad_data))
                    if _send_ok:
                        await _safe_send(bot, chat_id=chat_id,
                            text=(
                                f"✅ {prefix}<b>Cycle {cycle}</b> <code>{now}</code>\n"
                                f"⚠️ Price unchanged from last post — nudged\n"
                                f"💲 <code>{posted_price}</code> ({_mode_display_label(mode)})"
                                + _edit_notice_suffix(_held)
                            ),
                            parse_mode="HTML")
                else:
                    if await _handle_ad_cycle_failure(bot, chat_id, sess, slot_idx, label, cycle, last_code, last_msg, ad_data):
                        return

            elif ret_code == 0:
                if chase_ceiling:
                    # Extremely unlikely — the probe price (50x the formula
                    # number) was accepted outright instead of triggering an
                    # out-of-range rejection. A 50x-inflated number is NEVER a
                    # real, usable price — it exists purely to guarantee a
                    # rejection so the true boundary can be read from
                    # Bybit's error message. Leaving it live actually
                    # mispriced real ads on Bybit in production, and
                    # permanently broke fast-chase for that ad afterward
                    # (it compares future spot prices against this nonsense
                    # number and never finds room to act again). Correct it
                    # back to the real target immediately instead of just
                    # flagging it and moving on.
                    logger.warning(f"[{label}] Cycle {cycle} probe price {submit_price} was UNEXPECTEDLY accepted for user {chat_id} — correcting immediately, never leaving a 50x-inflated price live")
                    if _can_modify_slot(sess, slot_idx):
                        _record_modify_slot(sess, slot_idx)
                        correction_result = await asyncio.get_event_loop().run_in_executor(
                            _ad_executor, modify_ad, s["ad_id"], new_p_str, ad_data, creds
                        )
                        correction_code = correction_result.get("retCode", correction_result.get("ret_code", -1))
                        if correction_code == 0:
                            _reset_ad_failures(sess, slot_idx)
                            _set_ad_current_price(sess, slot_idx, new_p)
                            await _safe_send(bot, chat_id=chat_id,
                                text=(
                                    f"⚠️ {prefix}<b>Cycle {cycle}</b> <code>{now}</code>\n"
                                    f"🏔 Live-ceiling probe was unexpectedly accepted at <code>{submit_str}</code> — "
                                    f"corrected automatically back to <code>{new_p_str}</code>."
                                ),
                                parse_mode="HTML")
                        else:
                            # Correction itself failed — do NOT record the
                            # inflated price as current either way. Flag it
                            # loudly so a human checks Bybit directly.
                            await _safe_send(bot, chat_id=chat_id,
                                text=(
                                    f"🚨 {prefix}<b>Cycle {cycle}</b> <code>{now}</code>\n"
                                    f"Live-ceiling probe was unexpectedly accepted at <code>{submit_str}</code> and "
                                    f"the automatic correction back to <code>{new_p_str}</code> ALSO failed "
                                    f"(code={correction_code}). This ad may be live on Bybit at the wrong price "
                                    f"right now — please check it manually."
                                ),
                                parse_mode="HTML")
                    else:
                        await _safe_send(bot, chat_id=chat_id,
                            text=(
                                f"🚨 {prefix}<b>Cycle {cycle}</b> <code>{now}</code>\n"
                                f"Live-ceiling probe was unexpectedly accepted at <code>{submit_str}</code> but there's "
                                f"no modify budget left to correct it this instant — it will self-correct on the very "
                                f"next available modify. This ad is live on Bybit at the wrong price right now."
                            ),
                            parse_mode="HTML")
                else:
                    _reset_ad_failures(sess, slot_idx)
                    _set_ad_current_price(sess, slot_idx, submit_price)
                    # Only NOW — after Bybit has confirmed ret_code==0 — do
                    # we commit the copy-mode "last price" trackers. Doing
                    # this any earlier (before knowing the post succeeded)
                    # was the root cause of ad_copy ads permanently freezing:
                    # a failed/rejected modify would still get remembered as
                    # if it had gone live, so every later cycle compared the
                    # real market price against a price the ad was never
                    # actually moved to, and kept "confirming" no change was
                    # needed forever while the real ad sat frozen at its old
                    # price on Bybit.
                    if mode in ("ad_copy", "decodo_market"):
                        ad_data["price"] = submit_str
                    if mode == "ad_copy":
                        if _is_btc_ngn_ad(ad_data):
                            s["close_range_last_price"] = submit_str
                        elif _is_usdt_usd_ad(ad_data):
                            s["ad_copy_last_price"] = submit_str
                    elif mode == "decodo_market":
                        s["quick_market_last_price"] = submit_str
                    _send_ok, _held = _edit_notice(s, _ad_interval_seconds(s, ad_data))
                    if _send_ok:
                        await _safe_send(bot, chat_id=chat_id,
                            text=(f"✅ {prefix}<b>Cycle {cycle}</b> <code>{now}</code>\n💲 <code>{submit_str}</code> ({_mode_display_label(mode)})"
                                  + _edit_notice_suffix(_held)),
                            parse_mode="HTML")
            else:
                if await _handle_ad_cycle_failure(bot, chat_id, sess, slot_idx, label, cycle, ret_code, ret_msg, ad_data):
                    return

            # ── Wait out the rest of the scheduled interval ────────────────
            # Fast-chase polling for this slot is now handled entirely by
            # the shared per-user coordinator (_fast_chase_coordinator) —
            # see the note near _FAST_CHASE_POLL_SECS for why. This loop just
            # waits for the next scheduled cycle, scheduled off the
            # monotonic clock so a slow iteration can only ever delay
            # itself, never stack up and stall later checks.
            #
            # ── Cycle-restart-on-fast-chase (floating mode only) ───────────
            # Bybit resets its own 5-min countdown-to-offline on THIS ad
            # every time it's actually modified — including a fast-chase
            # post that lands between scheduled cycles, not just this
            # loop's own scheduled posts. So while waiting, watch this
            # slot's last-fast-modify timestamp: every time fast-chase
            # posts a real update here, push wait_until out a full fresh
            # interval from that moment, exactly mirroring what Bybit's
            # own countdown just did. No cap — if the market keeps moving
            # every few seconds, this keeps restarting indefinitely, same
            # as Bybit's real countdown would. Fixed-mode ads never touch
            # last_fast_modify_ts_by_slot (fast-chase skips them entirely),
            # so this is a no-op for them and behaves exactly as before.
            loop_clock = asyncio.get_event_loop()
            wait_until = loop_clock.time() + interval_secs
            _seen_fast_modify_ts = _last_fast_modify_ts(sess, slot_idx) if mode == "floating" else None
            while _ad_running(sess, slot_idx) and loop_clock.time() < wait_until:
                await asyncio.sleep(1)
                if mode == "floating":
                    _fc_ts = _last_fast_modify_ts(sess, slot_idx)
                    if _fc_ts is not None and _fc_ts != _seen_fast_modify_ts:
                        _seen_fast_modify_ts = _fc_ts
                        wait_until = _fc_ts + interval_secs
                        logger.info(
                            f"[{label}] Fast-chase modified price during wait for user {chat_id} — "
                            f"cycle timer restarted, next scheduled cycle now ~{interval}min from that post"
                        )
        except asyncio.CancelledError:
            logger.info(f"[{label}] Auto-update task cancelled for user {chat_id}")
            raise
        except Exception as _cycle_exc:
            # This is the core reliability fix: previously NOTHING wrapped the
            # scheduled cycle body, so any unexpected exception here (a bad
            # API response shape, a KeyError, a Decimal conversion issue, etc.)
            # would propagate all the way out of auto_update_loop and silently
            # kill the asyncio Task — refresh_running stayed True, the menu kept
            # showing "running", and nothing was ever logged pointing at why it
            # stopped. Now it's caught, logged loudly, and the loop keeps going.
            logger.error(
                f"[{label}] Cycle {cycle} crashed unexpectedly for user {chat_id}: {_cycle_exc}",
                exc_info=True
            )
            await asyncio.sleep(2)   # brief backoff so a persistent error can't hot-loop
    finally:
        # Always release this slot's Browserbase demand registration when
        # the loop stops — however it stops (normal stop, auto-stop after
        # repeated failures, cancellation, or an unexpected crash) — so a
        # slot that was using browserbase_market mode can never leave a
        # phantom "still active" entry behind that would keep the shared
        # collector's Browserbase session open (and burning the free
        # plan's monthly hour budget) after nothing is actually using it.
        _bb_unregister_demand(chat_id, slot_idx)
        # Same for Quick Market — otherwise a phantom demand entry would
        # keep direct_market.py's collector refreshing this pair (and
        # spending Decodo bandwidth) for up to IDLE_GRACE_SECONDS after
        # this ad has actually stopped.
        _dm_unregister_demand(chat_id, slot_idx)

    logger.info(f"🛑 PRICE LOOP STOPPED ({label}) for user {chat_id}")


async def _handle_ad_cycle_failure(bot, chat_id, sess, slot_idx, label, cycle, ret_code, ret_msg, ad_data=None) -> bool:
    """
    Shared failure handler for auto_update_loop. Returns True if the loop
    should stop immediately (this slot was auto-stopped), False if it
    should keep going to its normal inter-cycle sleep.
    """
    prefix = f"[{label}] " if _multi_ad(sess) else ""

    if ret_code == _BUDGET_COOLDOWN:
        # Not a real Bybit-side failure — just this one cycle's post being
        # skipped to stay under the shared modify budget. Never counts
        # towards the 2-in-a-row auto-stop.
        await _safe_send(bot, chat_id=chat_id,
            text=(
                f"⏳ {prefix}<b>Cycle {cycle}</b> <code>{datetime.now().strftime('%H:%M:%S')}</code>\n"
                f"Skipped — protecting Bybit's rate limit (too many recent edits). "
                f"Resumes next cycle automatically."
            ),
            parse_mode="HTML")
        return False

    extra = ""
    if ad_data:
        _ecur = ad_data.get("currencyId","").upper()
        extra = f"\n💱 Update {_ecur}/USDT ref if rate changed" if (currency_needs_ref(_ecur) or _ecur == "NGN") else ""

    if _multi_ad(sess):
        fail_count = _increment_ad_failures(sess, slot_idx)
        if fail_count >= 2:
            _set_ad_running(sess, slot_idx, False)
            _set_ad_task(sess, slot_idx, None)
            await _safe_send(bot, chat_id=chat_id,
                text=(
                    f"🛑 <b>{label} auto-stopped</b>\n\n"
                    f"2 failed updates in a row — likely too close to another ad's price, "
                    f"or a Bybit-side rejection.\n"
                    f"Last error: <code>{ret_code}</code> — <code>{_esc(str(ret_msg))}</code>{extra}\n\n"
                    f"Check/edit this ad directly on Bybit, then restart it from the bot."
                ),
                parse_mode="HTML")
            return True

    await _safe_send(bot, chat_id=chat_id,
        text=f"❌ {prefix}<b>Cycle {cycle} failed</b>\n<code>{ret_code}</code> — <code>{_esc(str(ret_msg))}</code>{extra}",
        parse_mode="HTML")
    return False



# ─────────────────────────────────────────
# 📤 Send / edit menu with banner image
# ─────────────────────────────────────────
async def send_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send the main menu with the banner image attached."""
    uid     = update.effective_user.id
    chat_id = update.effective_chat.id
    text    = main_menu_text(uid)
    kb      = main_menu_keyboard(uid)
    try:
        await context.bot.send_photo(
            chat_id=chat_id,
            photo=BANNER_URL,
            caption=text,
            reply_markup=kb,
            parse_mode="HTML"
        )
    except Exception as e:
        logger.warning(f"[Menu] Failed to send photo, falling back to text: {e}")
        await context.bot.send_message(
            chat_id=chat_id, text=text, reply_markup=kb, parse_mode="HTML"
        )


async def edit_menu(query, text: str, keyboard: InlineKeyboardMarkup):
    """Edit the existing menu message (photo caption or plain text).
    Tries caption first (photo messages), falls back to text, then sends new message."""
    # Try caption edit (for photo/banner messages)
    try:
        await query.edit_message_caption(caption=text, reply_markup=keyboard, parse_mode="HTML")
        return
    except Exception:
        pass
    # Try text edit (for plain text messages)
    try:
        await query.edit_message_text(text=text, reply_markup=keyboard, parse_mode="HTML")
        return
    except Exception as e:
        logger.warning(f"[edit_menu] edit failed: {e}")
    # Last resort — send as new message
    try:
        await query.message.reply_text(text=text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        logger.error(f"[edit_menu] send fallback also failed: {e}")


def _esc(value: str) -> str:
    """HTML-escape a string so it is safe inside parse_mode='HTML' messages.
    Escapes &, <, > which are the only three Telegram HTML mode cares about.
    API keys often contain underscores, dashes, dots — none of those need escaping.
    """
    return (value or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def _typing(context: ContextTypes.DEFAULT_TYPE, chat_id: int, min_delay: float = 0.5, max_delay: float = 1.4):
    """
    Show the 'typing…' indicator and hold for a short human-feeling delay
    before the caller sends its reply. Called at the top of every
    user-facing handler (buttons, free text, commands) so nothing lands
    instantly. Failures here are swallowed — this is cosmetic only and
    must never block or crash a real response.
    """
    try:
        await context.bot.send_chat_action(chat_id=chat_id, action="typing")
    except Exception:
        pass
    try:
        await asyncio.sleep(random.uniform(min_delay, max_delay))
    except Exception:
        pass


_TG_USERNAME_RE = re.compile(r'^@[A-Za-z][A-Za-z0-9_]{4,31}$')
_PHONE_RE       = re.compile(r'^\+[1-9]\d{6,14}$')


def validate_contact(text: str):
    """
    Validates a contact string as either a Telegram username (@name) or a
    WhatsApp/phone number in international format (+countrycode...).
    Returns (ok, normalized_contact, error_message) — error_message is ""
    when ok is True.
    """
    t = (text or "").strip()
    if not t:
        return False, "", "❌ Please send your Telegram username or WhatsApp number."
    if t.startswith("@"):
        if _TG_USERNAME_RE.match(t):
            return True, t, ""
        return False, "", (
            "❌ That doesn't look like a valid Telegram username.\n"
            "It should start with @ and be 5-32 characters (letters, numbers, underscores).\n"
            "Example: <code>@johnsmith</code>"
        )
    if t.startswith("+"):
        if _PHONE_RE.match(t):
            return True, t, ""
        return False, "", (
            "❌ That doesn't look like a valid WhatsApp number.\n"
            "Include your country code starting with +, digits only, no spaces or dashes.\n"
            "Example: <code>+2348012345678</code>"
        )
    return False, "", (
        "❌ Please start with <code>@</code> for a Telegram username (e.g. <code>@johnsmith</code>) "
        "or <code>+</code> for a WhatsApp number with country code (e.g. <code>+2348012345678</code>)."
    )


async def _submit_upgrade_request(bot, uid: int, uname: str, dname: str, contact: str):
    """
    Shared upgrade-request submission — saves to DB and notifies every
    admin, now including the contact detail the user provided. Used from
    the free-text message handler (after contact info passes validation);
    confirming to the user is handled separately by the caller since that
    differs by call site.
    """
    logger.info(f"[Upgrade] Request from uid={uid} uname=@{uname} — saving to DB")
    try:
        db.request_upgrade(uid, uname, dname, contact)
        logger.info(f"[Upgrade] DB write OK for uid={uid}")
    except Exception as _db_err:
        logger.error(f"[Upgrade] DB write FAILED for uid={uid}: {_db_err}")

    _admin_msg = (
        f"🔔 <b>New Upgrade Request!</b>\n\n"
        f"👤 User ID: <code>{uid}</code>\n"
        f"Username: @{uname if uname else 'None'}\n"
        f"Name: {_esc(dname)}\n"
        f"📞 Contact: {_esc(contact)}\n\n"
        f"Approve: <code>/upgrade {uid} 30</code>"
    )
    for _admin_id in list(_admin_chat_ids):
        try:
            await bot.send_message(chat_id=_admin_id, text=_admin_msg, parse_mode="HTML")
            logger.info(f"[Upgrade] Admin {_admin_id} notified for uid={uid}")
        except Exception as _notify_err:
            logger.error(f"[Upgrade] Could not notify admin {_admin_id}: {_notify_err}")


async def edit_menu_html(query, text: str, keyboard: InlineKeyboardMarkup):
    """Like edit_menu but uses HTML parse mode — safe for raw API keys / UUIDs."""
    try:
        await query.edit_message_caption(caption=text, reply_markup=keyboard, parse_mode="HTML")
        return
    except Exception:
        pass
    try:
        await query.edit_message_text(text=text, reply_markup=keyboard, parse_mode="HTML")
        return
    except Exception as e:
        logger.warning(f"[edit_menu_html] edit failed: {e}")
    try:
        await query.message.reply_text(text=text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        logger.error(f"[edit_menu_html] send fallback also failed: {e}")


# ─────────────────────────────────────────
# /start   /menu
# ─────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tuser = update.effective_user
    user, is_new = _get_or_register_user(tuser)
    if is_admin(tuser.id):
        _admin_chat_ids.add(update.message.chat_id)

    # Auto-downgrade expired pro users
    db.check_and_auto_downgrade(tuser.id)

    # ── Always refresh plan badge from DB so Pro shows instantly after upgrade ──
    global _current_user_id, _current_plan_badge
    _current_user_id    = tuser.id
    _current_plan_badge = sub.plan_badge(tuser.id)

    # ── Load persisted settings from disk into session ──
    # This ensures settings (Ad ID, UID, mode, interval, etc.) survive restarts.
    _load_settings_from_disk(tuser.id)

    await send_menu(update, context)


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)








# ─────────────────────────────────────────
# 🎛️ BUTTON HANDLER
# ─────────────────────────────────────────
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Main callback button handler. Wrapped with full exception logging so
    any crash is visible in Render logs with exact line + traceback.
    """
    try:
        await _button_handler_inner(update, context)
    except Exception as _bh_err:
        import traceback
        logger.error(
            f"[ButtonHandler] UNHANDLED EXCEPTION\n"
            f"  data={getattr(getattr(update, 'callback_query', None), 'data', '?')!r}\n"
            f"  user={getattr(getattr(update, 'callback_query', None), 'from_user', None)}\n"
            f"  error={_bh_err}\n"
            f"{traceback.format_exc()}"
        )
        try:
            q = update.callback_query
            if q:
                await q.answer("⚠️ An error occurred. Please try again.", show_alert=True)
        except Exception:
            pass


async def _button_handler_inner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # All per-user state accessed via _s(tuser.id).field — no globals needed

    query   = update.callback_query
    data    = query.data if update.callback_query else ""
    chat_id = query.message.chat_id if query and query.message else 0
    logger.debug(f"[ButtonHandler] Received callback: data={data!r} chat_id={chat_id}")
    try:
        await query.answer()
    except Exception as _ans_err:
        logger.warning(f"[ButtonHandler] query.answer() failed: {_ans_err}")

    # ── Register/update user on every interaction ──
    global _current_user_id, _current_plan_badge
    tuser = query.from_user
    user_rec, _ = _get_or_register_user(tuser)
    db.check_and_auto_downgrade(tuser.id)
    _current_user_id    = tuser.id
    _current_plan_badge = sub.plan_badge(tuser.id)

    # ── Per-user isolated state for non-admin users ──
    # Admin uses the global user_state; non-admins get their own isolated dict
    if is_admin(tuser.id):
        _btn_state = user_state
    else:
        if "state" not in context.user_data:
            context.user_data["state"] = {}
        _btn_state = context.user_data["state"]
    # NOTE: Bybit credentials are loaded per-call via get_user_creds(tuser.id)
    # — no globals are mutated here. See get_user_creds() for details.


    # ── Pro feature guard ──
    # Block non-admin free users from ALL functional sections.
    # They can only access: main_menu, upgrade_plan, upgrade_request_yes, bot_status,
    # get_my_ip, section_apis, set_api_*, delete_apis, delete_apis_confirm, reset_*
    _FREE_ALLOWED = {
        "main_menu", "upgrade_plan", "upgrade_request_yes",
        "bot_status", "reset_confirm", "reset_do",
        "section_apis", "set_api_bybit",
        "set_api_bybit_1", "set_api_bybit_2",
        "delete_apis", "delete_apis_confirm",
        "delete_bybit1_apis", "delete_bybit1_confirm",
        "delete_bybit2_apis", "delete_bybit2_confirm",
    }
    _is_free_allowed = (
        data in _FREE_ALLOWED
        or data.startswith("switch_account_")
    )
    if not is_admin(tuser.id) and not sub.is_pro(tuser.id) and not _is_free_allowed:
        await query.answer(
            "🔒 Upgrade to Pro to access this feature.",
            show_alert=True
        )
        await edit_menu(query,
            "🔒 *Pro Plan Required*\n\nYou need a Pro plan to use this bot.\n\nTap *⬆️ Upgrade Plan* to request access from the admin.",
            main_menu_keyboard(tuser.id)
        )
        return

    # Legacy per-feature guard (still applies for admin-visible toggles)
    if sub.requires_pro(data) and not sub.is_pro(tuser.id) and not is_admin(tuser.id):
        await query.answer(
            "🔒 Pro plan required. Tap Upgrade Plan in the menu.",
            show_alert=True
        )
        return

    # ── 🏠 Main menu ──
    if data == "main_menu":
        # Always refresh plan badge when returning to main menu so upgrades
        # are reflected immediately without needing a redeploy.
        db.check_and_auto_downgrade(tuser.id)
        _current_plan_badge = sub.plan_badge(tuser.id)
        await edit_menu(query, main_menu_text(tuser.id), main_menu_keyboard(tuser.id))

    # ── 🌍 Get My IP ──
    elif data == "get_my_ip":
        await query.edit_message_caption(caption="⏳ Fetching public IP...", parse_mode="HTML") \
            if query.message.photo else await query.edit_message_text("⏳ Fetching public IP...")
        import requests as _req
        ip = None
        for svc in ["https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com"]:
            try:
                ip = _req.get(svc, timeout=5).text.strip()
                if ip: break
            except Exception:
                continue
        txt = (
            f"🌍 <b>Public IP Address</b>\n\n<code>{ip}</code>\n\n"
            "👉 Add this to your Bybit API whitelist if it changed."
        ) if ip else "❌ Could not fetch IP. Try again."

        kb_rows = list(back_main())

        try:
            await query.edit_message_caption(caption=txt, reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode="HTML")
        except Exception:
            await query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode="HTML")

    # ── 🔑 Switch Account ──
    elif data.startswith("switch_account_"):
        idx      = int(data.split("_")[-1])
        accounts = get_all_accounts()
        # Allow up to 2 slots regardless of env keys — credentials come from DB
        if idx >= max(len(accounts), 2):
            await query.answer("Invalid account", show_alert=True)
            return
        if _any_ad_running(_s(tuser.id)) or _s(tuser.id).auto_pay_enabled:
            await query.answer("⚠️ Stop all running ads and Auto-Pay before switching accounts.", show_alert=True)
            return

        # ── PER-USER slot switch — does NOT affect any other user ──
        # NEVER call set_active_account() here — that modifies the global
        # bybit._active_index which is shared across ALL users.

        # ── Save current slot's AD BOT settings before switching ──
        old_slot_str = _get_user_slot_str(tuser.id)
        _s(tuser.id).settings[f"mode_{old_slot_str}"]          = _s(tuser.id).settings.get("mode", "fixed")
        _s(tuser.id).settings[f"increment_{old_slot_str}"]     = _s(tuser.id).settings.get("increment", "0.05")
        _s(tuser.id).settings[f"float_pct_{old_slot_str}"]     = _s(tuser.id).settings.get("float_pct", "")
        _s(tuser.id).settings[f"local_usdt_ref_{old_slot_str}"]= _s(tuser.id).settings.get("local_usdt_ref", "")
        _s(tuser.id).settings[f"interval_{old_slot_str}"]      = _s(tuser.id).settings.get("interval", 2)
        _save_settings(tuser.id)   # persist before slot change

        _s(tuser.id).selected_slot = idx   # only this user changes
        new_slot_str = _get_user_slot_str(tuser.id)

        # Clear volatile order/ad data (other users are untouched)
        _s(tuser.id).ad_data.clear()
        _s(tuser.id).seen_order_ids.clear(); _s(tuser.id).paid_order_ids.clear()

        # ── Restore new slot's saved AD BOT settings (do NOT overwrite with defaults) ──
        _s(tuser.id).settings["ad_id"]          = _s(tuser.id).settings.get(f"ad_id_{new_slot_str}", "")
        _s(tuser.id).settings["bybit_uid"]      = _s(tuser.id).settings.get(f"bybit_uid_{new_slot_str}", "")
        _s(tuser.id).settings["mode"]           = _s(tuser.id).settings.get(f"mode_{new_slot_str}", "fixed")
        _s(tuser.id).settings["increment"]      = _s(tuser.id).settings.get(f"increment_{new_slot_str}", "0.05")
        _s(tuser.id).settings["float_pct"]      = _s(tuser.id).settings.get(f"float_pct_{new_slot_str}", "")
        _s(tuser.id).settings["local_usdt_ref"] = _s(tuser.id).settings.get(f"local_usdt_ref_{new_slot_str}", "")
        _s(tuser.id).settings["interval"]       = _s(tuser.id).settings.get(f"interval_{new_slot_str}", 2)

        acct_label = accounts[idx]["label"] if idx < len(accounts) else f"Account {idx + 1}"
        logger.info(f"[Slot] User {tuser.id} switched to slot {idx+1} ({acct_label}) — other users unaffected")
        await edit_menu(query,
            f"✅ <b>Switched to {acct_label}</b>\n\nYour session cleared.\n\n" + main_menu_text(tuser.id),
            main_menu_keyboard(tuser.id)
        )

    # ── Section navigations ──
    elif data == "section_ads":
        _sess_ads = _s(tuser.id)
        if _sess_ads.editing_slot >= len(_sess_ads.extra_ad_slots):
            _sess_ads.editing_slot = -1   # guard against a stale index if a slot was removed elsewhere
        await edit_menu(query, ads_section_text(tuser.id), ads_section_keyboard(tuser.id))

    # ── 📡 Bot Status ──
    elif data == "bot_status":
        _bs = _s(tuser.id)
        done, total, bar = setup_progress(tuser.id)
        _running = [_ad_slot_label(i) for i in range(-1, TOTAL_AD_SLOTS - 1) if _ad_running(_bs, i)]
        r_status = ("🟢 Running — " + ", ".join(_running)) if _running else "🔴 Stopped"
        _all = get_all_accounts()
        _acct = _all[_bs.selected_slot]["label"] if _all and _bs.selected_slot < len(_all) else f"Account {_bs.selected_slot + 1}"
        txt = (
            f"📡 <b>Bot Status</b>\n\n"
            f"🔑 Active: <b>{_esc(_acct)}</b>\n"
            f"Setup: {bar} <code>{done}/{total}</code>\n\n"
            f"📊 Price Bot: {r_status}\n"
            f"💳 Auto-Pay: {'✅ ON (checks every 60s)' if _bs.auto_pay_enabled else '❌ OFF'}\n\n"
            f"🆔 Ad 1 ID: <code>{_bs.settings.get('ad_id') or 'Not set'}</code>\n"
            f"🔀 Ad 1 Mode: <code>{_mode_display_label(_bs.settings.get('mode','fixed'))}</code>"
        )
        await edit_menu(query, txt, InlineKeyboardMarkup(back_main()))

    # ── 🔁 Reset confirm ──
    elif data == "reset_confirm":
        await edit_menu(query,
            "⚠️ *Reset Session?*\n\nThis clears all settings and stops all running tasks.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Reset", callback_data="reset_do")],
                [InlineKeyboardButton("❌ Cancel",     callback_data="main_menu")],
            ])
        )

    elif data == "reset_do":
        _rs = _s(tuser.id)
        _rs.stop_all_tasks()   # ads (Ad 1-8), auto-pay worker
        _rs.current_price = Decimal("0"); _rs.ad_data.clear()
        _rs.seen_order_ids = set(); _rs.paid_order_ids = set()
        _rs.selected_slot = 0
        for k, v in [("ad_id",""),("bybit_uid",""),("mode","fixed"),
                     ("increment","0.05"),("float_pct",""),("local_usdt_ref",""),("interval",2)]:
            _rs.settings[k] = v
        await edit_menu(query,
            "✅ *Session reset!* All settings cleared.\n\nTap /menu to start fresh.",
            InlineKeyboardMarkup(back_main())
        )

    # ── 💳 Toggle Auto-Pay ──
    elif data == "toggle_auto_pay":
        _ap = _s(tuser.id)
        if _ap.auto_pay_enabled:
            _ap.auto_pay_enabled = False
            if _ap.auto_pay_task and not _ap.auto_pay_task.done():
                _ap.auto_pay_task.cancel()
            _ap.auto_pay_task = None
            await query.answer("💳 Auto-Pay disabled.")
        else:
            _c = get_user_creds(tuser.id)
            if not is_admin(tuser.id) and not (_c and _c.get("key")):
                await query.answer(
                    f"❌ No Bybit API key for Account {_get_user_slot_str(tuser.id)}. Add it in 🔑 Set APIs first.",
                    show_alert=True
                )
                return
            _ap.auto_pay_enabled = True   # set BEFORE create_task, no await in between
            if not (_ap.auto_pay_task and not _ap.auto_pay_task.done()):
                _ap.auto_pay_task = asyncio.create_task(auto_pay_loop(context.bot, tuser.id))
            await query.answer("💳 Auto-Pay enabled ✅")
        await edit_menu(query, main_menu_text(tuser.id), main_menu_keyboard(tuser.id))

    # ── 🆔 Set Ad ID ──
    elif data == "set_ad_id":
        _btn_state["action"]       = "ad_id"
        _btn_state["prev_section"] = "section_ads"
        sess = _s(tuser.id)
        slot_idx = sess.editing_slot
        if slot_idx == -1:
            slot_str = _get_user_slot_str(tuser.id)
            cur = (
                sess.settings.get(f"ad_id_{slot_str}", "")
                or sess.settings.get("ad_id", "")
                or "Not set"
            )
            label = f"Account {slot_str}"
        else:
            cur = _ad_settings(sess, slot_idx).get("ad_id", "") or "Not set"
            label = _ad_slot_label(slot_idx)
        await edit_menu(query,
            f"🆔 <b>Set Ad ID — {label}</b>\n\nCurrent: <code>{_esc(cur)}</code>\n\n"
            "Send your Bybit Ad ID.\n💡 Use 📃 My Ads List to find it.\n\n"
            "Example: `2040156088201854976`",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 👤 Set UID ──
    elif data == "set_uid":
        _btn_state["action"]       = "bybit_uid"
        _btn_state["prev_section"] = "section_ads"
        slot_str = _get_user_slot_str(tuser.id)
        # Read the slot-keyed value first (what ads_section_text displays),
        # fall back to the generic key for backwards compatibility
        cur = (
            _s(tuser.id).settings.get(f"bybit_uid_{slot_str}", "")
            or _s(tuser.id).settings.get("bybit_uid", "")
            or "Not set"
        )
        await edit_menu(query,
            f"👤 <b>Set Bybit UID — Account {slot_str}</b>\n\nCurrent: <code>{_esc(cur)}</code>\n\n"
            "Bybit App → Profile → copy UID under your username.\n\n"
            "Example: `520097760`",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 🗑 Delete UID ──
    elif data == "delete_uid":
        slot_str = _get_user_slot_str(tuser.id)
        cur = (
            _s(tuser.id).settings.get(f"bybit_uid_{slot_str}", "")
            or _s(tuser.id).settings.get("bybit_uid", "")
            or "Not set"
        )
        await edit_menu(query,
            f"🗑 <b>Delete UID — Account {slot_str}</b>\n\n"
            f"Current UID: <code>{_esc(cur)}</code>\n\n"
            f"This removes the UID for Account {slot_str} only.\n"
            f"Other accounts and users are not affected.\n\n"
            f"Tap confirm to delete.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Delete UID", callback_data="delete_uid_confirm")],
                [InlineKeyboardButton("❌ Cancel",          callback_data="set_uid")],
            ])
        )

    elif data == "delete_uid_confirm":
        slot_str = _get_user_slot_str(tuser.id)
        _s(tuser.id).settings[f"bybit_uid_{slot_str}"] = ""
        # Clear generic key only if it was pointing at this slot's value
        if _s(tuser.id).settings.get("bybit_uid") == _s(tuser.id).settings.get(f"bybit_uid_{slot_str}", ""):
            _s(tuser.id).settings["bybit_uid"] = ""
        # In all cases, sync generic key from slot key (which is now "")
        _s(tuser.id).settings["bybit_uid"] = ""
        _save_settings(tuser.id)
        logger.info(f"[UID] Deleted bybit_uid for user={tuser.id} slot={slot_str}")
        await edit_menu(query,
            f"✅ <b>UID deleted for Account {slot_str}.</b>\n\n"
            f"Tap 👤 Set UID to enter a new one.",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 📃 My Ads ──
    elif data == "fetch_my_ads":
        uid   = tuser.id
        creds = get_user_creds(tuser.id)
        # Guard: non-admin user with no API key saved
        # Guard: non-admin user with no API key saved
        if not is_admin(tuser.id) and not creds.get("key"):
            await edit_menu(query,
                "\u274c *No Bybit API set.*\n\nGo to \U0001f511 *Set APIs* \u2192 Set Bybit Account 1 API to add your key first.",
                InlineKeyboardMarkup(back_section("section_ads"))
            )
            return
            return
        await edit_menu(query, "⏳ Fetching your ads...", ads_section_keyboard(tuser.id))
        try:
            result = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(None, partial(get_my_ads, creds=creds)),
                timeout=20
            )
        except asyncio.TimeoutError:
            # Bybit (or, if Permanent IP is active, the proxy relay) never
            # responded — without this bound the user was left stuck on
            # "⏳ Fetching your ads..." forever with no error shown,
            # looking exactly like the bot had hung.
            logger.warning(f"[MyAds] user={tuser.id} get_my_ads timed out after 20s")
            await edit_menu(query,
                "⏱ <b>Bybit didn't respond in time.</b>\n\nThis can happen if Bybit or the "
                "network path to it is briefly slow. Please try again in a moment.",
                InlineKeyboardMarkup(back_section("section_ads"))
            )
            return
        except Exception as e:
            logger.error(f"[MyAds] user={tuser.id} get_my_ads raised: {e}")
            await edit_menu(query,
                f"❌ <b>Something went wrong fetching your ads.</b>\n<code>{_esc(str(e))}</code>",
                InlineKeyboardMarkup(back_section("section_ads"))
            )
            return
        ret_code = result.get("retCode", result.get("ret_code",-1))
        if ret_code == 0:
            items = result.get("result",{}).get("items",[])
            if not items:
                await edit_menu(query, "📃 No ads found.", InlineKeyboardMarkup(back_section("section_ads")))
                return
            # Slot-aware lookup — matches next_setup_hint() and every other
            # UID consumer in this file. The generic settings["bybit_uid"]
            # key is only kept in sync with the ACTIVE slot's
            # bybit_uid_{slot} value by the explicit "switch account" path;
            # if that resync is ever missed (or hasn't happened yet for
            # this session), the generic key can silently point at a
            # DIFFERENT account's UID than the one whose ads were just
            # fetched with this slot's API key. That mismatch filtered out
            # every ad with no error anywhere — Bybit's API call succeeded
            # fine, the userId just never matched the wrong UID being
            # compared against. Always prefer the slot-specific key first.
            slot_str  = _get_user_slot_str(tuser.id)
            bybit_uid = _s(tuser.id).settings.get(f"bybit_uid_{slot_str}") or _s(tuser.id).settings.get("bybit_uid","")
            lines = ["📃 *Your P2P Ads:*\n"]
            for item in items:
                if bybit_uid and str(item.get("userId","")) != str(bybit_uid):
                    continue
                side  = "BUY" if str(item.get("side","")) == "0" else "SELL"
                stat  = {10:"🟢",20:"🔴",30:"✅"}.get(item.get("status",0),"❓")
                lines.append(
                    f"{stat} <b>{side}</b> <code>{item.get('tokenId','')}/{item.get('currencyId','')}</code>"
                    f" | 💲<code>{item.get('price','')}</code>\n🆔 <code>{item.get('id','')}</code>\n"
                )
            if len(lines) == 1: lines.append("No ads match your UID.")
            lines.append("\n_Tap any ID to copy → use 🆔 Set Ad ID_")
            msg = "\n".join(lines)
            if len(msg) > 4000: msg = msg[:4000] + "...(truncated)"
            await edit_menu(query, msg, InlineKeyboardMarkup(back_section("section_ads")))
        else:
            await edit_menu(query,
                f"❌ <code>{result.get('retMsg',result.get('ret_msg',''))}</code>",
                InlineKeyboardMarkup(back_section("section_ads"))
            )

    # ── 📋 Fetch Ad Details ──
    elif data == "fetch_ad":
        sess = _s(tuser.id)
        slot_idx = sess.editing_slot
        s = _ad_settings(sess, slot_idx)
        if not s.get("ad_id"):
            await edit_menu(query, "❌ Set your Ad ID first.", InlineKeyboardMarkup(back_section("section_ads")))
            return
        _creds = get_user_creds(tuser.id)
        if not is_admin(tuser.id) and not _creds.get("key"):
            await edit_menu(query,
                "\u274c *No Bybit API set.*\n\nGo to \U0001f511 *Set APIs* \u2192 Set Bybit Account 1 API first.",
                InlineKeyboardMarkup(back_section("section_ads"))
            )
            return
        await edit_menu(query, "⏳ Loading ad from Bybit...", ads_section_keyboard(tuser.id))
        result   = await asyncio.get_event_loop().run_in_executor(
            None, partial(get_ad_details, s["ad_id"], creds=_creds)
        )
        ret_code = result.get("retCode", result.get("ret_code",-1))
        if ret_code == 0 and not _ad_matches_slot_pair(slot_idx, result.get("result", {}) or {}):
            _got = result.get("result", {}) or {}
            await edit_menu(query,
                f"❌ <b>Wrong ad type for {_ad_slot_label(slot_idx)}</b>\n\n"
                f"{_ad_slot_label(slot_idx)} only accepts <b>{_slot_pair_label(slot_idx)}</b> ads, "
                f"but this ad is <code>{_got.get('tokenId','?')}/{_got.get('currencyId','?')}</code>.\n\n"
                f"Use a {_slot_pair_label(slot_idx)} Ad ID here (Ad 1–4 BTC/USD · Ad 5 BTC/NGN · "
                f"Ad 6–7 ETH/USD · Ad 8 USDT/USD).",
                InlineKeyboardMarkup(back_section("section_ads"))
            )
            return
        if ret_code == 0:
            ad_data = _ad_data_of(sess, slot_idx)
            ad_data.clear()
            ad_data.update(result.get("result",{}))
            token    = ad_data.get("tokenId","—")
            currency = ad_data.get("currencyId","—")
            max_pct  = get_max_float_pct(currency, token)
            ad_stat  = {10:"🟢 Online",20:"🔴 Offline",30:"✅ Done"}.get(ad_data.get("status"),"?")
            next_hint = next_setup_hint(tuser.id) if slot_idx == -1 else "Now set mode + interval for this ad."
            await edit_menu(query,
                f"✅ <b>Ad Loaded! ({_ad_slot_label(slot_idx)})</b>\n\n"
                f"🆔 <code>{s['ad_id']}</code>\n"
                f"💱 <code>{token}/{currency}</code> | 💲 <code>{ad_data.get('price','')}</code>\n"
                f"Min: <code>{ad_data.get('minAmount','')}</code> | Max: <code>{ad_data.get('maxAmount','')}</code> | Qty: <code>{ad_data.get('lastQuantity','')}</code>\n"
                f"Status: {ad_stat} | Max float: <code>{max_pct}%</code>\n\n"
                f"_{next_hint}_",
                InlineKeyboardMarkup(back_section("section_ads"))
            )
        else:
            await edit_menu(query,
                f"❌ <code>{result.get('retMsg',result.get('ret_msg',''))}</code>",
                InlineKeyboardMarkup(back_section("section_ads"))
            )

    # ── 🔀 Switch Mode ──
    elif data == "mode_menu":
        sess = _s(tuser.id)
        slot_idx = sess.editing_slot
        s = _ad_settings(sess, slot_idx)
        ad_data = _ad_data_of(sess, slot_idx)
        cur_mode = s.get("mode", "fixed")
        is_usdt_usd = ad_data.get("currencyId","").upper() == "USD" and ad_data.get("tokenId","").upper() == "USDT"
        is_btc_ngn  = _is_btc_ngn_ad(ad_data)
        rows = [
            [InlineKeyboardButton(("✅ " if cur_mode == "fixed" else "") + "💲 Fixed",    callback_data="set_mode_fixed")],
            [InlineKeyboardButton(("✅ " if cur_mode == "floating" else "") + "📈 Floating", callback_data="set_mode_floating")],
        ]
        if is_usdt_usd or is_btc_ngn:
            rows.append([InlineKeyboardButton(("✅ " if cur_mode == "ad_copy" else "") + "🪞 Ad Copy", callback_data="set_mode_ad_copy")])
            rows.append([InlineKeyboardButton(("✅ " if cur_mode == "browserbase_market" else "") + "🌐 Browserbase Market", callback_data="set_mode_browserbase_market")])
            if slot_idx in QUICK_MARKET_SLOTS:
                rows.append([InlineKeyboardButton(("✅ " if cur_mode == "decodo_market" else "") + "⚡ Quick Market", callback_data="set_mode_decodo_market")])
        rows += back_section("section_ads")
        txt = (
            f"🔀 <b>{_ad_slot_label(slot_idx)} — Choose Mode</b>\n\n"
            "💲 <b>Fixed</b> — increments by a set amount each cycle.\n"
            "📈 <b>Floating</b> — tracks a % of live spot price.\n"
        )
        if is_usdt_usd:
            txt += "🪞 <b>Ad Copy</b> — copies the top live market ad price directly (USD/USDT only).\n"
        elif is_btc_ngn:
            txt += (
                "🪞 <b>Ad Copy</b> — matches a price prefix you set and copies the highest "
                "market ad sharing it (BTC/NGN).\n\n"
                "<i>Only one ad on the bot can use Ad Copy for BTC/NGN at a time.</i>\n"
            )
        if is_usdt_usd or is_btc_ngn:
            txt += (
                "🌐 <b>Browserbase Market</b> — every edit copies Bybit's live Rank #1 "
                f"{'BTC/NGN' if is_btc_ngn else 'USDT/USD'} price, read from a shared browser-based "
                "price feed (BTC/NGN and USDT/USD only).\n"
            )
            if slot_idx in QUICK_MARKET_SLOTS:
                txt += (
                    "⚡ <b>Quick Market</b> — copies Bybit's live Rank #1 price every edit from a "
                    "lighter always-on direct-HTTP feed. Only 1 active BTC/NGN and 1 active "
                    "USDT/USD Quick Market ad is allowed.\n"
                )
        await edit_menu(query, txt, InlineKeyboardMarkup(rows))

    elif data in ("set_mode_fixed", "set_mode_floating", "set_mode_ad_copy", "set_mode_browserbase_market", "set_mode_decodo_market"):
        sess = _s(tuser.id)
        slot_idx = sess.editing_slot
        s = _ad_settings(sess, slot_idx)
        ad_data = _ad_data_of(sess, slot_idx)
        new_mode = data[len("set_mode_"):]
        if new_mode == "decodo_market" and slot_idx not in QUICK_MARKET_SLOTS:
            await query.answer("Quick Market is only available on Ad 5 (BTC/NGN) and Ad 8 (USDT/USD).", show_alert=True)
            return
        if new_mode == "browserbase_market":
            _is_usdt = (ad_data.get("currencyId","").upper() == "USD"
                        and ad_data.get("tokenId","").upper() == "USDT")
            _is_btc_ngn = _is_btc_ngn_ad(ad_data)
            if not (_is_usdt or _is_btc_ngn):
                await query.answer("Browserbase Market is only available for USD/USDT and BTC/NGN ads.", show_alert=True)
                return
        if new_mode == "decodo_market":
            _is_usdt = (ad_data.get("currencyId","").upper() == "USD"
                        and ad_data.get("tokenId","").upper() == "USDT")
            _is_btc_ngn = _is_btc_ngn_ad(ad_data)
            if not (_is_usdt or _is_btc_ngn):
                await query.answer("Quick Market is only available for USD/USDT and BTC/NGN ads.", show_alert=True)
                return
        if new_mode == "ad_copy":
            _is_usdt = (ad_data.get("currencyId","").upper() == "USD"
                        and ad_data.get("tokenId","").upper() == "USDT")
            _is_btc_ngn = _is_btc_ngn_ad(ad_data)
            if not (_is_usdt or _is_btc_ngn):
                await query.answer("Ad Copy is only available for USD/USDT and BTC/NGN ads.", show_alert=True)
                return
            if _is_btc_ngn:
                # Exactly ONE BTC/NGN ad_copy slot per user. Two of them
                # would chase the same prefix against the same market list
                # and end up fighting each other for the same price.
                existing = _find_btc_ngn_ad_copy_slot(sess, exclude_slot=slot_idx)
                if existing is not None:
                    await query.answer(
                        f"{_ad_slot_label(existing)} is already using Ad Copy for BTC/NGN. "
                        f"Only one BTC/NGN Ad Copy ad is allowed — switch that one off first.",
                        show_alert=True
                    )
                    return
        s["mode"] = new_mode
        next_hint = ""
        if slot_idx == -1:
            slot_str = _get_user_slot_str(tuser.id)
            sess.settings[f"mode_{slot_str}"] = new_mode
            next_hint = f"\n\n_{next_setup_hint(tuser.id)}_"
        _save_settings(tuser.id)   # persists Ad 1 AND any extra slots, regardless of which was just edited
        note = " (takes effect next cycle)" if _ad_running(sess, slot_idx) else ""
        await edit_menu(query,
            f"🔀 <b>{_ad_slot_label(slot_idx)} switched to {new_mode.replace('_',' ').upper()}{note}</b>{next_hint}",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 🔝 Ad Copy Range ──
    # ── 🏆 Set Top Range (BTC/NGN Ad Copy) ──
    elif data == "set_top_range":
        sess = _s(tuser.id)
        slot_idx = sess.editing_slot
        s = _ad_settings(sess, slot_idx)
        ad_data = _ad_data_of(sess, slot_idx)
        if not _is_btc_ngn_ad(ad_data):
            await query.answer("Top Range is for BTC/NGN Ad Copy ads only.", show_alert=True)
            return
        _btn_state["action"]       = "top_range"
        _btn_state["prev_section"] = "section_ads"
        _cur = int(s.get("top_range", 1) or 1)
        await edit_menu(query,
            f"🏆 <b>Top Range — {_ad_slot_label(slot_idx)}</b>\n\n"
            f"Current: <code>{_cur}</code>\n\n"
            f"How far down the ranked band (from your Close Price Range) to copy:\n"
            f"• <code>1</code> — the leading (highest) price in the band\n"
            f"• <code>2</code> — the 2nd-highest price in the band\n"
            f"• <code>3</code> — the 3rd-highest, and so on\n\n"
            f"Example with band <code>100200</code>: 100200.9, 100200.5, 100200.3, 100200.03\n"
            f"→ Top Range <code>2</code> copies <code>100200.5</code>.\n\n"
            f"Send a whole number, 1 or higher.",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 👤 Set Merchant Watch (BTC/NGN Ad Copy) ──
    elif data == "set_merchant_watch":
        sess = _s(tuser.id)
        slot_idx = sess.editing_slot
        s = _ad_settings(sess, slot_idx)
        ad_data = _ad_data_of(sess, slot_idx)
        if not _is_btc_ngn_ad(ad_data):
            await query.answer("Merchant Watch is for BTC/NGN Ad Copy ads only.", show_alert=True)
            return
        _btn_state["action"]       = "merchant_watch"
        _btn_state["prev_section"] = "section_ads"
        _cur = (s.get("merchant_username") or "").strip() or "— not set, whole market —"
        await edit_menu(query,
            f"👤 <b>Merchant Watch — {_ad_slot_label(slot_idx)}</b>\n\n"
            f"Current: <code>{_esc(_cur)}</code>\n\n"
            f"Send the exact Bybit nickname of the merchant to track.\n\n"
            f"Once set, each cycle the bot fetches BTC/NGN market ads (ranks 1-300), keeps ONLY "
            f"that merchant's ad(s), then matches your Close Price Range digits against them and "
            f"copies whichever one sits at your Top Range position — the same rule as before, just "
            f"restricted to this one merchant instead of the whole market.\n\n"
            f"If the merchant isn't found that cycle, the bot skips and waits for the next one. "
            f"If the price hasn't changed, it also skips — only a genuine change gets copied.\n\n"
            f"Send <code>clear</code> to remove the restriction and go back to watching the whole market.",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 🎯 Set Close Price Range (BTC/NGN Ad Copy) ──
    elif data == "set_close_price_range":
        sess = _s(tuser.id)
        slot_idx = sess.editing_slot
        s = _ad_settings(sess, slot_idx)
        ad_data = _ad_data_of(sess, slot_idx)
        if not _is_btc_ngn_ad(ad_data):
            await query.answer("Close Price Range is for BTC/NGN Ad Copy ads only.", show_alert=True)
            return
        _btn_state["action"]       = "close_price_range"
        _btn_state["prev_section"] = "section_ads"
        _cur = (s.get("close_price_range") or "").strip() or "— not set —"
        await edit_menu(query,
            f"🎯 <b>Close Price Range — {_ad_slot_label(slot_idx)}</b>\n\n"
            f"Current: <code>{_esc(_cur)}</code>\n\n"
            f"Send the leading digits of the price band you want to track.\n"
            f"Example: <code>1047435</code>\n\n"
            f"Each cycle the bot fetches BTC/NGN market ads (ranks 1-300), keeps only the ads "
            f"whose price starts with those digits, and copies the <b>highest</b> one of them.\n\n"
            f"<i>e.g. with 1047435, against 104743520.00 / 104743530.20 / 104743590.50 / "
            f"104743730.80 it copies 104743590.50 — the last one is a different band (1047437).</i>",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 🔍 View Market Ads List (diagnostic) ──
    elif data == "view_market_ads" or data.startswith("view_market_ads_"):
        sess = _s(tuser.id)
        slot_idx = sess.editing_slot
        s = _ad_settings(sess, slot_idx)
        ad_data = _ad_data_of(sess, slot_idx)
        if not ad_data:
            await query.answer("Fetch ad details first.", show_alert=True)
            return
        # Params encoded directly in callback_data
        # (view_market_ads_s{side}_r{start}-{end}_t{token}) — stateless,
        # so no session storage needed just for a diagnostic view.
        # Range windows go up to 300 now (was capped at 100): 1-50, 50-100,
        # 100-150, 150-200, 200-250, 250-300 — same window set for both
        # BTC and USDT.
        RANGE_OPTIONS = [(1, 50), (50, 100), (100, 150), (150, 200), (200, 250), (250, 300)]
        # BTC/NGN only — extends the manual viewer just past 300, matching
        # the territory Merchant Watch itself falls back to.
        # BTC/NGN only — extends the manual viewer well past 300, up to
        # Bybit's 900-deep territory, in the same 50-item windows as the
        # rest of the viewer.
        EXTENDED_RANGE_OPTIONS_BTC_NGN = [
            (301, 350), (351, 400), (401, 450), (451, 500),
            (501, 550), (551, 600), (601, 650), (651, 700),
            (701, 750), (751, 800), (801, 850), (851, 900),
        ]
        side_override, start_n, end_n, token_override = None, 1, 50, None
        if data.startswith("view_market_ads_"):
            for part in data[len("view_market_ads_"):].split("_"):
                if part.startswith("s"):
                    side_override = part[1:]
                elif part.startswith("r"):
                    try:
                        a, b = part[1:].split("-")
                        start_n, end_n = int(a), int(b)
                    except (ValueError, IndexError):
                        pass
                elif part.startswith("t"):
                    token_override = part[1:]
        want_token    = token_override if token_override is not None else ad_data.get("tokenId","")
        want_currency = ad_data.get("currencyId","")
        want_side     = side_override if side_override is not None else _market_ads_query_side(ad_data)
        creds = get_user_creds(tuser.id, slot=_get_user_slot(tuser.id))
        await query.answer("Fetching live market ads...")
        items = await _fetch_market_ads_up_to(want_token, want_currency, want_side, end_n, creds)
        window = items[start_n - 1:end_n]   # the ranked positions this button actually shows

        # ── Raw field diagnostic dump (server logs only, not sent to the
        # user) ──
        # Investigating whether Bybit's own response already flags an ad
        # as ineligible/eligible for display (authTag, blocked, etc.)
        # rather than that only being decided client-side. Dumping the
        # full raw item — not just the fields bot.py otherwise reads —
        # so nothing Bybit sends back is hidden from this investigation.
        # Kept as one JSON line per ad so it's easy to grep/diff across
        # ranges (rank 1-50 vs 50-100, etc.) in Render's log viewer.
        _diag_fields = [
            "price", "userId", "isOnline", "lastQuantity", "minAmount",
            "maxAmount", "payments", "recentOrderNum", "recentExecuteRate",
            "authTag", "paymentPeriod", "blocked", "makerContact",
            "tradingPreferenceSet",
        ]
        logger.info(
            f"[MarketAdsDiag] {want_token}/{want_currency} side={want_side} "
            f"ranks {start_n}-{end_n} — {len(window)} item(s) — raw field dump follows"
        )
        for _rank, _it in enumerate(window, start_n):
            _diag = {f: _it.get(f, "<MISSING>") for f in _diag_fields}
            logger.info(f"[MarketAdsDiag] rank={_rank} id={_it.get('id','?')} {json.dumps(_diag, default=str)}")

        own_ids = {
            (_ad_settings(sess, i) or {}).get("ad_id","")
            for i in range(-1, TOTAL_AD_SLOTS - 1)
            if (_ad_settings(sess, i) or {}).get("ad_id")
        }
        # Same filtering AND selection the live cycle actually uses — so
        # this view shows exactly what would get copied, not just a raw
        # unfiltered dump. "Would copy" is always evaluated over the full
        # fetched list (rank 1 downward), independent of which window the
        # buttons are currently showing.
        _competing = [
            it for it in items
            if str(it.get("id","")) not in own_ids
            and it.get("tokenId","").upper()    == want_token.upper()
            and it.get("currencyId","").upper() == want_currency.upper()
        ]
        _chosen_price, _chosen_item = _pick_ad_copy_price(_competing)
        header_lines = [
            f"🔍 <b>Live Market Ads</b>",
            f"<code>{_esc(want_token)}/{_esc(want_currency)}</code> · side=<code>{_esc(str(want_side))}</code> "
            f"(your ad's own side is <code>{_esc(str(ad_data.get('side','?')))}</code>)",
            f"👉 Would copy: <code>{_esc(str(_chosen_price))}</code>" if _chosen_price else "⚠️ No eligible ad found.",
            f"📄 Showing ranks {start_n}-{end_n} · {len(items)} item(s) fetched total.",
            "",
        ]
        if not window:
            header_lines.append("No items in this window.")
        # Build each item as ONE complete, self-contained HTML chunk (never
        # split a tag across the truncation boundary) then only add whole
        # chunks while there's room — this is what was crashing edit_menu:
        # blindly slicing the final joined string at a fixed character
        # count could cut a <code> or <b> tag in half, and Telegram's HTML
        # parser rejects the whole message ("unsupported start tag").
        item_chunks = []
        for i, it in enumerate(window, start_n):
            mark = " 🔸YOURS" if str(it.get("id","")) in own_ids else ""
            if _chosen_item is not None and it.get("id") == _chosen_item.get("id"):
                mark += " ⭐WOULD COPY"
            nick = it.get("nickName","?")
            if len(nick) > 18:
                nick = nick[:17] + "…"
            item_chunks.append(
                f"<b>{i}.</b> <code>{_esc(str(it.get('price','?')))}</code>{mark}\n"
                f"    {_esc(nick)} {'🟢' if it.get('isOnline') else '⚪'} · "
                f"{_esc(str(it.get('minAmount','?')))}-{_esc(str(it.get('maxAmount','?')))}"
            )
        header_txt = "\n".join(header_lines)
        budget = 3800 - len(header_txt)
        shown_lines, used = [], 0
        for chunk in item_chunks:
            cost = len(chunk) + 1  # +1 for the joining newline
            if used + cost > budget:
                break
            shown_lines.append(chunk)
            used += cost
        if len(shown_lines) < len(item_chunks):
            shown_lines.append(f"…({len(item_chunks) - len(shown_lines)} more not shown — narrow the range to see them)")
        txt = "\n".join(header_lines + shown_lines)
        rows = [
            [
                InlineKeyboardButton(("✅ " if want_token.upper() == "USDT" else "") + "USDT", callback_data=f"view_market_ads_s{want_side}_r{start_n}-{end_n}_tUSDT"),
                InlineKeyboardButton(("✅ " if want_token.upper() == "BTC"  else "") + "BTC",  callback_data=f"view_market_ads_s{want_side}_r{start_n}-{end_n}_tBTC"),
            ],
            [
                InlineKeyboardButton(("✅ " if want_side == "0" else "") + "Side 0", callback_data=f"view_market_ads_s0_r{start_n}-{end_n}_t{want_token}"),
                InlineKeyboardButton(("✅ " if want_side == "1" else "") + "Side 1", callback_data=f"view_market_ads_s1_r{start_n}-{end_n}_t{want_token}"),
            ],
        ]
        # Range window buttons — 3 per row, same 6 windows for BTC and USDT.
        for row_pair in (RANGE_OPTIONS[0:3], RANGE_OPTIONS[3:6]):
            rows.append([
                InlineKeyboardButton(
                    ("✅ " if (a, b) == (start_n, end_n) else "") + f"{a}-{b}",
                    callback_data=f"view_market_ads_s{want_side}_r{a}-{b}_t{want_token}"
                )
                for a, b in row_pair
            ])
        # Extra ranges beyond 300, BTC/NGN only — matches how deep
        # Merchant Watch itself searches. 3 per row, same style as the
        # base RANGE_OPTIONS above.
        if want_token.upper() == "BTC" and want_currency.upper() == "NGN":
            for i in range(0, len(EXTENDED_RANGE_OPTIONS_BTC_NGN), 3):
                row_pair = EXTENDED_RANGE_OPTIONS_BTC_NGN[i:i+3]
                rows.append([
                    InlineKeyboardButton(
                        ("✅ " if (a, b) == (start_n, end_n) else "") + f"{a}-{b}",
                        callback_data=f"view_market_ads_s{want_side}_r{a}-{b}_t{want_token}"
                    )
                    for a, b in row_pair
                ])
        rows += back_section("section_ads")
        await edit_menu(query, txt, InlineKeyboardMarkup(rows))

    elif data == "set_ad_copy_range":
        sess = _s(tuser.id)
        slot_idx = sess.editing_slot
        s = _ad_settings(sess, slot_idx)
        cur = "top20" if s.get("ad_copy_range") == "top20" else "top10"  # legacy "top5"/"top10" both normalize to "top10" here
        rows = [
            [InlineKeyboardButton(("✅ " if cur == "top10" else "") + "1 - 10", callback_data="ad_copy_range_top10")],
            [InlineKeyboardButton(("✅ " if cur == "top20" else "") + "1 - 20", callback_data="ad_copy_range_top20")],
        ] + back_section("section_ads")
        await edit_menu(query,
            f"🔝 <b>{_ad_slot_label(slot_idx)} — Ad Copy Range</b>\n\n"
            "How deep into the live market listing (page 1) to look ONLY for the "
            "purpose of skipping past your own ad(s). The price copied is always "
            "whatever ad sits at position #1 once your own ads are skipped — this "
            "setting never picks a 'highest' price, just how far it's allowed to "
            "look for the first non-self ad.",
            InlineKeyboardMarkup(rows)
        )

    elif data in ("ad_copy_range_top10", "ad_copy_range_top20"):
        sess = _s(tuser.id)
        slot_idx = sess.editing_slot
        s = _ad_settings(sess, slot_idx)
        s["ad_copy_range"] = "top20" if data.endswith("top20") else "top10"
        _save_settings(tuser.id)
        note = " (takes effect next cycle)" if _ad_running(sess, slot_idx) else ""
        await edit_menu(query,
            f"🔝 <b>{_ad_slot_label(slot_idx)} Ad Copy range set to "
            f"{_ad_copy_range_label(s['ad_copy_range'])}{note}</b>",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── ➕ Set Increment ──
    elif data == "set_increment":
        _btn_state["action"]       = "increment"
        _btn_state["prev_section"] = "section_ads"
        sess = _s(tuser.id)
        s = _ad_settings(sess, sess.editing_slot)
        await edit_menu(query,
            f"➕ <b>Set Increment — {_ad_slot_label(sess.editing_slot)}</b>\n\nCurrent: <code>+{s.get('increment','0.05')}</code> per cycle\n\n"
            "Send the amount to add each cycle.\nExamples: `0.05` | `1` | `0.5`",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── ↔️ Set Manual Nudge Amount (Ad 2/Ad 3 only) ──
    elif data == "set_manual_nudge":
        _btn_state["action"]       = "manual_nudge"
        _btn_state["prev_section"] = "section_ads"
        sess = _s(tuser.id)
        slot_idx = sess.editing_slot
        s = _ad_settings(sess, slot_idx)
        ad_data = _ad_data_of(sess, slot_idx)
        cur = ad_data.get("currencyId","").upper()
        tok = ad_data.get("tokenId","").upper()
        _auto_gap = get_min_price_gap(cur, tok, None)
        _cur_val = (s.get("manual_nudge") or "").strip()
        await edit_menu(query,
            f"↔️ <b>Set Nudge Amount — {_ad_slot_label(slot_idx)}</b>\n\n"
            f"Pair: <code>{_esc(tok)}/{_esc(cur)}</code>\n"
            f"Automatic default for this pair: <code>{_auto_gap}</code>\n"
            f"Current manual override: <code>{_cur_val if _cur_val else '— none, using automatic —'}</code>\n\n"
            f"Send the amount to nudge {_ad_slot_label(slot_idx)}'s price down whenever it "
            f"would otherwise land too close to a senior ad's price (or, for fast-chase, "
            f"how much to subtract from Ad 1's freshly fetched price).\n"
            f"Example: <code>5</code> or <code>10</code>\n\n"
            f"Send <code>0</code> or <code>clear</code> to remove the override and go back to the automatic amount.",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 📊 Set Float % ──
    elif data == "set_float_pct":
        sess = _s(tuser.id)
        slot_idx = sess.editing_slot
        ad_data = _ad_data_of(sess, slot_idx)
        if not ad_data:
            await edit_menu(query, "❌ Fetch Ad Details first.", InlineKeyboardMarkup(back_section("section_ads")))
            return
        token    = ad_data.get("tokenId","USDT").upper()
        currency = ad_data.get("currencyId","NGN").upper()
        max_pct  = get_max_float_pct(currency, token)
        min_pct  = get_min_float_pct(currency, token)
        needs_ref = currency_needs_ref(currency) or currency == "NGN"
        _btn_state["action"]       = "float_pct"
        _btn_state["prev_section"] = "section_ads"
        s   = _ad_settings(sess, slot_idx)
        cur = s.get("float_pct","") or "Not set"
        formula = (
            f"<code>{token}/USDT × {currency}/USDT ref × your% ÷ 100</code>"
            if needs_ref else
            f"<code>{token}/USDT × your% ÷ 100</code>"
        )
        other_pcts = sess.get_active_float_pcts(exclude_index=slot_idx, currency_id=currency, token_id=token)
        gap_note = (
            f"\n\nℹ️ Your other active {token}/{currency} ad(s) are also using: "
            f"{', '.join(f'{p}%' for p in other_pcts)} — that's fine, using the same % is allowed. "
            f"The bot automatically keeps the posted PRICES far enough apart."
            if other_pcts else ""
        )
        await edit_menu(query,
            f"📊 <b>Set Float % — {_ad_slot_label(slot_idx)}</b>\n\nPair: <code>{token}/{currency}</code> | Range: <code>{min_pct}%–{max_pct}%</code>\nCurrent: <code>{cur}</code>\n\n"
            f"Formula: {formula}{gap_note}\n\n"
            f"Send a value between <code>{min_pct}</code> and <code>{max_pct}</code>. Example: <code>105</code>",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 💱 Set NGN Ref ──
    elif data == "set_ngn_ref":
        _btn_state["action"]       = "ngn_usdt_ref"
        _btn_state["prev_section"] = "section_ads"
        sess = _s(tuser.id)
        ad_data = _ad_data_of(sess, sess.editing_slot)
        _rcur = ad_data.get("currencyId","NGN").upper() if ad_data else "NGN"
        cur   = sess.shared_local_usdt_ref or "Not set"
        shared_note = " (shared by all your ads)" if _multi_ad(sess) else ""
        await edit_menu(query,
            f"💱 <b>{_rcur}/USDT Reference Price{shared_note}</b>\n\nCurrent: <code>{cur}</code>\n\n"
            f"Check Bybit P2P market for current {_rcur}/USDT rate.\n"
            f"Example: <code>{'1580' if _rcur == 'NGN' else '1.25' if _rcur == 'EUR' else '100'}</code> ({_rcur} per 1 USDT)",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── ⏱ Set Interval ──
    elif data == "set_interval":
        _btn_state["action"]       = "interval"
        _btn_state["prev_section"] = "section_ads"
        sess = _s(tuser.id)
        s = _ad_settings(sess, sess.editing_slot)
        ad_data = _ad_data_of(sess, sess.editing_slot)
        _floor = _interval_floor_secs(s, ad_data)
        if _floor is not None:
            _pair = f"{ad_data.get('tokenId','')}/{ad_data.get('currencyId','')}"
            await edit_menu(query,
                f"⏱ <b>Set Interval — {_ad_slot_label(sess.editing_slot)}</b> ({_esc(_pair)})\n\n"
                f"Current: every <code>{_ad_interval_label(s, ad_data)}</code>\n\n"
                f"Send the number of <b>SECONDS</b> between each market check "
                f"(minimum {_floor}).\n"
                f"Examples: <code>{_floor}</code> | <code>60</code> (1 min) | <code>300</code> (5 min)\n\n"
                f"<i>Checking often is safe in copy mode — the price is only edited when a new "
                f"price actually appears, otherwise the check just skips.</i>",
                InlineKeyboardMarkup(back_section("section_ads"))
            )
        else:
            await edit_menu(query,
                f"⏱ <b>Set Interval — {_ad_slot_label(sess.editing_slot)}</b>\n\nCurrent: every <code>{s.get('interval',2)}</code> min\n\n"
                f"Send minutes between each price update (minimum {bybit.MIN_AD_INTERVAL_MINUTES}).\nExamples: `2` | `5` | `10`",
                InlineKeyboardMarkup(back_section("section_ads"))
            )

    # ── 🔄 Update Once Now ──
    elif data == "update_now":
        if any(_ad_running(_s(tuser.id), _i) for _i in range(0, TOTAL_AD_SLOTS - 1)):
            await edit_menu(query,
                "❌ <b>Update Once Now</b> is only available when no other ad is running.\n\n"
                "Stop your other ads first if you need a one-off manual update on Ad 1.",
                InlineKeyboardMarkup(back_section("section_ads"))
            )
            return
        if not _s(tuser.id).ad_data or not _s(tuser.id).settings.get("ad_id"):
            await edit_menu(query, "❌ Load ad details first.", InlineKeyboardMarkup(back_section("section_ads")))
            return
        # Load per-user creds — MUST be done before modify_ad
        _update_creds = get_user_creds(tuser.id)
        if not _update_creds or not _update_creds.get("key"):
            await edit_menu(query,
                "❌ <b>No Bybit API key found.</b>\n\nGo to 🔑 Set APIs → Set Bybit API first.",
                InlineKeyboardMarkup(back_section("section_ads")))
            return
        mode = _s(tuser.id).settings.get("mode","fixed")
        await edit_menu(query, f"⏳ Updating ({_mode_display_label(mode)} mode)...", ads_section_keyboard(tuser.id))
        if mode == "fixed":
            # ── FIX: always compute the NEXT price (base + increment), not the last applied price.
            # current_price is 0 if the auto-loop has never run, so we start from ad_data["price"].
            # This mirrors exactly what auto_update_loop does on each cycle.
            _increment = Decimal(str(_s(tuser.id).settings.get("increment", "0.05")))
            _base = _s(tuser.id).current_price if _s(tuser.id).current_price else Decimal(str(_s(tuser.id).ad_data.get("price", "0")))
            _next_price = _base + _increment
            price = str(_next_price.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP))
        else:
            # ── FIX: call calc_floating_price directly (it is sync) — do NOT wrap in
            # run_in_executor which causes tuple-unpacking to silently fail.
            try:
                float_pct = float(_s(tuser.id).settings.get("float_pct") or 0)
            except (TypeError, ValueError):
                float_pct = 0
            if float_pct <= 0:
                await edit_menu(query,
                    "❌ Float % isn't set yet. Tap 📊 Set Float % first, then Update Once Now.",
                    InlineKeyboardMarkup(back_section("section_ads")))
                return
            local_usdt_ref = float(_s(tuser.id).settings.get("local_usdt_ref") or 0)
            price, err     = calc_floating_price(_s(tuser.id).ad_data, float_pct, local_usdt_ref)
            if err:
                await edit_menu(query, f"❌ <code>{_esc(str(err))}</code>", InlineKeyboardMarkup(back_section("section_ads")))
                return
        _record_modify_ad1(_s(tuser.id))
        result = await asyncio.get_event_loop().run_in_executor(
            _ad_executor, modify_ad, _s(tuser.id).settings["ad_id"], price, _s(tuser.id).ad_data, _update_creds
        )
        rc = result.get("retCode", result.get("ret_code",-1))
        rm = result.get("retMsg",  result.get("ret_msg",""))
        if rc == 912120022:
            # Same bounded retry as auto_update_loop — see there for why a
            # single exact-boundary attempt wasn't reliable enough.
            for _attempt in range(3):
                min_str, max_str = _extract_bybit_bounds(rm)
                was_too_high = max_str is not None
                bound_str = max_str if was_too_high else min_str
                if not bound_str:
                    break
                bound_dec = Decimal(bound_str)
                if _attempt < 2:
                    candidate_str = bound_str   # Bybit's own string, exactly — see auto_update_loop for why
                else:
                    margin        = _safety_margin(bound_dec)
                    candidate     = (bound_dec - margin) if was_too_high else (bound_dec + margin)
                    safe_rounding = ROUND_FLOOR if was_too_high else ROUND_CEILING
                    candidate_str = str(candidate.quantize(Decimal("0.01"), rounding=safe_rounding))
                if not _can_modify_ad1(_s(tuser.id)):
                    logger.info(f"[UpdateOnce] user {tuser.id} — proceeding past internal budget (blocking disabled by request)")
                _record_modify_ad1(_s(tuser.id))
                result = await asyncio.get_event_loop().run_in_executor(
                    _ad_executor, modify_ad, _s(tuser.id).settings["ad_id"], candidate_str, _s(tuser.id).ad_data, _update_creds
                )
                rc    = result.get("retCode", result.get("ret_code",-1))
                rm    = result.get("retMsg",  result.get("ret_msg",""))
                price = candidate_str
                if rc == 0 or rc != 912120022:
                    break
        elif rc == 90043:
            # Price rounds to the same value the ad already has live —
            # nudge by this pair's minimum gap and retry once (same
            # recoverable case as in auto_update_loop, see there for why).
            _ad_data_now = _s(tuser.id).ad_data
            _nudge = get_min_price_gap(_ad_data_now.get("currencyId",""), _ad_data_now.get("tokenId",""), Decimal(str(price)))
            _nudged_price = str((Decimal(str(price)) - _nudge).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
            _record_modify_ad1(_s(tuser.id))
            result = await asyncio.get_event_loop().run_in_executor(
                _ad_executor, modify_ad, _s(tuser.id).settings["ad_id"], _nudged_price, _ad_data_now, _update_creds
            )
            rc    = result.get("retCode", result.get("ret_code",-1))
            rm    = result.get("retMsg",  result.get("ret_msg",""))
            price = _nudged_price
        if rc == 0:
            # ── Advance current_price so the next cycle (auto or manual) continues from here.
            # Routed through _set_ad_current_price (not a direct assignment) so a manual
            # update also keeps the fast-chase ceiling reference in sync with reality —
            # same fix as the scheduled-cycle staleness bug, applied here too.
            _set_ad_current_price(_s(tuser.id), -1, Decimal(str(price)))
            await edit_menu(query,
                f"✅ <b>Updated!</b> Price: <code>{price}</code> ({mode.replace('_',' ').upper()})\n\n_{next_setup_hint(tuser.id)}_",
                InlineKeyboardMarkup(back_section("section_ads"))
            )
        else:
            await edit_menu(query, f"❌ <code>{rc}</code> — <code>{_esc(rm)}</code>", InlineKeyboardMarkup(back_section("section_ads")))

    # ── 🔑 API Setup Section ──
    elif data == "section_apis":
        uid  = query.from_user.id
        bk1  = "✅" if db.get_api(uid, "bybit_key_1")    else "❌"
        bk2  = "✅" if db.get_api(uid, "bybit_key_2")    else "❌"
        await edit_menu_html(query,
            f"🔑 <b>API Setup</b>\n\n"
            f"Your API keys are stored securely on the server.\n\n"
            f"Bybit Account 1 API: {bk1}\n"
            f"Bybit Account 2 API: {bk2}\n\n"
            f"⚠️ Keys are stored per user and never shared.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton(f"🔑 {bk1} Set Bybit Account 1 API", callback_data="set_api_bybit_1")],
                [InlineKeyboardButton(f"🔑 {bk2} Set Bybit Account 2 API", callback_data="set_api_bybit_2")],
                [InlineKeyboardButton("🗑 Delete All APIs",                 callback_data="delete_apis")],
                *back_main()
            ])
        )

    elif data == "set_api_bybit":
        # Legacy callback — redirect to account 1
        _btn_state["action"]       = "api_bybit_key_1"
        _btn_state["prev_section"] = "section_apis"
        _btn_state["_api_bybit_slot"] = "1"
        uid = query.from_user.id
        has = bool(db.get_api(uid, "bybit_key_1"))
        await edit_menu(query,
            f"🔑 <b>Set Bybit Account 1 API Key</b>\n\n"
            f"Status: {'✅ Key saved — new key will replace it' if has else '❌ Not set'}\n\n"
            "Send your Bybit API Key for Account 1.",
            InlineKeyboardMarkup(back_section("section_apis"))
        )

    elif data == "set_api_bybit_1":
        _btn_state["action"]          = "api_bybit_key_1"
        _btn_state["prev_section"]    = "section_apis"
        _btn_state["_api_bybit_slot"] = "1"
        uid = query.from_user.id
        has = bool(db.get_api(uid, "bybit_key_1"))
        await edit_menu(query,
            f"🔑 <b>Set Bybit Account 1 API Key</b>\n\n"
            f"Status: {'✅ Key saved — new key will replace it' if has else '❌ Not set'}\n\n"
            "Send your Bybit API Key for Account 1.",
            InlineKeyboardMarkup(back_section("section_apis"))
        )

    elif data == "set_api_bybit_2":
        _btn_state["action"]          = "api_bybit_key_2"
        _btn_state["prev_section"]    = "section_apis"
        _btn_state["_api_bybit_slot"] = "2"
        uid = query.from_user.id
        has = bool(db.get_api(uid, "bybit_key_2"))
        await edit_menu(query,
            f"🔑 <b>Set Bybit Account 2 API Key</b>\n\n"
            f"Status: {'✅ Key saved — new key will replace it' if has else '❌ Not set'}\n\n"
            "Send your Bybit API Key for Account 2.",
            InlineKeyboardMarkup(back_section("section_apis"))
        )

    elif data == "delete_apis":
        uid_d = query.from_user.id
        bk1 = "✅" if db.get_api(uid_d, "bybit_key_1")    else "—"
        bk2 = "✅" if db.get_api(uid_d, "bybit_key_2")    else "—"
        await edit_menu(query,
            f"🗑 <b>Delete API Keys</b>\n\n"
            f"Choose which keys to delete. This cannot be undone.\n\n"
            f"Bybit Account 1: {bk1}\n"
            f"Bybit Account 2: {bk2}",
            InlineKeyboardMarkup([
                [InlineKeyboardButton(f"🔑 Delete Bybit Acct 1 API {bk1}", callback_data="delete_bybit1_apis")],
                [InlineKeyboardButton(f"🔑 Delete Bybit Acct 2 API {bk2}", callback_data="delete_bybit2_apis")],
                [InlineKeyboardButton("🗑 Delete ALL APIs",                 callback_data="delete_apis_confirm")],
                [InlineKeyboardButton("❌ Cancel",                          callback_data="section_apis")],
            ])
        )

    elif data == "delete_apis_confirm":
        uid_del = query.from_user.id
        db.delete_all_apis(uid_del)
        await edit_menu(query,
            "✅ *All API keys deleted.*\n\n"
            "Your account is still active but API credentials have been removed.\n"
            "Re-enter them anytime via 🔑 Set APIs.",
            InlineKeyboardMarkup([*back_section("section_apis")])
        )

    # ── Granular delete confirmations ──
    elif data == "delete_bybit1_apis":
        uid_d = query.from_user.id
        has   = bool(db.get_api(uid_d, "bybit_key_1"))
        await edit_menu(query,
            f"🔑 <b>Delete Bybit Account 1 API?</b>\n\n"
            f"Status: {'✅ Saved' if has else '❌ Already empty'}\n\n"
            "This permanently removes your Account 1 API key and secret.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Delete", callback_data="delete_bybit1_confirm")],
                [InlineKeyboardButton("❌ Cancel",       callback_data="delete_apis")],
            ])
        )

    elif data == "delete_bybit1_confirm":
        uid_del = query.from_user.id
        db.save_api(uid_del, "bybit_key_1",    "")
        db.save_api(uid_del, "bybit_secret_1", "")
        # If this user is currently on slot 1, reset their slot to 0 (no global change)
        if _s(uid_del).selected_slot == 0:
            logger.info(f"[APIs] Bybit Account 1 keys deleted for user {uid_del} (was on slot 1)")
        await edit_menu(query,
            "✅ *Bybit Account 1 API deleted.*\n\nYou can re-add it anytime via 🔑 Set APIs.",
            InlineKeyboardMarkup([*back_section("section_apis")])
        )

    elif data == "delete_bybit2_apis":
        uid_d = query.from_user.id
        has   = bool(db.get_api(uid_d, "bybit_key_2"))
        await edit_menu(query,
            f"🔑 <b>Delete Bybit Account 2 API?</b>\n\n"
            f"Status: {'✅ Saved' if has else '❌ Already empty'}\n\n"
            "This permanently removes your Account 2 API key and secret.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Delete", callback_data="delete_bybit2_confirm")],
                [InlineKeyboardButton("❌ Cancel",       callback_data="delete_apis")],
            ])
        )

    elif data == "delete_bybit2_confirm":
        uid_del = query.from_user.id
        db.save_api(uid_del, "bybit_key_2",    "")
        db.save_api(uid_del, "bybit_secret_2", "")
        # If this user is currently on slot 2, reset their slot to 0 (no global change)
        if _s(uid_del).selected_slot == 1:
            _s(uid_del).selected_slot = 0
            logger.info(f"[APIs] Bybit Account 2 keys deleted for user {uid_del} — slot reset to 1")
        await edit_menu(query,
            "✅ *Bybit Account 2 API deleted.*\n\nYou can re-add it anytime via 🔑 Set APIs.",
            InlineKeyboardMarkup([*back_section("section_apis")])
        )

    # ── ⬆️ Upgrade Plan ──
    elif data == "upgrade_plan":
        uid   = query.from_user.id
        badge = sub.plan_badge(uid)
        exp   = db.get_plan_expiry_str(uid)
        user_rec = db.get_user(uid)
        pend  = user_rec.get("upgrade_pending", False) if user_rec else False
        if db.is_pro(uid):
            await edit_menu(query,
                f"💎 <b>You are already on Pro!</b>\n\n{exp}\n\nAll features are unlocked.",
                InlineKeyboardMarkup(back_main())
            )
            return
        if pend:
            await edit_menu(query,
                "⏳ *Upgrade request already pending.*\n\n"
                "The admin will review and approve shortly.\n"
                "You will receive a notification when approved.",
                InlineKeyboardMarkup(back_main())
            )
            return
        await edit_menu(query,
            f"⬆️ <b>Upgrade to Pro Plan</b>\n\n"
            f"Current: {badge}\n\n"
            f"Pro unlocks:\n"
            f"  ✅ Auto Price Update bot\n"
            f"  ✅ Auto-Pay (auto marks BUY orders paid)\n"
            f"  ✅ 8 ads: BTC/USD, BTC/NGN, ETH/USD, USDT/USD\n\n"
            f"Tap <b>Request Upgrade</b> to send a request to the admin.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Request Upgrade", callback_data="upgrade_request_yes")],
                [InlineKeyboardButton("❌ Cancel",          callback_data="main_menu")],
            ])
        )

    elif data == "upgrade_request_yes":
        _btn_state["action"] = "upgrade_contact"
        await edit_menu(query,
            "📞 <b>One more step before we submit your request</b>\n\n"
            "Send your Telegram username or WhatsApp number (with country code) so the admin can reach you before approving:\n\n"
            "• Telegram: <code>@johnsmith</code>\n"
            "• WhatsApp: <code>+2348012345678</code>",
            InlineKeyboardMarkup(back_main())
        )
                # The background _upgrade_notifier_loop will retry in 30 s

    # ── 🟢/🔴 Toggle Price Update ──
    elif data == "toggle_refresh":
        sess     = _s(tuser.id)
        slot_idx = sess.editing_slot
        s        = _ad_settings(sess, slot_idx)
        label    = _ad_slot_label(slot_idx)
        if _ad_running(sess, slot_idx):
            ad_data_now = _ad_data_of(sess, slot_idx)
            ad_data_now = _ad_data_of(sess, slot_idx)
            _set_ad_running(sess, slot_idx, False)
            _set_ad_task(sess, slot_idx, None)
            _set_ad_current_price(sess, slot_idx, Decimal("0"))
            await edit_menu(query,
                f"🔴 <b>{label} price update stopped.</b>\n\n" + ads_section_text(tuser.id),
                ads_section_keyboard(tuser.id)
            )
        else:
            ad_data = _ad_data_of(sess, slot_idx)
            if not ad_data or not s.get("ad_id"):
                hint_text = next_setup_hint(tuser.id) if slot_idx == -1 else "Set this ad's Ad ID and fetch its details first."
                await edit_menu(query,
                    f"❌ Not ready:\n\n_{hint_text}_",
                    InlineKeyboardMarkup(back_section("section_ads"))
                )
                return
            # ── Interval floor — defense in depth (already enforced when the
            # value was entered, but re-checked here in case of stale state) ──
            # USDT/USD runs on a seconds-level floor (5s) rather than the
            # 2-minute one, so it must be checked against its own rule —
            # otherwise its synced-down minutes value would fail the
            # minutes floor and block the ad from ever starting.
            _floor = _interval_floor_secs(s, ad_data)
            if _floor is not None:
                ok, err = validate_interval_seconds(_ad_interval_seconds(s, ad_data), floor=_floor)
            else:
                ok, err = validate_interval(s.get("interval", 2))
            if not ok:
                await edit_menu(query, err, InlineKeyboardMarkup(back_section("section_ads")))
                return
            # ── Float % range check — defense in depth (already enforced
            # when the value was entered, re-checked here in case of stale
            # state). Matching another active ad's % is allowed — the bot
            # keeps the actual posted PRICES apart live, in the loop
            # itself, not by restricting the % at this stage. ──
            if s.get("mode") == "floating":
                fok, ferr = validate_float_pct(
                    ad_data.get("currencyId","NGN"), ad_data.get("tokenId","USDT"),
                    s.get("float_pct", 0)
                )
                if not fok:
                    await edit_menu(query, ferr, InlineKeyboardMarkup(back_section("section_ads")))
                    return
            # ── Pair guard: each Ad slot is tied to one market pair ──
            if not _ad_matches_slot_pair(slot_idx, ad_data):
                await edit_menu(query,
                    f"❌ <b>{label} is a {_slot_pair_label(slot_idx)} ad slot.</b>\n\n"
                    f"The ad loaded here is "
                    f"{ad_data.get('tokenId','?')}/{ad_data.get('currencyId','?')}. "
                    f"Set a {_slot_pair_label(slot_idx)} Ad ID and fetch it again.",
                    InlineKeyboardMarkup(back_section("section_ads"))
                )
                return
            # ── Quick Market: only 1 active BTC/NGN and 1 active USDT/USD ──
            if s.get("mode") in ("decodo_market", "browserbase_market"):
                _pair_now = (ad_data.get("tokenId","").upper(), ad_data.get("currencyId","").upper())
                if _pair_now in QUICK_MARKET_PAIRS:
                    for _oi in range(-1, len(sess.extra_ad_slots)):
                        if _oi == slot_idx or not _ad_running(sess, _oi):
                            continue
                        _od = _ad_data_of(sess, _oi)
                        if (_od.get("tokenId","").upper(), _od.get("currencyId","").upper()) == _pair_now:
                            await edit_menu(query,
                                f"⚠️ <b>Cannot start {label}</b>\n\n"
                                f"You can't have more than 1 active BTC/NGN or 1 active USDT/USD "
                                f"ad in Quick Market. <b>{_ad_slot_label(_oi)}</b> is already running "
                                f"{_pair_now[0]}/{_pair_now[1]}.",
                                InlineKeyboardMarkup(back_section("section_ads"))
                            )
                            return
            # ── BTC/NGN Ad Copy exclusivity — defense in depth ──
            # The real gate is at mode-switch time (set_mode_ad_copy), but
            # that only catches it if the ad's pair was already known AT
            # THAT moment. A slot switched to ad_copy BEFORE its ad_id was
            # ever fetched (ad_data empty, so the pair check couldn't run
            # yet) could slip through and only turn out to be BTC/NGN
            # later, after a fetch. So re-check here too, right before
            # actually starting — two BTC/NGN ad_copy loops running
            # together would both chase the same market list and, worse,
            # each other's freshly-posted prices, fighting for the same
            # band.
            if s.get("mode") == "ad_copy" and _is_btc_ngn_ad(ad_data):
                _existing_btc_copy = _find_btc_ngn_ad_copy_slot(sess, exclude_slot=slot_idx)
                if _existing_btc_copy is not None:
                    await edit_menu(query,
                        f"⚠️ <b>Cannot start {label}</b>\n\n"
                        f"<b>{_ad_slot_label(_existing_btc_copy)}</b> is already running Ad Copy for BTC/NGN.\n\n"
                        f"Only one BTC/NGN Ad Copy ad is allowed at a time — switch "
                        f"{_ad_slot_label(_existing_btc_copy)} to a different mode first.",
                        InlineKeyboardMarkup(back_section("section_ads"))
                    )
                    return
            mode     = s.get("mode","fixed")
            interval = s.get("interval",2)
            if _ad_running(sess, slot_idx):
                # A concurrent press already won the race and started this
                # ad while we were validating above — don't spin up a
                # second task for the same slot. Two tasks both polling and
                # editing the same ad_id independently is exactly what
                # caused near-simultaneous MODIFY calls with drifting
                # internal price state and repeated 90043 rejections.
                await edit_menu(query,
                    f"⚠️ <b>{label} is already running.</b>\n\n" + ads_section_text(tuser.id),
                    ads_section_keyboard(tuser.id)
                )
                return
            _reset_ad_failures(sess, slot_idx)
            _set_ad_running(sess, slot_idx, True)   # set BEFORE create_task, no await in between — closes the race
            task = asyncio.create_task(auto_update_loop(context.bot, chat_id, slot_idx))
            _set_ad_task(sess, slot_idx, task)
            await edit_menu(query,
                f"🟢 <b>{label} price update started!</b>\n🔀 <code>{mode.replace('_',' ').upper()}</code> | ⏱ every <code>{_ad_interval_label(s, ad_data)}</code>\n\n"
                + ads_section_text(tuser.id),
                ads_section_keyboard(tuser.id)
            )

    # ── 🔀 Multi-ad: switch which ad the menu is editing ──
    elif re.fullmatch(r"edit_ad_[1-8]", data):
        sess = _s(tuser.id)
        _ensure_ad_slots(sess)
        target = int(data[-1]) - 2   # "edit_ad_1" -> -1, "edit_ad_2" -> 0 ... "edit_ad_8" -> 6
        if target == -1 or target < len(sess.extra_ad_slots):
            sess.editing_slot = target
        await edit_menu(query, ads_section_text(tuser.id), ads_section_keyboard(tuser.id))

    # ── 🗂 All-ads dashboard ──
    elif data == "ads_dashboard":
        await edit_menu(query, ads_dashboard_text(tuser.id), ads_dashboard_keyboard(tuser.id))

    # ── ⏹ Stop one specific ad from the dashboard ──
    elif re.fullmatch(r"stop_ad_[1-8]", data):
        sess = _s(tuser.id)
        target = int(data[-1]) - 2
        sess.stop_ad_slot(target)
        _set_ad_current_price(sess, target, Decimal("0"))
        await edit_menu(query,
            f"🔴 <b>{_ad_slot_label(target)} stopped.</b>\n\n" + ads_dashboard_text(tuser.id),
            ads_dashboard_keyboard(tuser.id)
        )

    # ── ⏹ Stop all ads in one tap ──
    elif data == "stop_all_ads":
        sess = _s(tuser.id)
        for i in range(-1, len(sess.extra_ad_slots)):
            sess.stop_ad_slot(i)
            _set_ad_current_price(sess, i, Decimal("0"))
        await edit_menu(query,
            "🔴 <b>All ads stopped.</b>\n\n" + ads_dashboard_text(tuser.id),
            ads_dashboard_keyboard(tuser.id)
        )

# ─────────────────────────────────────────
# 📝 TEXT INPUT HANDLER
# ─────────────────────────────────────────
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):

    tuser = update.effective_user
    uid   = tuser.id
    _get_or_register_user(tuser)  # ensure user exists in DB

    text = update.message.text.strip()

    # ── Per-user isolated state ──
    # Admin uses the global user_state dict.
    # Non-admin users get their own state via context.user_data so their
    # API key inputs are isolated and don't collide with the admin's state.
    if is_admin(uid):
        _state = user_state
    else:
        if "state" not in context.user_data:
            context.user_data["state"] = {}
        _state = context.user_data["state"]

    action = _state.get("action")
    prev   = _state.get("prev_section", "main_menu")

    async def reply_with_back(msg: str):
        """Reply with success message + back-to-previous button."""
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=back_prev(prev))

    # ── Bybit API — slot-aware (Account 1 or Account 2) ──
    if action in ("api_bybit_key", "api_bybit_key_1", "api_bybit_key_2"):
        slot = "2" if action == "api_bybit_key_2" else "1"
        val  = text.strip()
        next_action = f"api_bybit_secret_{slot}"
        _state["action"]              = next_action
        _state["prev_section"]        = "section_apis"
        _state["_api_bybit_key_temp"] = val
        _state["_api_bybit_slot"]     = slot
        await update.message.reply_text(
            f"✅ Account {slot} API Key received.\n\n"
            f"Step 2 of 2: Send your Bybit Account {slot} <b>API Secret</b>.",
            parse_mode="HTML"
        )
        return

    elif action in ("api_bybit_secret", "api_bybit_secret_1", "api_bybit_secret_2"):
        uid      = update.effective_user.id
        slot     = _state.pop("_api_bybit_slot", "1")
        key_temp = _state.pop("_api_bybit_key_temp", "")
        db.save_api(uid, f"bybit_key_{slot}",    key_temp)
        db.save_api(uid, f"bybit_secret_{slot}", text.strip())
        # Credentials are loaded per-call via get_user_creds() — no global mutation needed.
        _state["action"] = None
        _save_settings(uid)
        await update.message.reply_text(
            f"✅ <b>Bybit Account {slot} API saved!</b>\n\n"
            f"Key and Secret stored securely.\n"
            f"The bot uses Account {slot} keys when Account {slot} is active.",
            parse_mode="HTML",
            reply_markup=back_prev("section_apis")
        )
        return

    if action == "ad_id":
        sess = _s(uid)
        slot_idx = sess.editing_slot
        if slot_idx == -1:
            # Save under BOTH the slot-keyed key and the generic fallback key
            slot_str = _get_user_slot_str(uid)
            sess.settings[f"ad_id_{slot_str}"] = text.strip()
            sess.settings["ad_id"]              = text.strip()
            sess.ad_data.clear()
            _save_settings(uid)   # persisted — Ad 1 lives on disk per account slot
            logger.info(f"[AdID] Saved ad_id for user={uid} slot={slot_str} ad_id={text.strip()!r}")
            label = f"Account {slot_str}"
        else:
            # Ad 2/Ad 3's ad_id — now persisted too (see _save_settings),
            # not ephemeral in-memory-only as before.
            s = _ad_settings(sess, slot_idx)
            s["ad_id"] = text.strip()
            _ad_data_of(sess, slot_idx).clear()
            _save_settings(uid)
            logger.info(f"[AdID] Saved ad_id for user={uid} {_ad_slot_label(slot_idx)} ad_id={text.strip()!r}")
            label = _ad_slot_label(slot_idx)
        _state["action"] = None
        hint = next_setup_hint(uid) if slot_idx == -1 else "Now use Fetch Ad Details for this ad."
        await update.message.reply_text(
            f"✅ <b>Ad ID saved for {label}!</b>\n\n"
            f"<code>{_esc(text.strip())}</code>\n\n"
            f"<i>{_esc(hint)}</i>",
            parse_mode="HTML",
            reply_markup=back_prev("section_ads")
        )

    elif action == "bybit_uid":
        # Save under the slot-keyed key ONLY.
        # The generic "bybit_uid" key is synced from the ACTIVE slot's value so it
        # always reflects the current slot without leaking into other slots.
        slot_str = _get_user_slot_str(uid)
        _s(uid).settings[f"bybit_uid_{slot_str}"] = text.strip()
        # Keep generic key in sync with current slot (used by chat monitor etc.)
        _s(uid).settings["bybit_uid"] = text.strip()
        _state["action"] = None
        # Persist to disk immediately so it survives /start, restarts, slot switches
        _save_settings(uid)
        logger.info(f"[UID] Saved bybit_uid for user={uid} slot={slot_str} uid_value={text.strip()!r}")
        hint = next_setup_hint(uid)
        # Return to section_ads with back button pointing to the AD PRICE BOT menu
        try:
            await update.message.reply_text(
                f"✅ <b>UID saved for Account {slot_str}!</b>\n\n"
                f"<code>{_esc(text.strip())}</code>\n\n"
                f"<i>{_esc(hint)}</i>",
                parse_mode="HTML",
                reply_markup=back_prev("section_ads")
            )
        except Exception as _uid_reply_err:
            logger.warning(f"[UID] Reply failed: {_uid_reply_err}")
            await update.message.reply_text(
                f"✅ UID saved: <code>{_esc(text.strip())}</code>",
                parse_mode="HTML"
            )

    elif action == "increment":
        try:
            val = Decimal(text)
            if val <= 0: raise ValueError
            sess = _s(uid)
            slot_idx = sess.editing_slot
            s = _ad_settings(sess, slot_idx)
            s["increment"] = text
            if slot_idx == -1:
                slot_str = _get_user_slot_str(uid)
                sess.settings[f"increment_{slot_str}"] = text
            _save_settings(uid)
            _state["action"] = None
            hint = next_setup_hint(uid) if slot_idx == -1 else ""
            await reply_with_back(f"✅ <b>{_ad_slot_label(slot_idx)} increment saved!</b>\n\n<code>+{_esc(text)}</code> per cycle\n\n<i>{_esc(hint)}</i>")
        except Exception:
            await update.message.reply_text("❌ Send a positive number like `0.05`", parse_mode="HTML")

    elif action == "manual_nudge":
        sess = _s(uid)
        slot_idx = sess.editing_slot
        s = _ad_settings(sess, slot_idx)
        cleared = text.strip().lower() in ("0", "clear", "none", "reset")
        if cleared:
            s["manual_nudge"] = ""
            _save_settings(uid)
            _state["action"] = None
            await reply_with_back(
                f"✅ <b>{_ad_slot_label(slot_idx)} nudge override cleared</b> — back to the automatic amount."
            )
        else:
            try:
                val = Decimal(text)
                if val <= 0: raise ValueError
                s["manual_nudge"] = text.strip()
                _save_settings(uid)
                _state["action"] = None
                await reply_with_back(
                    f"✅ <b>{_ad_slot_label(slot_idx)} nudge amount saved!</b>\n\n"
                    f"<code>{_esc(text.strip())}</code> will be subtracted whenever {_ad_slot_label(slot_idx)}'s "
                    f"price would otherwise sit too close to a senior ad."
                )
            except Exception:
                await update.message.reply_text(
                    "❌ Send a positive number like `5` or `10` — or `0` to clear the override.",
                    parse_mode="HTML"
                )

    elif action == "float_pct":
        sess = _s(uid)
        slot_idx = sess.editing_slot
        ad_data = _ad_data_of(sess, slot_idx)
        token    = ad_data.get("tokenId","USDT").upper()
        currency = ad_data.get("currencyId","NGN").upper()
        ok, err = validate_float_pct(currency, token, text)
        if not ok:
            await update.message.reply_text(err, parse_mode="HTML")
            return
        s = _ad_settings(sess, slot_idx)
        s["float_pct"] = text
        if slot_idx == -1:
            slot_str = _get_user_slot_str(uid)
            sess.settings[f"float_pct_{slot_str}"] = text
        _save_settings(uid)
        _state["action"] = None
        hint = next_setup_hint(uid) if slot_idx == -1 else ""
        await reply_with_back(
            f"✅ <b>{_ad_slot_label(slot_idx)} float % saved!</b>\n\n<code>{text}%</code> for <code>{token}/{currency}</code>\n\n"
            f"_{hint}_"
        )

    elif action == "ngn_usdt_ref":
        try:
            val = float(text)
            if val <= 0: raise ValueError
            sess = _s(uid)
            sess.sync_shared_ref(text)   # applies to every ad slot at once
            if sess.editing_slot == -1:
                slot_str = _get_user_slot_str(uid)
                sess.settings[f"local_usdt_ref_{slot_str}"] = text
                _save_settings(uid)
            _scur = _ad_data_of(sess, sess.editing_slot).get("currencyId","NGN").upper()
            _state["action"] = None
            shared_note = " (applies to all your ads)" if _multi_ad(sess) else ""
            await reply_with_back(f"✅ <b>{_esc(_scur)}/USDT ref saved!{shared_note}</b>\n\n<code>{_esc(text)}</code>\n\n<i>{_esc(next_setup_hint(uid) if sess.editing_slot == -1 else '')}</i>")
        except Exception:
            await update.message.reply_text("❌ Send a number like `1580`", parse_mode="HTML")

    elif action == "top_range":
        try:
            val = int(text.strip())
            if val <= 0:
                raise ValueError
        except (TypeError, ValueError):
            await update.message.reply_text(
                "❌ Send a whole number, 1 or higher.\nExample: <code>2</code>",
                parse_mode="HTML"
            )
            return
        sess = _s(uid)
        slot_idx = sess.editing_slot
        s = _ad_settings(sess, slot_idx)
        s["top_range"] = val
        _save_settings(uid)   # persists to DB, so it survives redeploy + Auto Resume Agent
        _state["action"] = None
        _ordinal = {1: "leading (highest)", 2: "2nd-highest", 3: "3rd-highest"}.get(val, f"{val}th-highest")
        await reply_with_back(
            f"✅ <b>{_ad_slot_label(slot_idx)} Top Range saved!</b>\n\n"
            f"Now copying the <b>{_ordinal}</b> price within the Close Price Range band."
        )

    elif action == "merchant_watch":
        sess = _s(uid)
        slot_idx = sess.editing_slot
        s = _ad_settings(sess, slot_idx)
        cleared = text.strip().lower() in ("0", "clear", "none", "reset")
        if cleared:
            s["merchant_username"] = ""
            s["merchant_last_range"] = "1-300"
            _save_settings(uid)
            _state["action"] = None
            await reply_with_back(
                f"✅ <b>{_ad_slot_label(slot_idx)} Merchant Watch cleared</b> — "
                f"back to watching the whole market."
            )
        else:
            nick = text.strip()
            if not nick or len(nick) > 64:
                await update.message.reply_text(
                    "❌ Send a valid nickname (1-64 characters), or <code>clear</code> to remove it.",
                    parse_mode="HTML"
                )
                return
            s["merchant_username"] = nick
            s["merchant_last_range"] = "1-300"   # fresh merchant — don't carry over the old one's last-known range
            _save_settings(uid)   # persists to DB, so it survives redeploy + Auto Resume Agent
            _state["action"] = None
            await reply_with_back(
                f"✅ <b>{_ad_slot_label(slot_idx)} Merchant Watch saved!</b>\n\n"
                f"Now only tracking ad(s) from <code>{_esc(nick)}</code>, matched against your "
                f"Close Price Range and Top Range settings."
            )

    elif action == "close_price_range":
        digits = text.strip().replace(",", "").replace(".", "")
        if not digits.isdigit() or len(digits) < 3:
            await update.message.reply_text(
                "❌ Send digits only — at least 3 of them.\nExample: <code>1047435</code>",
                parse_mode="HTML"
            )
            return
        sess = _s(uid)
        slot_idx = sess.editing_slot
        s = _ad_settings(sess, slot_idx)
        s["close_price_range"] = digits
        _save_settings(uid)   # persists to DB, so it survives redeploy + Auto Resume Agent
        _state["action"] = None
        await reply_with_back(
            f"✅ <b>{_ad_slot_label(slot_idx)} close price range saved!</b>\n\n"
            f"Tracking prices starting with <code>{_esc(digits)}</code>\n\n"
            f"Each cycle the bot copies the highest BTC/NGN market ad in that band."
        )

    elif action == "interval":
        sess = _s(uid)
        slot_idx = sess.editing_slot
        s = _ad_settings(sess, slot_idx)
        ad_data = _ad_data_of(sess, slot_idx)

        _floor = _interval_floor_secs(s, ad_data)
        if _floor is not None:
            ok, err = validate_interval_seconds(text, floor=_floor)
            if not ok:
                await update.message.reply_text(err, parse_mode="HTML")
                return
            val = int(text)
            s["interval_secs"] = val
            # Keep the legacy minutes field roughly in sync so anything
            # still reading it (status text, older code paths) shows
            # something sane rather than a stale number.
            s["interval"] = max(1, round(val / 60))
            _save_settings(uid)
            _state["action"] = None
            await reply_with_back(
                f"✅ <b>{_ad_slot_label(slot_idx)} interval saved!</b>\n\n"
                f"Every <code>{_esc(_ad_interval_label(s, ad_data))}</code>"
            )
            return

        ok, err = validate_interval(text)
        if not ok:
            await update.message.reply_text(err, parse_mode="HTML")
            return
        val = int(text)
        s["interval"] = val
        s.pop("interval_secs", None)   # switching back to minutes clears any seconds override
        if slot_idx == -1:
            slot_str = _get_user_slot_str(uid)
            sess.settings[f"interval_{slot_str}"] = val
        _save_settings(uid)
        _state["action"] = None
        hint = next_setup_hint(uid) if slot_idx == -1 else ""
        await reply_with_back(f"✅ <b>{_ad_slot_label(slot_idx)} interval saved!</b>\n\nEvery <code>{_esc(str(val))}</code> min\n\n<i>{_esc(hint)}</i>")

    # ── ⬆️ Upgrade request — contact details ──
    elif action == "upgrade_contact":
        ok, contact, err = validate_contact(text)
        if not ok:
            await update.message.reply_text(err, parse_mode="HTML")
            return
        _state["action"] = None
        await update.message.reply_text(
            "⏳ <b>Upgrade Request Sent!</b>\n\n"
            "The admin has been notified and will review shortly.\n"
            "You will receive a message once approved.",
            parse_mode="HTML"
        )
        await _submit_upgrade_request(context.bot, uid, tuser.username or "", tuser.full_name or "", contact)


_notified_upgrade_ids: set = set()   # track which requests we already notified about

async def _upgrade_notifier_loop(bot):
    """Poll DB every 30 s for new upgrade requests and notify admins."""
    global _notified_upgrade_ids
    logger.info("[UpgradeNotifier] Background notifier started")
    while True:
        try:
            await asyncio.sleep(30)
            pending = db.get_pending_requests()
            for req in pending:
                uid_r  = req.get("user_id")
                if uid_r in _notified_upgrade_ids:
                    continue   # already notified
                uname_r = req.get("username", "")
                dname_r = req.get("display_name", "")
                contact_r = req.get("contact", "") or "— not provided —"
                msg = (
                    f"🔔 <b>New Upgrade Request!</b>\n\n"
                    f"👤 User ID: <code>{uid_r}</code>\n"
                    f"Username: @{uname_r}\n"
                    f"Name: {dname_r}\n"
                    f"📞 Contact: {contact_r}\n\n"
                    f"Approve: <code>/upgrade {uid_r} 30</code>"
                )
                notified = False
                for admin_id in list(_admin_chat_ids):
                    try:
                        await bot.send_message(
                            chat_id=admin_id, text=msg, parse_mode="HTML"
                        )
                        notified = True
                        logger.info(f"[UpgradeNotifier] Notified admin {admin_id} about uid={uid_r}")
                    except Exception as _e:
                        logger.warning(f"[UpgradeNotifier] Could not reach admin {admin_id}: {_e}")
                if notified:
                    _notified_upgrade_ids.add(uid_r)
                    # Clean up approved IDs no longer pending so re-requests work
                    current_pending_ids = {r.get("user_id") for r in pending}
                    _notified_upgrade_ids &= current_pending_ids
        except Exception as _loop_err:
            logger.error(f"[UpgradeNotifier] Loop error: {_loop_err}")












async def _plan_expiry_watchdog_loop(bot=None):
    """Enforce plan expiry WITHOUT waiting for the user to touch the bot.

    The problem this solves: is_pro() was always correct the instant a
    plan lapsed, but nothing ever asked it. Plan gating lived entirely on
    the button-press path, so a user whose plan expired at 19:10 kept
    editing ads, monitoring orders and auto-paying indefinitely — until
    they happened to tap something (or the process was redeployed). The
    Auto Resume Agent made that worse: it re-starts engines on its own
    schedule with no interaction at all, so an expired user's engines
    could be resurrected forever.

    So this runs on its own short cycle and, for every user whose Pro
    plan has lapsed, actively tears everything down and downgrades them.
    Checking every 60s means enforcement lands within a minute of expiry
    rather than whenever the user next shows up.
    """
    while True:
        await asyncio.sleep(60)
        try:
            for _sess in list(get_all_sessions()):
                uid = _sess.user_id
                try:
                    user = db.get_user(uid)
                    if not user or user.get("plan") != "pro":
                        continue          # already free (or unknown) — nothing to downgrade
                    if db.is_pro(uid):
                        continue          # still within the paid window

                    had_engines = _any_ad_running(_sess) or bool(_sess.auto_pay_enabled)

                    # Stop everything first, THEN downgrade — so there's no
                    # window where the plan says "free" but loops are still
                    # mid-cycle submitting edits on Bybit.
                    _sess.stop_all_tasks()
                    db.downgrade_user(uid)

                    logger.info(
                        f"[PlanWatchdog] User {uid} Pro plan expired — engines stopped "
                        f"(had_active={had_engines}) and account downgraded to free"
                    )

                    if bot:
                        try:
                            msg = (
                                "⏰ <b>Your Pro plan has expired</b>\n\n"
                                "Your account has been moved to the Free plan."
                            )
                            if had_engines:
                                msg += (
                                    "\n\nEverything that was running has been stopped:\n"
                                    "• Ad Price Bot / auto-update (Ad 1 – Ad 8)\n"
                                    "• Auto-Pay\n"
                                )
                            msg += "\n\nTap <b>⬆️ Upgrade Plan</b> to renew and turn them back on."
                            await bot.send_message(chat_id=uid, text=msg, parse_mode="HTML")
                        except Exception as e:
                            logger.debug(f"[PlanWatchdog] Could not notify user {uid}: {e}")
                except Exception as e:
                    logger.error(f"[PlanWatchdog] Failed processing user {uid}: {e}")
        except Exception as e:
            logger.error(f"[PlanWatchdog] Loop error: {e}")























# ─────────────────────────────────────────
# 📊 /userdata — Admin export (overrides admin_commands import)
# Includes total_buy_orders + total_sell_orders from DB and live session.
# ─────────────────────────────────────────
async def cmd_userdata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Download all user data as Excel. Admin only.

    Buy/sell totals:
      • DB value  — persisted by order monitor each time a new order is seen
      • Live session — get_session(uid).seen_order_ids / seen_sell_ids set sizes
      • Whichever is HIGHER wins, so totals are never under-reported.
    Totals reset naturally when a session clears — no permanent analytics added.
    """
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only.")
        return

    try:
        import io
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment
    except ImportError:
        await update.message.reply_text(
            "❌ <b>openpyxl not installed.</b>\n\nRun: <code>pip install openpyxl</code>",
            parse_mode="HTML"
        )
        return

    await update.message.reply_text("⏳ Building user data export...")

    try:
        users = db.get_all_users() or []

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Users"

        headers = [
            "User ID", "Username", "Display Name", "Plan", "Plan Expires",
            "Upgrade Pending", "Created At", "Last Active",
            "Total BUY Orders", "Total SELL Orders",
        ]
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill("solid", fgColor="1F4E79")

        for col, h in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=h)
            cell.font      = header_font
            cell.fill      = header_fill
            cell.alignment = Alignment(horizontal="center")

        for row_idx, user in enumerate(users, 2):
            uid = int(user.get("user_id") or user.get("id") or 0)

            # ── DB totals (persisted cumulatively by order monitor) ──
            db_buy  = int(user.get("total_buy_orders",  0) or 0)
            db_sell = int(user.get("total_sell_orders", 0) or 0)

            # ── Live session totals (in-memory, current session only) ──
            # get_session(uid) is safe to call for any uid — returns empty session
            # if the user has no active session (sets will be empty → 0 counts).
            try:
                sess      = get_session(uid)
                live_buy  = len(getattr(sess, "seen_order_ids", None) or set())
                live_sell = len(getattr(sess, "seen_sell_ids",  None) or set())
            except Exception:
                live_buy  = 0
                live_sell = 0

            # Take whichever is higher — DB may lag if session hasn't flushed yet,
            # live session resets to 0 after cleanup, so max() covers both cases.
            total_buy  = max(db_buy,  live_buy)
            total_sell = max(db_sell, live_sell)

            row_data = [
                uid,
                user.get("username",        ""),
                user.get("display_name",    "") or user.get("full_name", ""),
                user.get("plan",            "free"),
                user.get("plan_expires",    "") or user.get("plan_expiry", ""),
                user.get("upgrade_pending", False),
                user.get("created_at",      ""),
                user.get("stats", {}).get("last_active", "") or user.get("last_active", "") or user.get("last_seen", ""),
                total_buy,
                total_sell,
            ]
            for col, val in enumerate(row_data, 1):
                ws.cell(row=row_idx, column=col, value=val)

        # Auto-width columns
        for col in ws.columns:
            max_len = max((len(str(c.value or "")) for c in col), default=10)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 40)

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)

        from datetime import datetime as _dt
        fname = f"userdata_{_dt.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        await update.message.reply_document(
            document=buf,
            filename=fname,
            caption=(
                f"📊 <b>User Data Export</b>\n\n"
                f"👥 {len(users)} users\n"
                f"🕐 Generated: <code>{_dt.now().strftime('%Y-%m-%d %H:%M:%S')}</code>\n\n"
                f"BUY/SELL totals: DB cumulative + live session (max of both)."
            ),
            parse_mode="HTML"
        )

    except Exception as _ude:
        import traceback
        logger.error(f"[userdata] Export error: {_ude}\n{traceback.format_exc()}")
        await update.message.reply_text(
            f"❌ <b>Export failed</b>\n\n<code>{_esc(str(_ude)[:300])}</code>",
            parse_mode="HTML"
        )


def start_bot():
    application = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .updater(None)
        .build()
    )
    # ── User commands ──
    application.add_handler(CommandHandler("start",            start))
    application.add_handler(CommandHandler("menu",             menu_command))
    application.add_handler(CommandHandler("upgrade",       cmd_upgrade))
    application.add_handler(CommandHandler("downgrade",     cmd_downgrade))
    application.add_handler(CommandHandler("requests",      cmd_requests))
    application.add_handler(CommandHandler("listusers",     cmd_listusers))
    application.add_handler(CommandHandler("userdata",      cmd_userdata))

    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    # ── Global error handler — logs ALL unhandled exceptions with full traceback ──
    async def _global_error_handler(update, context):
        import traceback
        tb = "".join(traceback.format_exception(type(context.error), context.error, context.error.__traceback__))
        logger.error(
            f"[GlobalError] Unhandled exception\n"
            f"  update={update}\n"
            f"  error={context.error}\n"
            f"{tb}"
        )
        # Optionally notify the user something went wrong
        try:
            if update and update.effective_message:
                await update.effective_message.reply_text(
                    "⚠️ An unexpected error occurred. Please try again or use /menu to restart."
                )
        except Exception:
            pass

    application.add_error_handler(_global_error_handler)

    async def _post_init(app):
        # Auto-Pay and the ad price bots are started by the user and run until
        # the user switches them off — no scheduled resets, no auto-resume.
        asyncio.create_task(_plan_expiry_watchdog_loop(app.bot))
        asyncio.create_task(_upgrade_notifier_loop(app.bot))
        asyncio.create_task(start_market_collector())
        asyncio.create_task(start_direct_market_collector())

        from telegram import BotCommand, BotCommandScopeChat
        admin_commands = [
            BotCommand("upgrade",   "Upgrade a user to Pro"),
            BotCommand("downgrade", "Downgrade a user"),
            BotCommand("requests",  "List upgrade requests"),
            BotCommand("listusers", "List all users"),
            BotCommand("userdata",  "Download user data Excel"),
        ]
        user_commands = [
            BotCommand("start", "Start the bot"),
            BotCommand("menu",  "Open main menu"),
        ]
        try:
            await app.bot.set_my_commands(user_commands)
        except Exception as _e:
            logger.warning(f"[Init] Could not set default commands: {_e}")
        for _admin_id in list(ADMIN_IDS):
            try:
                await app.bot.set_my_commands(
                    user_commands + admin_commands,
                    scope=BotCommandScopeChat(chat_id=_admin_id)
                )
                logger.info(f"[Init] Admin commands set for {_admin_id}")
            except Exception as _e:
                logger.warning(f"[Init] Could not set admin commands for {_admin_id}: {_e}")

        logger.info("🟡 Plan watchdog + upgrade notifier + market collectors started")

    application.post_init = _post_init
    logger.info("🤖 Bot handlers registered")
    return application
