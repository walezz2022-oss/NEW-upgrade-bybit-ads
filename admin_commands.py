"""
admin_commands.py — Admin-only Telegram command handlers.

Commands:
  /upgrade <user_id> <days>       — upgrade a user to pro
  /downgrade <user_id>            — downgrade user to free
  /requests                       — list pending upgrade requests
  /userdata                       — download all user data as Excel
  /listusers                      — list all users with plan status


NOTE on /userdata:
  cmd_userdata here is a thin stub that defers to the version defined in bot.py.
  bot.py defines a full cmd_userdata that:
    • builds the Excel directly (bypasses db.export_users_to_excel)
    • reads total_buy_orders / total_sell_orders from DB
    • merges live session counts via get_session(uid) per user
    • takes max(db_total, live_total) so totals are never under-reported
  The bot.py version is registered last in start_bot(), so it wins.

NOTE on formatting:
  All messages here use parse_mode="HTML" with every dynamic field passed
  through esc(). Telegram usernames/display names can legally contain
  underscores and other Markdown special characters — using legacy
  "Markdown" parse mode with unescaped text throws
  `Can't parse entities: can't find end of the entity` and crashes the
  handler. HTML mode + escaping avoids that entirely.
"""

import html
import logging
import io
from datetime import datetime
from telegram import Update
from telegram.ext import ContextTypes
import db
from config import ADMIN_IDS

logger = logging.getLogger(__name__)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def esc(value) -> str:
    """HTML-escape any user-supplied text before embedding it in an
    HTML-parse-mode Telegram message. Use this on every username,
    display name, or other free-text field pulled from the DB."""
    return html.escape(str(value), quote=False)




# ─────────────────────────────────────────
# /upgrade <user_id> <days>
# ─────────────────────────────────────────
async def cmd_upgrade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: <code>/upgrade &lt;user_id&gt; &lt;days&gt;</code>\n\nExample: <code>/upgrade 123456789 30</code>",
            parse_mode="HTML"
        )
        return
    try:
        target_id = int(args[0])
        days      = int(args[1])
        if days < 1:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Invalid arguments. Usage: <code>/upgrade 123456789 30</code>", parse_mode="HTML")
        return

    user = db.get_user(target_id)
    if not user:
        await update.message.reply_text(f"❌ User <code>{target_id}</code> not found in database.", parse_mode="HTML")
        return

    updated = db.upgrade_user(target_id, days)
    db.remove_upgrade_request(target_id)

    ref_line = ""

    try:
        exp_str = updated.get("plan_expires", "")
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                f"🎉 <b>Your upgrade has been approved!</b>\n\n"
                f"💎 Plan: <b>Pro</b>\n"
                f"⏰ Expires: <code>{esc(exp_str)}</code>\n\n"
                f"You now have full access to all bot features.\n"
                f"Tap /menu to see your updated profile!"
            ),
            parse_mode="HTML"
        )
        notified = "✅ User notified"
    except Exception as e:
        notified = f"⚠️ Could not notify user: {esc(e)}"

    await update.message.reply_text(
        f"✅ <b>User upgraded!</b>\n\n"
        f"User ID: <code>{target_id}</code>\n"
        f"Username: @{esc(user.get('username','?'))}\n"
        f"Plan: Pro\n"
        f"Expires: <code>{esc(updated.get('plan_expires',''))}</code>\n\n"
        f"{notified}"
        f"{ref_line}",
        parse_mode="HTML"
    )
    logger.info(f"[Admin] Upgraded user {target_id} for {days} days by admin {update.effective_user.id}")


# ─────────────────────────────────────────
# /downgrade <user_id>
# ─────────────────────────────────────────
async def cmd_downgrade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    args = context.args
    if not args:
        await update.message.reply_text("Usage: <code>/downgrade &lt;user_id&gt;</code>", parse_mode="HTML")
        return
    try:
        target_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.", parse_mode="HTML")
        return

    user = db.get_user(target_id)
    if not user:
        await update.message.reply_text(f"❌ User <code>{target_id}</code> not found.", parse_mode="HTML")
        return

    db.downgrade_user(target_id)

    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                "⚠️ <b>Your Pro plan has ended.</b>\n\n"
                "You have been moved to the Free plan.\n"
                "Contact the admin to renew your subscription."
            ),
            parse_mode="HTML"
        )
    except Exception:
        pass

    await update.message.reply_text(
        f"✅ User <code>{target_id}</code> (@{esc(user.get('username','?'))}) downgraded to Free.",
        parse_mode="HTML"
    )


# ─────────────────────────────────────────
# /requests — list pending upgrade requests
# ─────────────────────────────────────────
async def cmd_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    pending = db.get_pending_requests()
    if not pending:
        await update.message.reply_text("📋 No pending upgrade requests.")
        return

    lines = [f"📋 <b>Pending Upgrade Requests ({len(pending)}):</b>\n"]
    for req in pending:
        uid   = req.get("user_id", "?")
        uname = esc(req.get("username", "?"))
        dname = esc(req.get("display_name", "?"))
        reqat = esc(req.get("requested_at", "?"))
        contact = esc(req.get("contact", "")) or "— not provided —"
        ref_line = ""
        lines.append(
            f"👤 <code>{uid}</code> — @{uname} ({dname})\n"
            f"   📅 Requested: {reqat}\n"
            f"   📞 Contact: {contact}\n"
            f"{ref_line}"
            f"   ✅ Approve: <code>/upgrade {uid} 30</code>\n"
        )

    msg = "\n".join(lines)
    if len(msg) > 4000:
        msg = msg[:4000] + "\n...(truncated)"
    await update.message.reply_text(msg, parse_mode="HTML")


# ─────────────────────────────────────────
# /listusers — list all users
# ─────────────────────────────────────────
async def cmd_listusers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    users = db.get_all_users()
    if not users:
        await update.message.reply_text("No users registered yet.")
        return

    lines = [f"👥 <b>All Users ({len(users)}):</b>\n"]
    for u in sorted(users, key=lambda x: x.get("created_at",""), reverse=True):
        uid   = u.get("user_id","?")
        uname = esc(u.get("username","?"))
        plan  = esc(u.get("plan","free").upper())
        exp   = esc(u.get("plan_expires","") or "—")
        pend  = " ⏳" if u.get("upgrade_pending") else ""
        icon  = "💎" if plan == "PRO" else "⚪"
        lines.append(f"{icon} <code>{uid}</code> @{uname} — {plan}{pend} | exp: {exp}")

    msg = "\n".join(lines)
    if len(msg) > 4000:
        msg = msg[:4000] + "\n...(truncated)"
    await update.message.reply_text(msg, parse_mode="HTML")


# ─────────────────────────────────────────
# /userdata — download Excel
# ─────────────────────────────────────────
# NOTE: The full implementation lives in bot.py as a local override.
# bot.py defines cmd_userdata after importing this module, so the
# bot.py version is what gets registered with CommandHandler("userdata", cmd_userdata).
# This stub exists only so the import in bot.py does not fail.
async def cmd_userdata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stub — overridden by bot.py's local cmd_userdata definition."""
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text("⏳ Generating Excel report...")
    try:
        data = db.export_users_to_excel()
        if not data:
            await update.message.reply_text(
                "❌ Failed to generate Excel.\n\nMake sure `openpyxl` is in requirements.txt"
            )
            return
        filename = f"users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        await update.message.reply_document(
            document=io.BytesIO(data),
            filename=filename,
            caption=f"📊 User data export — {len(db.get_all_users())} users"
        )
    except Exception as e:
        logger.error(f"[Admin] userdata export error: {e}")
        await update.message.reply_text(f"❌ Export failed: {esc(e)}", parse_mode="HTML")
