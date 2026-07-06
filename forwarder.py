#!/usr/bin/env python3
"""
Telegram Broadcast Tool (terminal)
==================================

A terminal utility for managing your own Telegram accounts and posting ads
from your "Saved Messages" to destination chats that YOU pick manually
(groups/channels you own or administer). It also includes a live listener
for saving ads, capturing mentions, and auto-replying to DMs.

Design principles:
  * You explicitly select every destination. There is no "send to all groups".
  * Rate limits are RESPECTED, not evaded. On a FloodWaitError we wait exactly
    as long as Telegram asks; inter-message delays are a courtesy pause.
  * The DM auto-reply sends ONE message per contact (with a cooldown), never a
    burst. Flooding someone with repeated messages is spam and gets accounts
    banned, so it is intentionally not supported.
  * Sessions and credentials stay local (./sessions and ./config.json).

Run with:  python3 forwarder.py
"""

import os
import sys
import json
import asyncio
import random
import time
from datetime import datetime

# ---- Third party ------------------------------------------------------------
try:
    import questionary
    from questionary import Choice
    from colorama import init as colorama_init
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import (
        Progress,
        SpinnerColumn,
        BarColumn,
        TextColumn,
        TimeElapsedColumn,
        MofNCompleteColumn,
    )
    from telethon import TelegramClient, events
    from telethon.errors import (
        FloodWaitError,
        SessionPasswordNeededError,
        PhoneCodeInvalidError,
        PhoneCodeExpiredError,
        PhoneNumberInvalidError,
        ChatWriteForbiddenError,
    )
    from telethon.tl.types import Channel, MessageMediaWebPage
except ImportError as exc:  # pragma: no cover
    print("Missing dependency:", exc)
    print("Install with:  pip install telethon rich questionary colorama")
    sys.exit(1)

# -----------------------------------------------------------------------------
colorama_init(autoreset=True)
console = Console()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")
SESSIONS_DIR = os.path.join(BASE_DIR, "sessions")
CAPTURES_FILE = os.path.join(BASE_DIR, "captured_mentions.txt")

DEFAULT_SETTINGS = {
    "rounds": 1,                    # how many times to repeat the whole run
    "delay_between_messages": 3,    # courtesy pause between individual sends (s)
    "delay_between_rounds": 30,     # pause between repeat rounds (s)
    "jitter": True,                 # add small random jitter to the courtesy pause
    "autoreply_cooldown": 3600,     # min seconds between auto-replies to same user
    "dm_alert_count": 10,           # self-alert messages sent to YOUR own Saved Messages
    "dm_alert_delay": 2,            # seconds between self-alert messages (flood-safe)
    "dm_alert_cooldown": 60,        # min seconds between alert bursts for same sender
}

DEFAULT_TEXTS = {
    # Sent once to anyone who DMs the account (editable). Covers "away" + promo.
    "dm_autoreply": "Hey! I'm not at my desk right now. Meanwhile, check out @intraxio.",
    # Self-alert pushed to YOUR own Saved Messages when someone DMs, so the
    # notifications ping you and you get online fast. {who} = the sender.
    "dm_alert": "{who} dmed you — check it out.",
    # Confirmation shown in Saved Messages when an ad is saved via /savead.
    "savead_confirm": "Ad saved.",
}

DEFAULT_FEATURES = {
    "savead_enabled": True,          # watch Saved Messages for /savead
    "capture_mentions_enabled": True,  # save messages that tag the account
    "dm_autoreply_enabled": True,    # auto-reply once to incoming DMs
    "dm_alert_enabled": True,        # self-alert (to your own Saved Messages) on a DM
}

os.makedirs(SESSIONS_DIR, exist_ok=True)


# -----------------------------------------------------------------------------
# Colored console helpers
# -----------------------------------------------------------------------------
def ok(msg):
    console.print(f"[bold green]OK[/bold green]  {msg}")


def err(msg):
    console.print(f"[bold red]ERR[/bold red] {msg}")


def warn(msg):
    console.print(f"[bold yellow]!![/bold yellow]  {msg}")


def info(msg):
    console.print(f"[bold cyan]i[/bold cyan]   {msg}")


def banner():
    art = r"""
   _______     _
  |__   __|   | |
     | | ___  | | ___  __ _ _ __ __ _ _ __ ___
     | |/ _ \ | |/ _ \/ _` | '__/ _` | '_ ` _ \
     | |  __/ | |  __/ (_| | | | (_| | | | | | |
     |_|\___| |_|\___|\__, |_|  \__,_|_| |_| |_|
                       __/ |
                      |___/   Broadcast Tool
"""
    console.print(f"[bold cyan]{art}[/bold cyan]")
    console.print(
        "[dim]Post your saved ads to chats you pick manually. "
        "Respects Telegram rate limits.[/dim]\n"
    )


# -----------------------------------------------------------------------------
# Config persistence
# -----------------------------------------------------------------------------
def _blank_config():
    return {
        "accounts": {},
        "ads": [],
        "settings": dict(DEFAULT_SETTINGS),
        "texts": dict(DEFAULT_TEXTS),
        "features": dict(DEFAULT_FEATURES),
    }


def load_config():
    if not os.path.exists(CONFIG_FILE):
        return _blank_config()
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError) as e:
        warn(f"Could not read config ({e}); starting fresh.")
        return _blank_config()

    data.setdefault("accounts", {})
    data.setdefault("ads", [])

    settings = dict(DEFAULT_SETTINGS)
    settings.update(data.get("settings", {}))
    data["settings"] = settings

    texts = dict(DEFAULT_TEXTS)
    texts.update(data.get("texts", {}))
    data["texts"] = texts

    features = dict(DEFAULT_FEATURES)
    features.update(data.get("features", {}))
    data["features"] = features
    return data


def save_config(config):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as fh:
            json.dump(config, fh, indent=2)
    except OSError as e:
        err(f"Failed to save config: {e}")


def session_path_for(phone):
    safe = phone.replace("+", "").replace(" ", "")
    return os.path.join(SESSIONS_DIR, f"session_{safe}")


def build_client(account):
    return TelegramClient(
        session_path_for(account["phone"]),
        int(account["api_id"]),
        account["api_hash"],
    )


# -----------------------------------------------------------------------------
# Persistent client pool — accounts stay connected & listening for the whole
# session so /savead, mention capture and DM handling work while you use the
# menu. Telethon processes updates in the background on the running event loop.
# -----------------------------------------------------------------------------
CLIENTS = {}                                  # phone -> connected TelegramClient
LISTEN_STATE = {"reply": {}, "alert": {}}     # per-user cooldown tracking


async def start_account(phone, account, config):
    """Connect one account, register listener handlers, keep it live."""
    if phone in CLIENTS and CLIENTS[phone].is_connected():
        return CLIENTS[phone]
    client = build_client(account)
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        return None
    me = await client.get_me()
    register_handlers(client, config, me.id, LISTEN_STATE["reply"], LISTEN_STATE["alert"])
    CLIENTS[phone] = client
    return client


async def start_all_accounts(config):
    """Bring every saved account online and listening."""
    accounts = config["accounts"]
    if not accounts:
        warn("No accounts yet. Use 'Add Account' — it will start listening immediately.")
        return
    f = config["features"]
    info("Bringing accounts online...")
    live = 0
    for phone, acc in accounts.items():
        try:
            client = await start_account(phone, acc, config)
            if client is None:
                warn(f"{phone}: not logged in — re-add via 'Add Account'.")
                continue
            me = await client.get_me()
            ok(f"Listening on {phone} (@{me.username or me.first_name}).")
            live += 1
        except Exception as e:
            err(f"Could not start {phone}: {e}")
    if live:
        console.print(Panel(
            f"[green]{live}[/green] account(s) online and listening for:\n"
            f"  /savead in Saved Messages: {'on' if f['savead_enabled'] else 'off'}\n"
            f"  mention capture:           {'on' if f['capture_mentions_enabled'] else 'off'}\n"
            f"  DM auto-reply (1x):        {'on' if f['dm_autoreply_enabled'] else 'off'}\n"
            f"  DM self-alert (x{config['settings']['dm_alert_count']}):        "
            f"{'on' if f['dm_alert_enabled'] else 'off'}",
            title="Listener active", style="green"))


async def stop_all_accounts():
    for client in list(CLIENTS.values()):
        try:
            await client.disconnect()
        except Exception:
            pass
    CLIENTS.clear()


async def ask_int(prompt, current, lo, hi):
    raw = await questionary.text(f"{prompt} [{current}]:").ask_async()
    if raw is None or raw.strip() == "":
        return current
    try:
        val = int(raw.strip())
    except ValueError:
        warn("Not a number; keeping current value.")
        return current
    if not (lo <= val <= hi):
        warn(f"Must be between {lo} and {hi}; keeping current value.")
        return current
    return val


# -----------------------------------------------------------------------------
# 1) Add account (interactive login)
# -----------------------------------------------------------------------------
async def add_account(config):
    console.print(Panel("Add Telegram Account", style="cyan"))
    info("Get your API ID / API Hash from https://my.telegram.org -> API development tools")

    api_id = await questionary.text("API ID:").ask_async()
    api_hash = await questionary.text("API Hash:").ask_async()
    phone = await questionary.text("Phone number (with country code, e.g. +15551234567):").ask_async()

    if not (api_id and api_hash and phone):
        warn("Cancelled — missing input.")
        return
    api_id, api_hash, phone = api_id.strip(), api_hash.strip(), phone.strip()

    if not api_id.isdigit():
        err("API ID must be numeric.")
        return
    if phone in config["accounts"]:
        warn("That phone number is already added.")
        return

    account = {"api_id": api_id, "api_hash": api_hash, "phone": phone}
    client = build_client(account)

    ok_login = False
    try:
        await client.connect()
        if await client.is_user_authorized():
            ok(f"{phone} is already authorized.")
        else:
            await client.send_code_request(phone)
            info("A login code was sent to your Telegram app.")
            code = await questionary.text("Enter the code:").ask_async()
            try:
                await client.sign_in(phone, code.strip())
            except SessionPasswordNeededError:
                info("Two-step verification is enabled on this account.")
                password = await questionary.password("Enter your 2FA password:").ask_async()
                await client.sign_in(password=password)

        me = await client.get_me()
        display = f"@{me.username}" if me.username else (me.first_name or phone)
        config["accounts"][phone] = account
        save_config(config)
        ok(f"Logged in as {display}. Session saved.")
        ok_login = True
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
        err("The login code was invalid or expired. Try again.")
    except PhoneNumberInvalidError:
        err("That phone number is not valid.")
    except FloodWaitError as e:
        err(f"Telegram asked us to wait {e.seconds}s before trying again.")
    except Exception as e:
        err(f"Login failed: {e}")

    if ok_login:
        # Keep this account connected and start listening immediately.
        me = await client.get_me()
        register_handlers(client, config, me.id,
                          LISTEN_STATE["reply"], LISTEN_STATE["alert"])
        CLIENTS[phone] = client
        ok(f"{phone} is now online and listening (/savead, DMs, mentions).")
    else:
        await client.disconnect()


# -----------------------------------------------------------------------------
# 2) List accounts
# -----------------------------------------------------------------------------
async def list_accounts(config):
    accounts = config["accounts"]
    if not accounts:
        warn("No accounts added yet. Use 'Add Account'.")
        return

    table = Table(title="Hosted Accounts", header_style="bold cyan")
    table.add_column("#", justify="right")
    table.add_column("Phone")
    table.add_column("API ID")
    table.add_column("Session")
    table.add_column("Status")

    for i, (phone, acc) in enumerate(accounts.items(), 1):
        status = "[red]not connected[/red]"
        client = CLIENTS.get(phone)
        if client is not None and client.is_connected():
            try:
                me = await client.get_me()
                who = f"@{me.username}" if me.username else (me.first_name or "authorized")
                status = f"[green]listening: {who}[/green]"
            except Exception:
                status = "[yellow]connected[/yellow]"

        session_exists = os.path.exists(session_path_for(phone) + ".session")
        table.add_row(
            str(i), phone, acc["api_id"],
            "[green]yes[/green]" if session_exists else "[red]no[/red]",
            status,
        )
    console.print(table)


# -----------------------------------------------------------------------------
# Remove account
# -----------------------------------------------------------------------------
async def remove_account(config):
    accounts = config["accounts"]
    if not accounts:
        warn("No accounts to remove.")
        return

    phone = await questionary.select(
        "Select an account to remove:",
        choices=list(accounts.keys()) + [Choice("Cancel", value=None)],
    ).ask_async()
    if not phone:
        return

    if not await questionary.confirm(
        f"Remove {phone} and delete its local session file?", default=False
    ).ask_async():
        return

    # Disconnect the live client first so the session file isn't locked.
    client = CLIENTS.pop(phone, None)
    if client is not None:
        try:
            await client.disconnect()
        except Exception:
            pass

    session_file = session_path_for(phone) + ".session"
    try:
        if os.path.exists(session_file):
            os.remove(session_file)
    except OSError as e:
        warn(f"Could not delete session file: {e}")

    del accounts[phone]
    save_config(config)
    ok(f"Removed {phone}.")


# -----------------------------------------------------------------------------
# Ad store management
# -----------------------------------------------------------------------------
def _ad_preview(ad):
    if ad["kind"] == "text":
        return (ad["content"][:60] + "…") if len(ad["content"]) > 60 else ad["content"]
    return ad.get("preview") or f"(saved message #{ad.get('message_id')})"


async def manage_ads(config):
    while True:
        ads = config["ads"]
        console.print(Panel("Manage Ads", style="cyan"))
        if ads:
            table = Table(header_style="bold cyan")
            table.add_column("#", justify="right")
            table.add_column("Kind")
            table.add_column("Preview")
            for i, ad in enumerate(ads, 1):
                table.add_row(str(i), ad["kind"], _ad_preview(ad))
            console.print(table)
        else:
            info("No ads saved yet. Use /savead in Saved Messages, or add one here.")

        action = await questionary.select(
            "Ad actions:",
            choices=[
                Choice("Add a text ad", value="add"),
                Choice("Edit a text ad", value="edit"),
                Choice("Remove an ad", value="remove"),
                Choice("Back", value="back"),
            ],
        ).ask_async()

        if action in (None, "back"):
            return
        if action == "add":
            content = await questionary.text("Ad text:").ask_async()
            if content and content.strip():
                ads.append({"kind": "text", "content": content.strip(),
                            "added": datetime.now().isoformat()})
                save_config(config)
                ok("Ad added.")
        elif action == "edit":
            text_ads = [(i, a) for i, a in enumerate(ads) if a["kind"] == "text"]
            if not text_ads:
                warn("No editable text ads.")
                continue
            idx = await questionary.select(
                "Which ad?",
                choices=[Choice(_ad_preview(a), value=i) for i, a in text_ads]
                + [Choice("Cancel", value=None)],
            ).ask_async()
            if idx is None:
                continue
            new = await questionary.text("New text:", default=ads[idx]["content"]).ask_async()
            if new and new.strip():
                ads[idx]["content"] = new.strip()
                save_config(config)
                ok("Ad updated.")
        elif action == "remove":
            if not ads:
                continue
            idx = await questionary.select(
                "Remove which ad?",
                choices=[Choice(_ad_preview(a), value=i) for i, a in enumerate(ads)]
                + [Choice("Cancel", value=None)],
            ).ask_async()
            if idx is None:
                continue
            ads.pop(idx)
            save_config(config)
            ok("Ad removed.")


# -----------------------------------------------------------------------------
# Settings (delays, rounds, texts, feature toggles)
# -----------------------------------------------------------------------------
async def edit_settings(config):
    while True:
        console.print(Panel("Settings", style="cyan"))
        section = await questionary.select(
            "What do you want to change?",
            choices=[
                Choice("Timing (rounds / delays)", value="timing"),
                Choice("Edit texts (DM auto-reply, etc.)", value="texts"),
                Choice("Toggle listener features", value="features"),
                Choice("Back", value="back"),
            ],
        ).ask_async()

        if section in (None, "back"):
            return

        if section == "timing":
            s = config["settings"]
            info("Rounds: how many times to repeat the whole broadcast.")
            s["rounds"] = await ask_int("Rounds", s["rounds"], 1, 100)
            info("Delay between messages: courtesy pause after each send (helps avoid rate limits).")
            s["delay_between_messages"] = await ask_int(
                "Delay between messages (seconds)", s["delay_between_messages"], 0, 3600)
            info("Delay between rounds: pause before repeating the whole broadcast.")
            s["delay_between_rounds"] = await ask_int(
                "Delay between rounds (seconds)", s["delay_between_rounds"], 0, 86400)
            s["jitter"] = await questionary.confirm(
                "Add small random jitter to message delay?", default=s["jitter"]).ask_async()
            info("Auto-reply cooldown: minimum time before replying to the same person again.")
            s["autoreply_cooldown"] = await ask_int(
                "Auto-reply cooldown (seconds)", s["autoreply_cooldown"], 0, 604800)
            info("DM self-alert count: how many notifications to push to YOUR OWN Saved "
                 "Messages when someone DMs, so the pings get you online.")
            s["dm_alert_count"] = await ask_int("DM self-alert count", s["dm_alert_count"], 1, 50)
            info("DM self-alert delay: seconds between those self-notifications (2+ is flood-safe).")
            s["dm_alert_delay"] = await ask_int("DM self-alert delay (seconds)", s["dm_alert_delay"], 0, 60)
            info("DM self-alert cooldown: min gap before alerting again for the same sender.")
            s["dm_alert_cooldown"] = await ask_int("DM self-alert cooldown (seconds)", s["dm_alert_cooldown"], 0, 86400)
            save_config(config)
            ok("Timing saved.")

        elif section == "texts":
            t = config["texts"]
            key = await questionary.select(
                "Which text?",
                choices=[
                    Choice("DM auto-reply message", value="dm_autoreply"),
                    Choice("DM self-alert message ({who} = sender)", value="dm_alert"),
                    Choice("Saved-ad confirmation", value="savead_confirm"),
                    Choice("Back", value=None),
                ],
            ).ask_async()
            if key is None:
                continue
            new = await questionary.text("New text:", default=t[key]).ask_async()
            if new is not None:
                t[key] = new
                save_config(config)
                ok("Text updated.")

        elif section == "features":
            f = config["features"]
            f["savead_enabled"] = await questionary.confirm(
                "Watch Saved Messages for /savead?", default=f["savead_enabled"]).ask_async()
            f["capture_mentions_enabled"] = await questionary.confirm(
                "Save messages that tag the account (in groups)?",
                default=f["capture_mentions_enabled"]).ask_async()
            f["dm_autoreply_enabled"] = await questionary.confirm(
                "Auto-reply once to incoming DMs?", default=f["dm_autoreply_enabled"]).ask_async()
            f["dm_alert_enabled"] = await questionary.confirm(
                "Push self-alerts to your own Saved Messages on a DM?",
                default=f["dm_alert_enabled"]).ask_async()
            save_config(config)
            ok("Features saved.")


# -----------------------------------------------------------------------------
# Forwarding (copy-paste, to manually selected chats)
# -----------------------------------------------------------------------------
async def fetch_writable_chats(client):
    chats = []
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        is_group = getattr(dialog, "is_group", False)
        is_channel = getattr(dialog, "is_channel", False)
        if not (is_group or is_channel):
            continue

        can_post = True
        if isinstance(entity, Channel):
            if entity.broadcast and not (entity.creator or getattr(entity, "admin_rights", None)):
                can_post = False
        if getattr(entity, "left", False) or getattr(entity, "kicked", False):
            can_post = False

        chats.append({
            "id": dialog.id,
            "title": dialog.name or str(dialog.id),
            "can_post": can_post,
            "kind": "channel" if (is_channel and not is_group) else "group",
        })
    return chats


async def send_ad_copy(client, chat_id, ad):
    """Send an ad as a fresh message (copy-paste), NOT a forward."""
    if ad["kind"] == "text":
        await client.send_message(chat_id, ad["content"])
        return

    # msgref: re-fetch the saved message and copy its content + media
    msg = await client.get_messages("me", ids=ad["message_id"])
    if msg is None:
        raise ValueError("saved message no longer exists")

    has_media = msg.media is not None and not isinstance(msg.media, MessageMediaWebPage)
    if has_media:
        await client.send_file(
            chat_id, msg.media,
            caption=(msg.message or None),
            formatting_entities=(msg.entities or None),
        )
    elif msg.message:
        await client.send_message(
            chat_id, msg.message, formatting_entities=(msg.entities or None))
    else:
        raise ValueError("saved message has no content to copy")


async def forward_messages(config):
    accounts = config["accounts"]
    if not accounts:
        warn("No accounts added yet. Use 'Add Account'.")
        return
    if not config["ads"]:
        warn("No ads saved yet. Add one first:")
        console.print("   • In your Saved Messages, type [cyan]/savead your ad text[/cyan]")
        console.print("   • Or reply to a message there with [cyan]/savead[/cyan] (keeps media)")
        console.print("   • Or use menu option [cyan]3. Manage Ads[/cyan] -> Add a text ad")
        return

    phone = await questionary.select(
        "Post from which account?",
        choices=list(accounts.keys()) + [Choice("Cancel", value=None)],
    ).ask_async()
    if not phone:
        return

    # Reuse the already-connected, listening client (do NOT open a second one).
    client = CLIENTS.get(phone)
    if client is None or not client.is_connected():
        err(f"{phone} is not online. It should connect at startup — try re-adding it.")
        return
    if not await client.is_user_authorized():
        err("This account is not logged in. Re-add it via 'Add Account'.")
        return

    try:
        # --- choose which ads to send ---
        ad_choice = await questionary.checkbox(
            "Select the ads to post (space to toggle, enter to confirm):",
            choices=[Choice(_ad_preview(a), value=i, checked=True)
                     for i, a in enumerate(config["ads"])],
        ).ask_async()
        if not ad_choice:
            warn("No ads selected. Aborting.")
            return
        selected_ads = [config["ads"][i] for i in ad_choice]

        # --- choose destinations (manual, multi-select) ---
        info("Loading your groups and channels...")
        chats = await fetch_writable_chats(client)
        if not chats:
            warn("No groups or channels found on this account.")
            return

        choices = []
        for c in chats:
            label = f"{c['title']}  [{c['kind']}]"
            if not c["can_post"]:
                label += "  (no post rights)"
            choices.append(Choice(title=label, value=c["id"],
                                  disabled=None if c["can_post"] else "cannot post"))

        selected_ids = await questionary.checkbox(
            "Select the destination chats (space to toggle, enter to confirm):",
            choices=choices,
        ).ask_async()
        if not selected_ids:
            warn("No destinations selected. Aborting.")
            return

        # --- ask timing interactively with explanations ---
        s = config["settings"]
        console.print(Panel("Timing for this run", style="yellow"))
        info("Rounds: how many times to repeat the entire broadcast. Use 1 for a single pass.")
        rounds = await ask_int("Rounds", s["rounds"], 1, 100)
        info("Delay between messages: seconds to wait after each send. A few seconds "
             "keeps you well under Telegram's limits and looks natural.")
        base_delay = await ask_int("Delay between messages (seconds)", s["delay_between_messages"], 0, 3600)
        round_delay = 0
        if rounds > 1:
            info("Delay between rounds: seconds to wait before repeating the whole broadcast.")
            round_delay = await ask_int("Delay between rounds (seconds)", s["delay_between_rounds"], 0, 86400)

        id_to_title = {c["id"]: c["title"] for c in chats}
        total = len(selected_ids) * len(selected_ads) * rounds

        console.print(Panel(
            f"Account: [cyan]{phone}[/cyan]\n"
            f"Ads: [cyan]{len(selected_ads)}[/cyan]\n"
            f"Destinations: [cyan]{len(selected_ids)}[/cyan] chats you selected\n"
            f"Rounds: [cyan]{rounds}[/cyan]  |  msg delay: [cyan]{base_delay}s[/cyan]"
            f"  |  round delay: [cyan]{round_delay}s[/cyan]\n"
            f"Total posts: [cyan]{total}[/cyan]",
            title="Confirm broadcast", style="yellow"))
        if not await questionary.confirm("Proceed?", default=False).ask_async():
            warn("Cancelled.")
            return

        await run_forwarding(client, selected_ads, selected_ids, id_to_title,
                             rounds, base_delay, round_delay, s["jitter"])
    except Exception as e:
        err(f"Forwarding error: {e}")
    # NOTE: never disconnect here — the client stays online and listening.


async def run_forwarding(client, ads, selected_ids, id_to_title,
                         rounds, base_delay, round_delay, jitter):
    total = len(selected_ids) * len(ads) * rounds
    sent = failed = 0
    log_lines = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Posting", total=total)

        for rnd in range(1, rounds + 1):
            progress.update(task, description=f"Round {rnd}/{rounds}")
            for chat_id in selected_ids:
                title = id_to_title.get(chat_id, str(chat_id))
                for ad in ads:
                    try:
                        await send_ad_copy(client, chat_id, ad)
                        sent += 1
                        log_lines.append(f"[green]sent[/green]  -> {title}")
                    except FloodWaitError as e:
                        progress.update(task, description=f"Rate limited: waiting {e.seconds}s")
                        warn(f"Telegram rate limit hit. Waiting {e.seconds}s as requested.")
                        await asyncio.sleep(e.seconds)
                        try:
                            await send_ad_copy(client, chat_id, ad)
                            sent += 1
                            log_lines.append(f"[green]sent[/green]  -> {title} (after wait)")
                        except Exception as e2:
                            failed += 1
                            log_lines.append(f"[red]fail[/red]  -> {title}: {e2}")
                    except ChatWriteForbiddenError:
                        failed += 1
                        log_lines.append(f"[yellow]skip[/yellow]  -> {title}: no permission")
                    except Exception as e:
                        failed += 1
                        log_lines.append(f"[red]fail[/red]  -> {title}: {e}")

                    progress.advance(task)
                    pause = base_delay
                    if jitter and base_delay > 0:
                        pause = base_delay + random.uniform(0, 2)
                    if pause > 0:
                        await asyncio.sleep(pause)

            if rnd < rounds and round_delay > 0:
                progress.update(task, description=f"Waiting {round_delay}s before round {rnd + 1}")
                await asyncio.sleep(round_delay)

    console.print()
    table = Table(title="Broadcast Summary", header_style="bold cyan")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Sent", f"[green]{sent}[/green]")
    table.add_row("Failed / skipped", f"[red]{failed}[/red]")
    table.add_row("Total attempted", str(total))
    console.print(table)

    if log_lines:
        console.print("[dim]Recent activity:[/dim]")
        for line in log_lines[-12:]:
            console.print("  " + line)


# -----------------------------------------------------------------------------
# Live listener: /savead, mention capture, DM auto-reply
# -----------------------------------------------------------------------------
def register_handlers(client, config, own_id, reply_state, alert_state):
    features = config["features"]
    texts = config["texts"]
    settings = config["settings"]

    @client.on(events.NewMessage(chats=own_id, outgoing=True))
    async def on_saved(event):
        if not config["features"].get("savead_enabled"):
            return
        text = (event.raw_text or "").strip()
        if not text.lower().startswith("/savead"):
            return
        if event.is_reply:
            replied = await event.get_reply_message()
            if replied is None:
                await event.respond("Nothing to save (reply not found).")
                return
            preview = (replied.message or "(media)")[:60]
            config["ads"].append({
                "kind": "msgref", "message_id": replied.id,
                "preview": preview, "added": datetime.now().isoformat()})
        else:
            content = text[len("/savead"):].strip()
            if not content:
                await event.respond("Usage: reply to a message with /savead, or /savead <text>")
                return
            config["ads"].append({
                "kind": "text", "content": content,
                "added": datetime.now().isoformat()})
        save_config(config)
        await event.respond(config["texts"].get("savead_confirm", "Ad saved."))
        console.print(f"[green]/savead[/green] captured an ad ({len(config['ads'])} total).")

    @client.on(events.NewMessage(incoming=True))
    async def on_incoming(event):
        # Mention capture in groups
        if event.is_group and event.mentioned and config["features"].get("capture_mentions_enabled"):
            try:
                chat = await event.get_chat()
                sender = await event.get_sender()
                who = getattr(sender, "username", None) or getattr(sender, "first_name", "?")
                chat_title = getattr(chat, "title", str(event.chat_id))
                line = (f"{datetime.now().isoformat()} | {chat_title} | "
                        f"@{who} | {event.raw_text}")
                with open(CAPTURES_FILE, "a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
                console.print(f"[cyan]mention[/cyan] captured in {chat_title}.")
            except Exception as e:
                warn(f"Could not save mention: {e}")

        if not event.is_private:
            return

        sender = await event.get_sender()
        if sender is None or getattr(sender, "bot", False) or getattr(sender, "is_self", False):
            return
        uid = event.sender_id
        who = ("@" + sender.username) if getattr(sender, "username", None) \
            else (getattr(sender, "first_name", None) or str(uid))
        now = time.monotonic()

        # DM auto-reply to the sender (single message, cooldown-limited)
        if config["features"].get("dm_autoreply_enabled"):
            cooldown = config["settings"].get("autoreply_cooldown", 3600)
            if now - reply_state.get(uid, 0) >= cooldown:
                reply_state[uid] = now
                try:
                    await event.respond(config["texts"].get("dm_autoreply", ""))
                    console.print(f"[green]auto-reply[/green] sent to {who}.")
                except FloodWaitError as e:
                    warn(f"Rate limited on auto-reply; waiting {e.seconds}s.")
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    warn(f"Auto-reply failed: {e}")

        # Self-alert: ping YOUR OWN Saved Messages so notifications get you online.
        # These go to "me" (your own account), never to the sender.
        if config["features"].get("dm_alert_enabled"):
            cooldown = config["settings"].get("dm_alert_cooldown", 60)
            if now - alert_state.get(uid, 0) >= cooldown:
                alert_state[uid] = now
                count = int(config["settings"].get("dm_alert_count", 10))
                delay = float(config["settings"].get("dm_alert_delay", 2))
                text = config["texts"].get("dm_alert", "{who} dmed you.").replace("{who}", who)
                console.print(f"[cyan]alert[/cyan] pushing {count} self-notifications ({who}).")
                for _ in range(max(1, count)):
                    try:
                        await client.send_message("me", text)
                    except FloodWaitError as e:
                        warn(f"Flood wait on alert; waiting {e.seconds}s.")
                        await asyncio.sleep(e.seconds)
                    except Exception as e:
                        warn(f"Alert send failed: {e}")
                        break
                    if delay > 0:
                        await asyncio.sleep(delay)


async def listener_status(config):
    """Show which accounts are live and what the listener is handling.

    Listening is always-on: accounts connect at startup and keep processing
    updates in the background while you use the menu. This view also lets you
    (re)connect any account that isn't currently online.
    """
    accounts = config["accounts"]
    if not accounts:
        warn("No accounts yet. Use 'Add Account' — it starts listening immediately.")
        return

    f = config["features"]
    table = Table(title="Listener", header_style="bold cyan")
    table.add_column("Account")
    table.add_column("State")
    offline = []
    for phone in accounts:
        client = CLIENTS.get(phone)
        if client is not None and client.is_connected():
            table.add_row(phone, "[green]online — listening[/green]")
        else:
            table.add_row(phone, "[red]offline[/red]")
            offline.append(phone)
    console.print(table)
    console.print(
        f"Handling: /savead [{'on' if f['savead_enabled'] else 'off'}]  •  "
        f"mentions [{'on' if f['capture_mentions_enabled'] else 'off'}]  •  "
        f"DM auto-reply [{'on' if f['dm_autoreply_enabled'] else 'off'}]  •  "
        f"DM self-alert x{config['settings']['dm_alert_count']} "
        f"[{'on' if f['dm_alert_enabled'] else 'off'}]")

    if offline and await questionary.confirm(
            f"Reconnect {len(offline)} offline account(s) now?", default=True).ask_async():
        for phone in offline:
            try:
                client = await start_account(phone, accounts[phone], config)
                if client:
                    ok(f"{phone} is back online and listening.")
                else:
                    warn(f"{phone}: not logged in — re-add via 'Add Account'.")
            except Exception as e:
                err(f"Could not reconnect {phone}: {e}")


# -----------------------------------------------------------------------------
# Main menu loop
# -----------------------------------------------------------------------------
async def main():
    banner()
    config = load_config()
    info(f"Loaded {len(config['accounts'])} account(s) and {len(config['ads'])} ad(s).")

    # Bring every account online and listening immediately.
    await start_all_accounts(config)

    actions = {
        "add": add_account,
        "forward": forward_messages,
        "ads": manage_ads,
        "listen": listener_status,
        "list": list_accounts,
        "settings": edit_settings,
        "remove": remove_account,
    }

    try:
        while True:
            console.print()
            choice = await questionary.select(
                "Main menu",
                choices=[
                    Choice("1. Add Account", value="add"),
                    Choice("2. Post Ads (copy-paste to selected chats)", value="forward"),
                    Choice("3. Manage Ads", value="ads"),
                    Choice("4. Listener Status", value="listen"),
                    Choice("5. List Accounts", value="list"),
                    Choice("6. Settings", value="settings"),
                    Choice("7. Remove Account", value="remove"),
                    Choice("8. Exit", value="exit"),
                ],
            ).ask_async()

            if choice is None or choice == "exit":
                info("Goodbye.")
                break
            try:
                await actions[choice](config)
            except KeyboardInterrupt:
                warn("Interrupted; returning to menu.")
            except Exception as e:
                err(f"Unexpected error: {e}")
    finally:
        await stop_all_accounts()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[bold cyan]i[/bold cyan]   Exiting.")
