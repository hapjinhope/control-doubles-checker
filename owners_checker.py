import argparse
import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from requests import HTTPError

LOG = logging.getLogger("owners-checker")


def build_headers(api_key: str) -> Dict[str, str]:
    return {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a number") from exc


def _env_optional_int(name: str) -> Optional[int]:
    raw = os.getenv(name)
    if raw in (None, ""):
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc


@dataclass
class CheckConfig:
    supabase_url: str
    supabase_key: str
    owners_table: str
    check_column: str
    reset_hour: int
    owners_link_column: str
    objects_table: str
    objects_price_column: str
    objects_link_column: str
    parser_url: str
    window_start_hour: int
    window_end_hour: int
    avito_interval: float
    cian_interval: float
    batch_size: int
    notify_bot_token: Optional[str]
    notify_chat_id: Optional[str]
    notify_thread_id: Optional[int]
    telegram_proxy_url: Optional[str]
    log_level: str
    summary_report_hour: int


def load_config() -> CheckConfig:
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    parser_url = os.getenv("PARSER_CHECK_URL")
    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
    if not parser_url:
        raise RuntimeError("PARSER_CHECK_URL must be set (parser endpoint)")

    window_start = _env_int("CHECK_WINDOW_START_HOUR", 1)
    window_end = _env_int("CHECK_WINDOW_END_HOUR", 4)
    if window_start < 0 or window_start > 23:
        raise RuntimeError("CHECK_WINDOW_START_HOUR must be within 0-23")
    if window_end <= window_start or window_end > 24:
        raise RuntimeError("CHECK_WINDOW_END_HOUR must be within 1-24 and greater than start")

    owners_link_column = os.getenv("OWNERS_LINK_COLUMN") or "external_id"
    objects_link_column = os.getenv("OBJECTS_LINK_COLUMN") or owners_link_column

    return CheckConfig(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        owners_table=os.getenv("OWNERS_TABLE", "owners"),
        check_column=os.getenv("OWNERS_CHECK_COLUMN", "checked"),
        reset_hour=_env_int("OWNERS_CHECK_RESET_HOUR", 0),
        owners_link_column=owners_link_column,
        objects_table=os.getenv("OBJECTS_TABLE", "objects"),
        objects_price_column=os.getenv("OBJECTS_PRICE_COLUMN", "price"),
        objects_link_column=objects_link_column,
        parser_url=parser_url,
        window_start_hour=window_start,
        window_end_hour=window_end,
        avito_interval=_env_float("AVITO_REQUEST_INTERVAL_SECONDS", 60.0),
        cian_interval=_env_float("CIAN_REQUEST_INTERVAL_SECONDS", 0.0),
        batch_size=_env_int("CHECK_BATCH_SIZE", 200),
        notify_bot_token=os.getenv("NOTIFY_BOT_TOKEN") or None,
        notify_chat_id=os.getenv("NOTIFY_CHAT_ID") or None,
        notify_thread_id=_env_optional_int("NOTIFY_THREAD_ID"),
        telegram_proxy_url=os.getenv("TELEGRAM_PROXY_URL") or None,
        log_level=os.getenv("OWNERS_CHECK_LOGLEVEL", "INFO").upper(),
        summary_report_hour=_env_int("SUMMARY_REPORT_HOUR", 10),
    )


class OwnersRepository:
    def __init__(self, config: CheckConfig):
        self.config = config
        self.session = requests.Session()
        self.rest_url = self._rest_url(config.owners_table)

    def _rest_url(self, table: str) -> str:
        return self.config.supabase_url.rstrip("/") + f"/rest/v1/{table}"

    def fetch_unchecked(self, limit: int) -> List[Dict[str, Any]]:
        params = {
            "select": self._select_fields(),
            "order": "updated_at.asc.nullsfirst",
            "limit": limit,
            "or": f"({self.config.check_column}.is.false,{self.config.check_column}.is.null)",
        }
        response = self.session.get(
            self.rest_url,
            headers=build_headers(self.config.supabase_key),
            params=params,
            timeout=30,
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            LOG.error("Failed to fetch owners batch: %s - %s", exc, response.text)
            raise
        data = response.json()
        return data if isinstance(data, list) else []

    def update_owner(self, owner_id: int, payload: Dict[str, Any]) -> None:
        headers = build_headers(self.config.supabase_key)
        headers["Prefer"] = "return=minimal"
        params = {"id": f"eq.{owner_id}"}
        response = self.session.patch(
            self.rest_url,
            headers=headers,
            params=params,
            json=payload,
            timeout=30,
        )
        if response.status_code not in (200, 204):
            response.raise_for_status()

    def reset_checks(self) -> None:
        headers = build_headers(self.config.supabase_key)
        headers["Prefer"] = "return=minimal"
        payload = {self.config.check_column: False}
        response = self.session.patch(
            self.rest_url,
            headers=headers,
            params={"id": "not.is.null"},
            json=payload,
            timeout=60,
        )
        if response.status_code not in (200, 204):
            response.raise_for_status()

    def delete_owner(self, owner_id: int) -> None:
        headers = build_headers(self.config.supabase_key)
        response = self.session.delete(
            self.rest_url,
            headers=headers,
            params={"id": f"eq.{owner_id}"},
            timeout=30,
        )
        if response.status_code not in (200, 204):
            response.raise_for_status()

    def _select_fields(self) -> str:
        fields = ["id", "url", "parsed", "status", "updated_at", self.config.check_column]
        link_col = self.config.owners_link_column
        if link_col and link_col not in fields:
            fields.append(link_col)
        return ",".join(fields)


class ObjectsRepository:
    def __init__(self, config: CheckConfig):
        self.config = config
        self.session = requests.Session()
        self.rest_url = config.supabase_url.rstrip("/") + f"/rest/v1/{config.objects_table}"
        self.link_column = config.objects_link_column or "id"

    def _make_params(self, link_value: Any) -> Dict[str, Any]:
        return {
            "select": f"id,{self.config.objects_price_column},{self.link_column}",
            self.link_column: f"eq.{link_value}",
            "limit": 1,
        }

    def fetch_by_link(self, link_value: Any) -> Optional[Dict[str, Any]]:
        params = self._make_params(link_value)
        response = self.session.get(
            self.rest_url,
            headers=build_headers(self.config.supabase_key),
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list) and data:
            return data[0]
        return None

    def update_price(self, link_value: Any, new_price: Any) -> None:
        headers = build_headers(self.config.supabase_key)
        headers["Prefer"] = "return=minimal"
        payload = {self.config.objects_price_column: new_price}
        response = self.session.patch(
            self.rest_url,
            headers=headers,
            params={self.link_column: f"eq.{link_value}"},
            json=payload,
            timeout=30,
        )
        if response.status_code not in (200, 204):
            response.raise_for_status()

    def delete_object(self, link_value: Any) -> None:
        headers = build_headers(self.config.supabase_key)
        response = self.session.delete(
            self.rest_url,
            headers=headers,
            params={self.link_column: f"eq.{link_value}"},
            timeout=30,
        )
        if response.status_code not in (200, 204):
            response.raise_for_status()


class TelegramNotifier:
    def __init__(self, config: CheckConfig):
        self.token = config.notify_bot_token
        self.chat_id = config.notify_chat_id
        self.thread_id = config.notify_thread_id
        self.session = requests.Session()
        if config.telegram_proxy_url:
            self.session.proxies.update({"http": config.telegram_proxy_url, "https": config.telegram_proxy_url})

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    def send_summary(self, report_date: date, lines: List[str]) -> None:
        if not self.enabled or not lines:
            return
        header = f"🗒️ Отчёт по проверке за {report_date.strftime('%d.%m.%Y')}"
        text = header + "\n" + "\n".join(lines)
        self._send_message(text)

    def _send_message(self, text: str) -> None:
        if not self.enabled:
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload: Dict[str, Any] = {
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if self.thread_id is not None:
            payload["message_thread_id"] = self.thread_id
        try:
            response = self.session.post(url, json=payload, timeout=15)
            response.raise_for_status()
        except requests.RequestException as exc:
            LOG.error("Unable to send Telegram message: %s", exc)

    @staticmethod
    def _format_reference(object_id: int, external_id: Optional[Any]) -> str:
        if external_id not in (None, ""):
            return f"#{object_id} (external_id {external_id})"
        return f"#{object_id}"

    @staticmethod
    def _fmt_price(value: float) -> str:
        return f"{value:,.0f}".replace(",", " ")


class OwnersCheckService:
    def __init__(self, config: CheckConfig):
        self.config = config
        self.owners_repo = OwnersRepository(config)
        self.objects_repo = ObjectsRepository(config)
        self.notifier = TelegramNotifier(config)
        self.parser_session = requests.Session()
        self.last_rate_call: Dict[str, datetime] = {}
        self.next_run_at: Optional[datetime] = None
        self.last_reset_date: Optional[date] = None
        self.daily_events: List[str] = []
        self.events_date: date = datetime.now().date()
        self.last_summary_date: Optional[date] = None

    def run_forever(self) -> None:
        LOG.info(
            "Checker ready: window %02d-%02d, reset hour %02d",
            self.config.window_start_hour,
            self.config.window_end_hour,
            self.config.reset_hour,
        )
        while True:
            now = datetime.now().astimezone()
            self._maybe_reset(now)
            self._maybe_send_summary(now)
            if self.next_run_at is None:
                self.next_run_at = self._pick_next_run(now)
                LOG.info("Next run scheduled at %s", self.next_run_at.isoformat())
            if now >= self.next_run_at:
                try:
                    count = self.run_cycle()
                    LOG.info("Cycle finished, processed %d owners", count)
                except Exception:
                    LOG.exception("Cycle failed")
                finally:
                    self.next_run_at = self._pick_next_run(now + timedelta(minutes=1))
                    LOG.info("Next run scheduled at %s", self.next_run_at.isoformat())
            time.sleep(15)

    def run_cycle(self) -> int:
        processed = 0
        while True:
            batch = self.owners_repo.fetch_unchecked(self.config.batch_size)
            if not batch:
                break
            for owner in batch:
                processed += int(self._process_owner(owner))
        return processed

    def _maybe_reset(self, now: datetime) -> None:
        if now.hour < self.config.reset_hour:
            return
        if self.last_reset_date == now.date():
            return
        LOG.info("Resetting %s column for table %s", self.config.check_column, self.config.owners_table)
        self.owners_repo.reset_checks()
        self.last_reset_date = now.date()

    def _pick_next_run(self, reference: datetime) -> datetime:
        tz = reference.tzinfo
        start = datetime.combine(reference.date(), dt_time(self.config.window_start_hour), tzinfo=tz)
        end = datetime.combine(reference.date(), dt_time(self.config.window_end_hour), tzinfo=tz)
        if reference >= end:
            start += timedelta(days=1)
            end += timedelta(days=1)
        elif reference > start:
            start = reference
        window_seconds = int((end - start).total_seconds())
        if window_seconds <= 0:
            raise RuntimeError("Invalid window configuration")
        return start + timedelta(seconds=random.randint(0, max(window_seconds - 1, 0)))

    def _process_owner(self, owner: Dict[str, Any]) -> bool:
        owner_id = owner.get("id")
        link_value = self._owner_link(owner)
        url = (owner.get("url") or "").strip()
        if not owner_id:
            return False
        if not url:
            LOG.warning("Owner #%s skipped: empty URL", owner_id)
            self._mark_checked(owner_id, {"parsed": "EMPTY_URL"})
            return True

        source = self._detect_source(url)
        try:
            self._apply_rate_limit(source)
            parser_result = self._call_parser(owner_id, url, source)
        except HTTPError as exc:
            LOG.error("Parser HTTP error for owner #%s (%s): %s", owner_id, source, exc)
            return False
        except requests.RequestException as exc:
            LOG.error("Parser network error for owner #%s: %s", owner_id, exc)
            return False

        status_flag = self._extract_status(parser_result)
        inactive_reason = None
        if status_flag is False:
            inactive_reason = self._build_inactive_reason(parser_result)
            self._delete_everywhere(owner_id, link_value, parser_result, inactive_reason)
            return True

        self._handle_price(owner_id, link_value, parser_result)
        payload = {
            self.config.check_column: True,
            "parsed": self._stringify_result(parser_result),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if status_flag is not None:
            payload["status"] = status_flag
        else:
            payload["status"] = True
        self.owners_repo.update_owner(owner_id, payload)
        return True

    def _call_parser(self, owner_id: int, url: str, source: str) -> Any:
        payload = {"url": url, "owner_id": owner_id, "source": source}
        response = self.parser_session.post(self.config.parser_url, json=payload, timeout=120)
        response.raise_for_status()
        try:
            return response.json()
        except ValueError:
            return response.text.strip()

    def _apply_rate_limit(self, source: str) -> None:
        interval = self._interval_for_source(source)
        if interval <= 0:
            self.last_rate_call[source] = datetime.now()
            return
        last_call = self.last_rate_call.get(source)
        if last_call:
            elapsed = (datetime.now() - last_call).total_seconds()
            delay = interval - elapsed
            if delay > 0:
                time.sleep(delay)
        self.last_rate_call[source] = datetime.now()

    def _interval_for_source(self, source: str) -> float:
        if source == "avito":
            return self.config.avito_interval
        if source == "cian":
            return self.config.cian_interval
        return max(self.config.cian_interval, 2.0)

    @staticmethod
    def _detect_source(url: str) -> str:
        lowered = url.lower()
        if "avito" in lowered:
            return "avito"
        if "cian" in lowered or "циан" in lowered:
            return "cian"
        return "unknown"

    def _delete_everywhere(
        self,
        owner_id: int,
        link_value: Optional[Any],
        parser_result: Optional[Any] = None,
        reason: Optional[str] = None,
    ) -> None:
        row = self.objects_repo.fetch_by_link(link_value) if link_value else None
        external_value = row.get(self.objects_repo.link_column) if row else link_value
        label = self._format_external_label(external_value)
        summary_line = "❌ " + label + " — объявление больше не актуально"
        LOG.info(summary_line)
        self._register_event(summary_line)
        try:
            self.owners_repo.delete_owner(owner_id)
            LOG.info("Owner %s удалён", label)
        except Exception:
            LOG.exception("Failed to delete owner %s", label)
        if external_value in (None, ""):
            LOG.warning(
                "%s отсутствует для %s, пропускаем удаление в objects",
                self.objects_repo.link_column,
                label,
            )
        else:
            try:
                self.objects_repo.delete_object(external_value)
                LOG.info(
                    "Object %s удалён из %s по %s=%s",
                    label,
                    self.config.objects_table,
                    self.objects_repo.link_column,
                    external_value,
                )
            except Exception:
                LOG.exception(
                    "Failed to delete object %s by %s %s",
                    label,
                    self.objects_repo.link_column,
                    external_value,
                )

    def _handle_price(self, owner_id: int, link_value: Optional[Any], parser_result: Any) -> None:
        if not isinstance(parser_result, dict):
            return
        if not link_value:
            LOG.debug("Owner #%s не содержит %s — пропуск обновления цены", owner_id, self.config.owners_link_column)
            return
        raw_price = self._extract_price(parser_result)
        if raw_price is None:
            return
        new_price = self._normalize_price(raw_price)
        if new_price is None:
            return
        row = self.objects_repo.fetch_by_link(link_value)
        if not row:
            LOG.debug(
                "Object with %s=%s not found in %s",
                self.objects_repo.link_column,
                link_value,
                self.config.objects_table,
            )
            return
        label = self._format_object_label(owner_id, link_value)
        current_raw = row.get(self.config.objects_price_column)
        current_price = self._normalize_price(current_raw)
        if current_price is None or new_price < current_price:
            try:
                self.objects_repo.update_price(link_value, new_price)
                if current_price is None:
                    LOG.info("Initialized price for %s -> %s", label, new_price)
                else:
                    LOG.info("Price decreased for %s: %s -> %s", label, current_price, new_price)
                    self._register_event(
                        f"📉 {label} — цена изменилась {self._fmt_price(current_price)} → {self._fmt_price(new_price)}"
                    )
            except Exception:
                LOG.exception("Failed to update price for %s", label)
        elif new_price > current_price:
            LOG.info("Price increased for %s: %s -> %s", label, current_price, new_price)
            self._register_event(
                f"📈 {label} — цена изменилась {self._fmt_price(current_price)} → {self._fmt_price(new_price)}"
            )

    @staticmethod
    def _extract_status(data: Any) -> Optional[bool]:
        if not isinstance(data, dict):
            return None
        for key in ("status", "is_actual", "actual", "is_active", "active"):
            value = data.get(key)
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {"unpublished", "inactive", "deleted", "closed", "removed", "not_active"}:
                    return False
                if lowered in {"active", "published", "available", "actual"}:
                    return True
        return None

    @staticmethod
    def _extract_price(data: Dict[str, Any]) -> Optional[Any]:
        for key in ("price", "price_total", "price_rub", "amount", "price_value"):
            if key in data:
                return data[key]
        return None

    @staticmethod
    def _normalize_price(value: Any) -> Optional[float]:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            cleaned = value.replace("\xa0", "").replace(" ", "")
            cleaned = re.sub(r"[^\d.,-]", "", cleaned)
            if not cleaned:
                return None
            cleaned = cleaned.replace(",", ".")
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None

    def _stringify_result(self, data: Any) -> str:
        if isinstance(data, (dict, list)):
            return json.dumps(data, ensure_ascii=False)
        return str(data)

    def _mark_checked(self, owner_id: int, extra: Dict[str, Any]) -> None:
        payload = {
            self.config.check_column: True,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        payload.update(extra)
        self.owners_repo.update_owner(owner_id, payload)

    def _owner_link(self, owner: Dict[str, Any]) -> Optional[Any]:
        column = self.config.owners_link_column
        if not column:
            return owner.get("id")
        value = owner.get(column)
        if value in ("", None) and column != "id":
            return owner.get("id")
        return value

    def _format_external_label(self, link_value: Optional[Any]) -> str:
        column = self.objects_repo.link_column
        if link_value in (None, ""):
            return f"{column}=unknown"
        return f"{column} {link_value}"

    @staticmethod
    def _format_snippet(value: Any) -> str:
        if value is None:
            return "None"
        if isinstance(value, (str, int, float, bool)):
            return str(value)[:200]
        try:
            return json.dumps(value, ensure_ascii=False)[:200]
        except (TypeError, ValueError):
            return str(value)[:200]

    @staticmethod
    def _build_inactive_reason(parser_result: Any) -> Optional[str]:
        if isinstance(parser_result, dict):
            parts: List[str] = []
            status_val = parser_result.get("status")
            message = parser_result.get("message")
            source = parser_result.get("source")
            if status_val:
                parts.append(f"parser status={status_val}")
            if message:
                parts.append(message)
            if source:
                parts.append(f"source={source}")
            if parts:
                return "; ".join(parts)
        return None

    @staticmethod
    def _fmt_price(value: float) -> str:
        return f"{value:,.0f}".replace(",", " ")

    def _register_event(self, line: str) -> None:
        today = datetime.now().date()
        if today != self.events_date:
            self.daily_events = []
            self.events_date = today
            self.last_summary_date = None
        self.daily_events.append(line)

    def _maybe_send_summary(self, now: datetime) -> None:
        if now.hour < self.config.summary_report_hour:
            return
        if self.last_summary_date == now.date():
            return
        if not self.daily_events:
            self.last_summary_date = now.date()
            return
        LOG.info("Sending daily summary (%d events)", len(self.daily_events))
        self.notifier.send_summary(now.date(), self.daily_events)
        self.last_summary_date = now.date()
        self.daily_events = []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Nightly owners checker")
    parser.add_argument("--run-now", action="store_true", help="run a single cycle immediately and exit")
    parser.add_argument("--reset-only", action="store_true", help="reset checked flag and exit")
    return parser.parse_args()


def main() -> None:
    config = load_config()
    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    args = parse_args()
    service = OwnersCheckService(config)
    if args.reset_only:
        LOG.info("Resetting checkmarks (manual run)")
        service.owners_repo.reset_checks()
        return
    if args.run_now:
        LOG.info("Manual cycle started")
        processed = service.run_cycle()
        LOG.info("Manual cycle finished, processed %d owners", processed)
        return
    service.run_forever()


if __name__ == "__main__":
    main()
