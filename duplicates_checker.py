import argparse
import json
import logging
import os
import re
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from dotenv import load_dotenv


DUP_LOG = logging.getLogger("duplicates")


def build_headers(api_key: str) -> Dict[str, str]:
    return {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
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
class SupabaseConfig:
    url: str
    key: str
    owners_table: str
    objects_table: str
    duplicates_table: str


@dataclass
class DuplicateSettings:
    scan_interval: int
    telegram_poll_interval: int
    min_group_size: int
    price_bucket: int
    area_bucket: float
    page_size: int
    bot_token: Optional[str]
    chat_id: Optional[str]
    thread_id: Optional[int]
    telegram_proxy_url: Optional[str]
    log_level: str
    offset_file: Path


def load_duplicate_settings() -> Tuple[SupabaseConfig, DuplicateSettings]:
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
    owners_table = os.getenv("OWNERS_TABLE", "owners")
    objects_table = os.getenv("OBJECTS_TABLE", "objects")
    duplicates_table = os.getenv("DUPLICATES_TABLE", "duplicates")
    bot_token = (
        os.getenv("DUPLICATES_BOT_TOKEN")
        or os.getenv("NOTIFY_BOT_TOKEN")
        or os.getenv("TELEGRAM_BOT_TOKEN")
    )
    chat_id = os.getenv("DUPLICATES_CHAT_ID") or os.getenv("NOTIFY_CHAT_ID")
    thread_id = _env_optional_int("DUPLICATES_THREAD_ID") or _env_optional_int("NOTIFY_THREAD_ID")
    settings = DuplicateSettings(
        scan_interval=_env_int("DUPLICATES_SCAN_INTERVAL_SECONDS", 600),
        telegram_poll_interval=_env_int("DUPLICATES_TELEGRAM_POLL_SECONDS", 15),
        min_group_size=_env_int("DUPLICATES_MIN_GROUP_SIZE", 2),
        price_bucket=_env_int("DUPLICATES_PRICE_BUCKET", 5000),
        area_bucket=_env_float("DUPLICATES_AREA_BUCKET", 1.0),
        page_size=_env_int("DUPLICATES_PAGE_SIZE", 500),
        bot_token=bot_token,
        chat_id=chat_id,
        thread_id=thread_id,
        telegram_proxy_url=os.getenv("TELEGRAM_PROXY_URL"),
        log_level=os.getenv("DUPLICATES_LOGLEVEL", "INFO").upper(),
        offset_file=Path(os.getenv("DUPLICATES_OFFSET_FILE", ".duplicates_bot.offset")),
    )
    supa = SupabaseConfig(
        url=supabase_url.rstrip("/"),
        key=supabase_key,
        owners_table=owners_table,
        objects_table=objects_table,
        duplicates_table=duplicates_table,
    )
    return supa, settings


def _normalize_address(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    lowered = value.lower()
    lowered = re.sub(r"[.,/\\-]", " ", lowered)
    lowered = re.sub(r"\b(город|г\.)\b", "", lowered)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip() or None


def _normalize_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = re.sub(r"[^\d.,]", "", value)
        if not cleaned:
            return None
        cleaned = cleaned.replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _round_bucket(value: Optional[float], bucket: float) -> Optional[float]:
    if value is None or bucket <= 0:
        return None
    return round(value / bucket) * bucket


class OwnersRepository:
    def __init__(self, config: SupabaseConfig):
        self.config = config
        self.session = requests.Session()
        self.base_url = f"{config.url}/rest/v1/{config.owners_table}"

    def fetch_many(self, owner_ids: Iterable[int]) -> Dict[int, Dict[str, Any]]:
        ids = sorted({oid for oid in owner_ids if isinstance(oid, int)})
        if not ids:
            return {}
        params = {
            "select": "id,url,status,parsed,updated_at",
            "id": f"in.({','.join(str(x) for x in ids)})",
        }
        response = self.session.get(
            self.base_url,
            headers=build_headers(self.config.key),
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        return {row["id"]: row for row in data}

    def fetch_one(self, owner_id: int) -> Optional[Dict[str, Any]]:
        params = {"select": "*", "id": f"eq.{owner_id}", "limit": 1}
        response = self.session.get(
            self.base_url,
            headers=build_headers(self.config.key),
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        rows = response.json()
        if isinstance(rows, list) and rows:
            return rows[0]
        return None

    def update_owner(self, owner_id: int, payload: Dict[str, Any]) -> None:
        headers = build_headers(self.config.key)
        headers["Prefer"] = "return=minimal"
        response = self.session.patch(
            self.base_url,
            headers=headers,
            params={"id": f"eq.{owner_id}"},
            json=payload,
            timeout=30,
        )
        if response.status_code not in (200, 204):
            response.raise_for_status()

    def delete_owner(self, owner_id: int) -> None:
        response = self.session.delete(
            self.base_url,
            headers=build_headers(self.config.key),
            params={"id": f"eq.{owner_id}"},
            timeout=30,
        )
        if response.status_code not in (200, 204):
            response.raise_for_status()


class ObjectsRepository:
    def __init__(self, config: SupabaseConfig, settings: DuplicateSettings):
        self.config = config
        self.settings = settings
        self.session = requests.Session()
        self.base_url = f"{config.url}/rest/v1/{config.objects_table}"
        self.fields = [
            "owners_id",
            "external_id",
            "address",
            "rooms",
            "floor",
            "total_area",
            "price",
            "status",
            "type",
            "title",
            "updated_at",
        ]

    def fetch_active(self) -> List[Dict[str, Any]]:
        headers = build_headers(self.config.key)
        results: List[Dict[str, Any]] = []
        offset = 0
        while True:
            range_end = offset + self.settings.page_size - 1
            headers["Range"] = f"{offset}-{range_end}"
            params = {"select": ",".join(self.fields), "order": "updated_at.desc"}
            response = self.session.get(
                self.base_url,
                headers=headers,
                params=params,
                timeout=60,
            )
            if response.status_code not in (200, 206):
                DUP_LOG.error("Objects fetch failed [%s]: %s", response.status_code, response.text)
                response.raise_for_status()
            batch = response.json()
            if not batch:
                break
            for row in batch:
                if not row.get("owners_id"):
                    continue
                if not self._is_active(row.get("status")):
                    continue
                results.append(row)
            if len(batch) < self.settings.page_size:
                break
            offset += self.settings.page_size
        return results

    @staticmethod
    def _is_active(status_value: Any) -> bool:
        if status_value is None:
            return True
        if isinstance(status_value, bool):
            return status_value
        lowered = str(status_value).strip().lower()
        if lowered in {"active", "published", "true"}:
            return True
        if lowered in {"inactive", "unpublished", "deleted", "false", "archived"}:
            return False
        return True

    def delete_by_owner(self, owner_id: int) -> None:
        response = self.session.delete(
            self.base_url,
            headers=build_headers(self.config.key),
            params={"owners_id": f"eq.{owner_id}"},
            timeout=30,
        )
        if response.status_code not in (200, 204):
            response.raise_for_status()


class DuplicatesRepository:
    def __init__(self, config: SupabaseConfig):
        self.config = config
        self.session = requests.Session()
        self.base_url = f"{config.url}/rest/v1/{config.duplicates_table}"

    def has_pending(self, owner_ids: Sequence[int]) -> bool:
        ids = [str(x) for x in sorted({oid for oid in owner_ids if isinstance(oid, int)})]
        if not ids:
            return False
        params = {
            "select": "id,duplicate_group",
            "owners_id": f"in.({','.join(ids)})",
            "duplicate_status": "eq.pending",
            "limit": 1,
        }
        response = self.session.get(
            self.base_url,
            headers=build_headers(self.config.key),
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        return isinstance(data, list) and len(data) > 0

    def create_group(self, group_id: str, owner_ids: Sequence[int]) -> None:
        payload = [
            {
                "owners_id": oid,
                "duplicate_group": group_id,
                "duplicate_status": "pending",
            }
            for oid in owner_ids
        ]
        response = self.session.post(
            self.base_url,
            headers=build_headers(self.config.key),
            json=payload,
            timeout=30,
        )
        if response.status_code not in (200, 201):
            DUP_LOG.error("Failed to insert duplicates group: %s", response.text)
            response.raise_for_status()

    def fetch_group(self, group_id: str) -> List[Dict[str, Any]]:
        params = {"select": "*", "duplicate_group": f"eq.{group_id}"}
        response = self.session.get(
            self.base_url,
            headers=build_headers(self.config.key),
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else []

    def update_status(self, group_id: str, status: str) -> None:
        payload: Dict[str, Any] = {"duplicate_status": status}
        if status in {"resolved", "ignored"}:
            payload["resolved_at"] = datetime.now(timezone.utc).isoformat()
        headers = build_headers(self.config.key)
        headers["Prefer"] = "return=minimal"
        response = self.session.patch(
            self.base_url,
            headers=headers,
            params={"duplicate_group": f"eq.{group_id}"},
            json=payload,
            timeout=30,
        )
        if response.status_code not in (200, 204):
            response.raise_for_status()

    def delete_group(self, group_id: str) -> None:
        response = self.session.delete(
            self.base_url,
            headers=build_headers(self.config.key),
            params={"duplicate_group": f"eq.{group_id}"},
            timeout=30,
        )
        if response.status_code not in (200, 204):
            response.raise_for_status()


@dataclass
class DuplicateCluster:
    signature: str
    objects: List[Dict[str, Any]]


class DuplicateDetector:
    def __init__(self, settings: DuplicateSettings):
        self.settings = settings

    def find_clusters(self, rows: Sequence[Dict[str, Any]]) -> List[DuplicateCluster]:
        buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            signature = self._signature(row)
            if not signature:
                continue
            buckets[signature].append(row)
        clusters: List[DuplicateCluster] = []
        for signature, members in buckets.items():
            owners = {item.get("owners_id") for item in members}
            if len(members) < self.settings.min_group_size or len(owners) < 2:
                continue
            clusters.append(DuplicateCluster(signature=signature, objects=members))
        return clusters

    def _signature(self, row: Dict[str, Any]) -> Optional[str]:
        address = _normalize_address(row.get("address"))
        if not address:
            return None
        rooms = row.get("rooms")
        floor = row.get("floor")
        total_area = _round_bucket(_normalize_number(row.get("total_area")), self.settings.area_bucket)
        price = _round_bucket(_normalize_number(row.get("price")), self.settings.price_bucket)
        components = [
            address,
            f"rooms:{rooms if rooms not in (None, '') else '?'}",
            f"floor:{floor if floor not in (None, '') else '?'}",
            f"area:{total_area if total_area is not None else '?'}",
            f"price:{price if price is not None else '?'}",
        ]
        return "|".join(str(part) for part in components)


class DuplicateTelegramBot:
    def __init__(self, settings: DuplicateSettings):
        self.settings = settings
        self.session = requests.Session()
        if settings.telegram_proxy_url:
            self.session.proxies.update(
                {"http": settings.telegram_proxy_url, "https": settings.telegram_proxy_url}
            )
        self.api_base = f"https://api.telegram.org/bot{settings.bot_token}" if settings.bot_token else ""
        self.last_update_id = self._load_offset()

    @property
    def enabled(self) -> bool:
        return bool(self.settings.bot_token and self.settings.chat_id)

    def _load_offset(self) -> Optional[int]:
        try:
            return int(self.settings.offset_file.read_text().strip())
        except FileNotFoundError:
            return None
        except ValueError:
            return None

    def _save_offset(self) -> None:
        if self.last_update_id is None:
            return
        self.settings.offset_file.write_text(str(self.last_update_id))

    def send_duplicate_alert(
        self,
        group_id: str,
        lines: List[str],
        owner_ids: Sequence[int],
    ) -> None:
        if not self.enabled:
            DUP_LOG.info("Telegram disabled, skipping alert for group %s", group_id)
            return
        payload: Dict[str, Any] = {
            "chat_id": self.settings.chat_id,
            "text": "\n".join(lines),
            "disable_web_page_preview": True,
            "reply_markup": {
                "inline_keyboard": self._build_keyboard(group_id, owner_ids),
            },
        }
        if self.settings.thread_id is not None:
            payload["message_thread_id"] = self.settings.thread_id
        response = self.session.post(f"{self.api_base}/sendMessage", json=payload, timeout=15)
        if response.status_code != 200:
            DUP_LOG.error("Failed to send duplicate alert: %s", response.text)

    def _build_keyboard(self, group_id: str, owner_ids: Sequence[int]) -> List[List[Dict[str, str]]]:
        buttons: List[List[Dict[str, str]]] = []
        if owner_ids:
            target = min(owner_ids)
            buttons.append(
                [
                    {
                        "text": "🧷 Слить",
                        "callback_data": f"merge|{group_id}|{target}",
                    }
                ]
            )
        buttons.append(
            [
                {
                    "text": "❌ Игнорировать",
                    "callback_data": f"ignore|{group_id}",
                }
            ]
        )
        return buttons

    def poll_updates(self, handler: Callable[[Dict[str, Any]], None]) -> None:
        if not self.enabled:
            return
        params: Dict[str, Any] = {"timeout": 0, "allowed_updates": ["callback_query"]}
        if self.last_update_id is not None:
            params["offset"] = self.last_update_id + 1
        response = self.session.get(f"{self.api_base}/getUpdates", params=params, timeout=30)
        if response.status_code != 200:
            DUP_LOG.error("getUpdates failed: %s", response.text)
            return
        data = response.json()
        results = data.get("result", [])
        for update in results:
            self.last_update_id = update["update_id"]
            handler(update)
        if results:
            self._save_offset()

    def answer_callback(self, callback_id: str, text: str) -> None:
        if not self.enabled:
            return
        payload = {"callback_query_id": callback_id, "text": text, "show_alert": False}
        self.session.post(f"{self.api_base}/answerCallbackQuery", json=payload, timeout=15)

    def edit_message_footer(self, chat_id: int, message_id: int, suffix: str) -> None:
        if not self.enabled:
            return
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "reply_markup": {"inline_keyboard": []},
        }
        self.session.post(f"{self.api_base}/editMessageReplyMarkup", json=payload, timeout=15)
        self.session.post(
            f"{self.api_base}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": suffix,
                "reply_to_message_id": message_id,
                "disable_web_page_preview": True,
            },
            timeout=15,
        )


class DuplicateService:
    def __init__(
        self,
        supabase: SupabaseConfig,
        settings: DuplicateSettings,
    ):
        self.supabase = supabase
        self.settings = settings
        self.objects_repo = ObjectsRepository(supabase, settings)
        self.owners_repo = OwnersRepository(supabase)
        self.duplicates_repo = DuplicatesRepository(supabase)
        self.detector = DuplicateDetector(settings)
        self.bot = DuplicateTelegramBot(settings)

    def scan_once(self) -> None:
        DUP_LOG.info("Scanning objects for duplicates…")
        objects = self.objects_repo.fetch_active()
        DUP_LOG.info("Fetched %d active objects", len(objects))
        clusters = self.detector.find_clusters(objects)
        DUP_LOG.info("Found %d candidate clusters", len(clusters))
        for cluster in clusters:
            owner_ids = sorted(
                {int(obj["owners_id"]) for obj in cluster.objects if isinstance(obj.get("owners_id"), int)}
            )
            if not owner_ids:
                continue
            if self.duplicates_repo.has_pending(owner_ids):
                DUP_LOG.debug("Owners %s already pending duplicate review", owner_ids)
                continue
            group_id = str(uuid.uuid4())
            try:
                self.duplicates_repo.create_group(group_id, owner_ids)
            except Exception:
                DUP_LOG.exception("Failed to persist duplicate group %s", group_id)
                continue
            DUP_LOG.info("Registered duplicate group %s for owners %s", group_id, owner_ids)
            self._notify(group_id, cluster.objects, owner_ids)

    def _notify(self, group_id: str, objects: Sequence[Dict[str, Any]], owner_ids: Sequence[int]) -> None:
        owners = self.owners_repo.fetch_many(owner_ids)
        lines = ["⚠️ Возможный дубль", "", "Кандидаты:"]
        for idx, obj in enumerate(objects, start=1):
            owner_id = obj.get("owners_id")
            owner = owners.get(owner_id, {})
            url = owner.get("url") or "— ссылка не найдена"
            lines.append(f"{idx}) {url}")
        self.bot.send_duplicate_alert(group_id, lines, owner_ids)

    def process_updates(self, update: Dict[str, Any]) -> None:
        callback = update.get("callback_query")
        if not callback:
            return
        data = callback.get("data", "")
        chat = callback.get("message", {}).get("chat", {})
        message_id = callback.get("message", {}).get("message_id")
        chat_id = chat.get("id")
        if not data or not message_id or chat_id is None:
            return
        DUP_LOG.info("Received callback %s", data)
        parts = data.split("|")
        action = parts[0] if parts else ""
        if action == "merge" and len(parts) == 3:
            group_id = parts[1]
            try:
                target_owner = int(parts[2])
            except ValueError:
                self.bot.answer_callback(callback["id"], "Некорректный owner id")
                return
            message = self._merge_group(group_id, target_owner)
            self.bot.answer_callback(callback["id"], message)
            if "успешно" in message.lower():
                self.bot.edit_message_footer(chat_id, message_id, message)
        elif action == "ignore" and len(parts) == 2:
            group_id = parts[1]
            message = self._ignore_group(group_id)
            self.bot.answer_callback(callback["id"], message)
            if "игнор" in message.lower():
                self.bot.edit_message_footer(chat_id, message_id, message)
        else:
            self.bot.answer_callback(callback["id"], "Неизвестное действие")

    def _merge_group(self, group_id: str, target_owner: int) -> str:
        rows = self.duplicates_repo.fetch_group(group_id)
        if not rows:
            return "Группа не найдена"
        status = rows[0].get("duplicate_status")
        if status != "pending":
            return f"Группа уже {status}"
        owner_ids = [row.get("owners_id") for row in rows if row.get("owners_id")]
        if target_owner not in owner_ids:
            return "owner не входит в группу"
        donors = [oid for oid in owner_ids if oid != target_owner]
        target = self.owners_repo.fetch_one(target_owner)
        if not target:
            return f"owner {target_owner} не найден"
        DUP_LOG.info("Merging group %s into owner %s (donors %s)", group_id, target_owner, donors)
        for donor in donors:
            try:
                self.objects_repo.delete_by_owner(donor)
                self.owners_repo.delete_owner(donor)
                DUP_LOG.info("Удалён дубль owner %s и связанные объекты", donor)
            except Exception:
                DUP_LOG.exception("Не удалось удалить owner %s", donor)
                return "Ошибка при удалении дублей"
        self.duplicates_repo.update_status(group_id, "resolved")
        return f"Группа {group_id[:8]} успешно слита в owner {target_owner}"

    def _ignore_group(self, group_id: str) -> str:
        rows = self.duplicates_repo.fetch_group(group_id)
        if not rows:
            return "Группа не найдена"
        status = rows[0].get("duplicate_status")
        if status != "pending":
            return f"Группа уже {status}"
        self.duplicates_repo.update_status(group_id, "ignored")
        return f"Группа {group_id[:8]} помечена как игнор"

    def run_forever(self) -> None:
        DUP_LOG.info("Duplicate monitor started: scan every %ss", self.settings.scan_interval)
        next_scan = time.time()
        next_poll = time.time()
        while True:
            now = time.time()
            if now >= next_scan:
                try:
                    self.scan_once()
                except Exception:
                    DUP_LOG.exception("Duplicate scan failed")
                finally:
                    next_scan = now + self.settings.scan_interval
            if now >= next_poll:
                try:
                    self.bot.poll_updates(self.process_updates)
                except Exception:
                    DUP_LOG.exception("Failed to poll Telegram updates")
                finally:
                    next_poll = now + self.settings.telegram_poll_interval
            time.sleep(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Objects duplicate quarantine")
    parser.add_argument("--run-once", action="store_true", help="run a single duplicate scan and exit")
    parser.add_argument("--log-level", default=None, help="override log level (e.g. DEBUG)")
    return parser.parse_args()


def main() -> None:
    supa, settings = load_duplicate_settings()
    args = parse_args()
    log_level = args.log_level or settings.log_level
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    service = DuplicateService(supa, settings)
    if args.run_once:
        DUP_LOG.info("Manual duplicate scan started")
        service.scan_once()
        DUP_LOG.info("Manual duplicate scan finished")
        return
    service.run_forever()


if __name__ == "__main__":
    main()
