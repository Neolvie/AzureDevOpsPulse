import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler

from collector import TFSClient, sync_repository, sync_work_items
from database import Database
from logger import get_logger

log = get_logger("scheduler")

# Статус синхронизации хранится отдельно по каждому инстансу:
#   _sync_status[instance_id] = {running, started_at, message, progress, total}
_sync_status: dict = {}
_scheduler: Optional[BackgroundScheduler] = None
_lock = threading.Lock()


def _status(instance_id: str) -> dict:
    """Вернуть (создав при необходимости) словарь статуса для инстанса."""
    return _sync_status.setdefault(
        instance_id,
        {"running": False, "started_at": None, "message": "", "progress": 0, "total": 0},
    )


def get_sync_status(instance_id: str = "default") -> dict:
    return dict(_status(instance_id))


def _last_synced_to_from_date(ts: str, fallback: str) -> str:
    """Конвертирует last_synced timestamp в from_date для TFS API с запасом 1 час."""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        dt = dt - timedelta(hours=1)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return fallback


def run_sync(db: Database, from_date: str, to_date: str, incremental: bool = False,
             instance_id: str = "default"):
    st = _status(instance_id)
    with _lock:
        if st["running"]:
            log.warning("[%s] Синхронизация уже запущена, пропуск", instance_id)
            return
        mode = "инкрементальная" if incremental else "полная"
        st.update({"running": True, "started_at": datetime.now(timezone.utc).isoformat(),
                   "message": f"Запуск ({mode})...", "progress": 0, "total": 0})

    log.info("[%s] Синхронизация запущена (%s): %s → %s", instance_id, mode, from_date, to_date)
    try:
        settings = db.get_settings()
        if not settings.get("pat") or not settings.get("collection"):
            st.update({"running": False, "message": "Ошибка: PAT или коллекция не настроены"})
            log.error("[%s] Синхронизация прервана: PAT/коллекция не заданы", instance_id)
            return

        client = TFSClient(
            url=settings.get("tfs_url", ""),
            pat=settings["pat"],
            collection=settings["collection"],
        )

        ok, msg = client.test_connection()
        if not ok:
            st.update({"running": False, "message": f"Ошибка подключения: {msg}"})
            log.error("[%s] Синхронизация прервана: %s", instance_id, msg)
            return

        all_projects = client.get_projects()
        selected_ids = db.get_selected_projects()
        if selected_ids:
            projects = [p for p in all_projects if p["id"] in selected_ids]
            log.info("Фильтр проектов: %d из %d выбрано", len(projects), len(all_projects))
        else:
            projects = all_projects
            log.info("Проекты не выбраны — синхронизируются все (%d)", len(projects))

        repos_all = []
        for proj in projects:
            repos = client.get_repositories(proj["id"])
            for r in repos:
                repos_all.append((proj, r))

        st["total"] = len(repos_all)
        st["message"] = f"Найдено {len(repos_all)} репозиториев"

        for i, (proj, repo) in enumerate(repos_all, 1):
            repo_id = repo["id"]
            st["progress"] = i
            st["message"] = f"[{i}/{len(repos_all)}] {proj['name']} / {repo['name']}"

            # Инкрементальный режим: берём from_date из last_synced репозитория
            if incremental:
                last_ts = db.get_repo_last_synced(repo_id)
                repo_from = _last_synced_to_from_date(last_ts, from_date) if last_ts else from_date
                if last_ts:
                    log.info("Инкрементально: %s с %s", repo["name"], repo_from)
            else:
                repo_from = from_date

            sync_repository(
                client=client,
                db=db,
                project_id=proj["id"],
                project_name=proj["name"],
                repo=repo,
                from_date=repo_from,
                to_date=to_date,
                collection=settings["collection"],
            )

        # Work items — синхронизируем по уникальным проектам
        synced_projects = {proj["id"] for proj, _ in repos_all}
        for pi, proj_id in enumerate(synced_projects, 1):
            st["message"] = f"Work items [{pi}/{len(synced_projects)}]..."

            if incremental:
                wi_last_ts = db.get_project_wi_last_synced(proj_id)
                wi_from = _last_synced_to_from_date(wi_last_ts, from_date) if wi_last_ts else from_date
                if wi_last_ts:
                    log.info("WI инкрементально: проект %s с %s", proj_id, wi_from)
            else:
                wi_from = from_date

            sync_work_items(client=client, db=db, project_id=proj_id,
                            from_date=wi_from, to_date=to_date)

        st["message"] = "Обновление карты логинов..."
        db.rebuild_login_map()
        st.update({"running": False, "message": f"Завершено ({mode}). Репозиториев: {len(repos_all)}"})
        log.info("[%s] Синхронизация завершена (%s): %d репозиториев", instance_id, mode, len(repos_all))
    except Exception as e:
        st.update({"running": False, "message": f"Ошибка: {e}"})
        log.exception("[%s] Синхронизация завершилась с ошибкой", instance_id)


def start_sync_async(db: Database, from_date: str, to_date: str, incremental: bool = False,
                     instance_id: str = "default"):
    t = threading.Thread(target=run_sync, args=(db, from_date, to_date, incremental, instance_id),
                         daemon=True)
    t.start()


def start_scheduler(dbs: dict, instances: list, interval_hours: int, default_period_days: int):
    """Запустить фоновую синхронизацию для всех настроенных инстансов.

    dbs       — {instance_id: Database}
    instances — [{id, name, db_path}, ...]
    """
    global _scheduler
    _scheduler = BackgroundScheduler(timezone="UTC")

    started = 0
    for inst in instances:
        iid = inst["id"]
        db = dbs.get(iid)
        if db is None:
            continue
        s = db.get_settings()
        if not (s.get("pat") and s.get("collection")):
            log.info("[%s] Планировщик пропущен: PAT/коллекция не настроены", iid)
            continue

        def _job(db=db, iid=iid):
            to_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            from_date = (datetime.now(timezone.utc) - timedelta(days=default_period_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
            run_sync(db, from_date, to_date, instance_id=iid)

        _scheduler.add_job(_job, "interval", hours=interval_hours, id=f"auto_sync_{iid}")
        started += 1

    if started:
        _scheduler.start()
        log.info("Планировщик запущен для %d инстанс(ов), интервал: %d ч", started, interval_hours)
    else:
        log.warning("Планировщик не запущен: ни один инстанс не настроен")


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        log.info("Планировщик остановлен")
