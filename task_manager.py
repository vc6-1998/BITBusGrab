# coding=utf-8
"""
    @Author：零 若
    @file： task_manager.py
    @date：2025/10/29 21:09
    @Python  : 3.10.18
    别放弃，即使前方荆棘成林！
"""

import atexit
import json
import logging
import os
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from api_client import BusAPI

BOOKING_OPEN_LEAD_TIME = timedelta(hours=1)
PAYMENT_WINDOW = timedelta(minutes=10)
TASK_RETENTION_DAYS = 7
TASK_PERSIST_INTERVAL_SECONDS = 2
ACTIVE_TASK_STATUSES = {'pending', 'waiting', 'running'}
RESUMABLE_TASK_STATUSES = {'paused', 'cancelled', 'interrupted', 'failed'}


class Task:
    def __init__(self, task_id, bus_id, bus_info, seat_ids, auto_mode, target_count, config, seat_priorities=None):
        self.task_id = task_id
        self.bus_id = bus_id
        self.bus_info = bus_info
        self.seat_ids = seat_ids
        self.auto_mode = auto_mode
        self.target_count = target_count
        self.config = config
        self.status = 'pending'  # pending, waiting, running, paused, success, failed, cancelled, interrupted
        self.message = ''
        self.created_at = datetime.now()
        self.success_at = None
        self.start_time = None
        self.departure_time = None
        self.thread = None
        self.stop_flag = False
        self.stop_event = threading.Event()
        self.reserved_seats = []
        self.seat_priorities = seat_priorities or {}  # 座位优先级配置
        self.lock = threading.Lock()  # 用于保护 reserved_seats
        self.update_runtime_settings(config)

    def update_runtime_settings(self, config):
        self.config = config
        settings = config.get('task_settings', {})
        if not isinstance(settings, dict):
            settings = {}
        self.task_settings = dict(settings)
        self.parallel_workers = int(settings.get('max_parallel_workers', 10))
        self.auto_empty_poll_seconds = float(settings.get('auto_empty_poll_seconds', 1.0))
        self.auto_after_attempt_poll_seconds = float(
            settings.get('auto_after_attempt_poll_seconds', 0.2)
        )


class TaskManager:
    def __init__(self, database_path='tasks.sqlite3'):
        self.tasks = {}
        self.lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        self.database_path = os.path.abspath(database_path)
        self.persistence_stop = threading.Event()
        self._initialize_storage()
        self._load_persisted_tasks()
        self.persistence_thread = threading.Thread(
            target=self._persistence_loop,
            daemon=True,
            name='task-persistence',
        )
        self.persistence_thread.start()
        atexit.register(self._persist_all_tasks)

    def _initialize_storage(self):
        directory = os.path.dirname(self.database_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with sqlite3.connect(self.database_path) as database:
            database.execute(
                'CREATE TABLE IF NOT EXISTS tasks ('
                'task_id TEXT PRIMARY KEY, '
                'created_at TEXT NOT NULL, '
                'departure_time TEXT, '
                'payload TEXT NOT NULL'
                ')'
            )

    @staticmethod
    def _parse_datetime(value):
        if isinstance(value, datetime):
            return value
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _format_datetime(value):
        return value.strftime('%Y-%m-%d %H:%M:%S') if value else ''

    def _notification_context_lines(self, task):
        """构造所有通知共用的班次、任务模式和座位信息。"""
        with task.lock:
            reserved_seats = list(task.reserved_seats)

        route = (
            f"{task.bus_info.get('origin_address', '未知')} → "
            f"{task.bus_info.get('end_address', '未知')}"
        )
        departure_time = (
            self._format_datetime(task.departure_time)
            or task.bus_info.get('origin_time', '未知')
        )
        if task.auto_mode:
            mode = f"自动模式（目标 {task.target_count} 个）"
        else:
            selected_seats = ', '.join(map(str, task.seat_ids)) or '无'
            mode = f"手动模式（选择座位：{selected_seats}）"

        return [
            f"路线：{route}",
            f"发车时间：{departure_time}",
            f"任务模式：{mode}",
            f"已预订座位：{', '.join(map(str, reserved_seats)) or '无'}",
        ]

    def _success_notification_message(self, task, extra_lines=None):
        """构造统一格式的成功通知，并附上支付截止时间。"""
        payment_deadline = task.success_at + PAYMENT_WINDOW
        lines = self._notification_context_lines(task) + [
            "结果：抢票成功",
            f"成功时间：{self._format_datetime(task.success_at)}",
            f"请于 {self._format_datetime(payment_deadline)} 前完成支付（成功后10分钟内）",
        ]
        if extra_lines:
            lines.extend(extra_lines)
        return '\n'.join(lines)

    def _failure_notification_message(
        self,
        task,
        outcome,
        reason,
        failure_details=None,
        details_omitted=False,
    ):
        """构造包含班次上下文、结束时间和失败原因的通知。"""
        lines = self._notification_context_lines(task) + [
            f"结果：{outcome}",
            f"结束时间：{self._format_datetime(datetime.now())}",
            f"原因：{reason}",
        ]
        if failure_details:
            lines.extend(f"失败详情：{detail}" for detail in failure_details)
        if details_omitted:
            lines.append('其他失败详情已省略')
        return '\n'.join(lines)

    def _progress_notification_message(self, task):
        """构造自动模式部分成功时的进度通知。"""
        with task.lock:
            reserved_count = len(task.reserved_seats)
        lines = self._notification_context_lines(task)
        lines.append(f"预订进度：已抢到 {reserved_count}/{task.target_count} 个")
        return '\n'.join(lines)

    @staticmethod
    def _remember_failure_detail(details, seat_id, reason, limit=5):
        """保存少量不同的座位失败原因，避免长时间自动任务积累过多文本。"""
        reason_text = str(reason).replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ').strip()
        detail = f"座位 {seat_id}：{reason_text or '未知原因'}"
        if detail in details:
            return False
        if len(details) >= limit:
            return True
        details.append(detail)
        return False

    def _task_payload(self, task):
        with task.lock:
            reserved_seats = list(task.reserved_seats)
        return {
            'task_id': task.task_id,
            'bus_id': task.bus_id,
            'bus_info': task.bus_info,
            'seat_ids': task.seat_ids,
            'auto_mode': task.auto_mode,
            'target_count': task.target_count,
            'seat_priorities': task.seat_priorities,
            'task_settings': task.task_settings,
            'status': task.status,
            'message': task.message,
            'created_at': self._format_datetime(task.created_at),
            'success_at': self._format_datetime(task.success_at),
            'start_time': self._format_datetime(task.start_time),
            'departure_time': self._format_datetime(task.departure_time),
            'reserved_seats': reserved_seats,
        }

    def _write_payload(self, payload):
        created_at = payload.get('created_at') or self._format_datetime(datetime.now())
        departure_time = payload.get('departure_time') or None
        with sqlite3.connect(self.database_path, timeout=10) as database:
            database.execute(
                'INSERT OR REPLACE INTO tasks (task_id, created_at, departure_time, payload) '
                'VALUES (?, ?, ?, ?)',
                (
                    payload['task_id'],
                    created_at,
                    departure_time,
                    json.dumps(payload, ensure_ascii=False),
                ),
            )

    def _persist_task(self, task):
        with self.lock:
            if self.tasks.get(task.task_id) is not task:
                return
            self._write_payload(self._task_payload(task))

    def _persist_all_tasks(self):
        with self.lock:
            tasks = list(self.tasks.values())
        for task in tasks:
            try:
                self._persist_task(task)
            except (OSError, sqlite3.Error, TypeError, ValueError):
                logging.exception('Failed to persist task %s', task.task_id)

    def _cleanup_expired_tasks(self):
        cutoff = datetime.now() - timedelta(days=TASK_RETENTION_DAYS)
        with self.lock:
            with sqlite3.connect(self.database_path, timeout=10) as database:
                rows = database.execute(
                    'SELECT task_id, created_at, departure_time FROM tasks'
                ).fetchall()
                expired_ids = []
                for task_id, created_at, departure_time in rows:
                    reference_time = (
                        self._parse_datetime(departure_time)
                        or self._parse_datetime(created_at)
                    )
                    if reference_time and reference_time < cutoff:
                        expired_ids.append(task_id)
                database.executemany(
                    'DELETE FROM tasks WHERE task_id = ?',
                    ((task_id,) for task_id in expired_ids),
                )
            for task_id in expired_ids:
                self.tasks.pop(task_id, None)

    def _load_persisted_tasks(self):
        self._cleanup_expired_tasks()
        with sqlite3.connect(self.database_path, timeout=10) as database:
            rows = database.execute(
                'SELECT task_id, payload FROM tasks ORDER BY created_at ASC'
            ).fetchall()

        invalid_ids = []
        for task_id, raw_payload in rows:
            try:
                payload = json.loads(raw_payload)
                if not isinstance(payload, dict):
                    raise ValueError('任务记录格式无效')
                bus_info = payload.get('bus_info') or {}
                if not isinstance(bus_info, dict):
                    raise ValueError('班车信息格式无效')
                task = Task(
                    task_id=task_id,
                    bus_id=payload.get('bus_id'),
                    bus_info=bus_info,
                    seat_ids=payload.get('seat_ids') or [],
                    auto_mode=bool(payload.get('auto_mode')),
                    target_count=payload.get('target_count', 1),
                    config={'task_settings': payload.get('task_settings') or {}},
                    seat_priorities=payload.get('seat_priorities') or {},
                )
                task.created_at = self._parse_datetime(payload.get('created_at')) or task.created_at
                task.success_at = self._parse_datetime(payload.get('success_at'))
                task.start_time = self._parse_datetime(payload.get('start_time'))
                task.departure_time = self._parse_datetime(payload.get('departure_time'))
                task.reserved_seats = payload.get('reserved_seats') or []
                task.status = payload.get('status', 'failed')
                task.message = payload.get('message', '')

                if task.status in ACTIVE_TASK_STATUSES:
                    task.status = 'interrupted'
                    task.message = '程序关闭时任务中断，可在发车前重新启动'

                self.tasks[task_id] = task
            except (TypeError, ValueError, json.JSONDecodeError):
                invalid_ids.append(task_id)
                logging.exception('Failed to restore task %s', task_id)

        if invalid_ids:
            with sqlite3.connect(self.database_path, timeout=10) as database:
                database.executemany(
                    'DELETE FROM tasks WHERE task_id = ?',
                    ((task_id,) for task_id in invalid_ids),
                )

        self._persist_all_tasks()

    def _persistence_loop(self):
        last_cleanup = time.monotonic()
        while not self.persistence_stop.wait(TASK_PERSIST_INTERVAL_SECONDS):
            self._persist_all_tasks()
            if time.monotonic() - last_cleanup >= 3600:
                try:
                    self._cleanup_expired_tasks()
                except sqlite3.Error:
                    logging.exception('Failed to clean up expired tasks')
                last_cleanup = time.monotonic()

    def create_task(self, bus_id, bus_info, seat_ids, auto_mode, target_count, config, seat_priorities=None):
        """创建任务"""
        task_id = str(uuid.uuid4())
        task = Task(task_id, bus_id, bus_info, seat_ids, auto_mode, target_count, config, seat_priorities)

        # 班车服务固定在发车前一小时开放预约。
        try:
            origin_time_str = bus_info.get('origin_time', '')  # 格式：HH:MM
            date_str = bus_info.get('date', '')  # 格式：YYYY-MM-DD

            if not date_str:
                # 如果没有日期，使用当前日期
                date_str = datetime.now().strftime('%Y-%m-%d')

            if not origin_time_str:
                raise ValueError('未找到发车时间')

            # 组合日期和时间
            full_datetime_str = f"{date_str} {origin_time_str}"

            # 解析为 datetime 对象
            origin_datetime = datetime.strptime(full_datetime_str, '%Y-%m-%d %H:%M')

            task.departure_time = origin_datetime
            task.start_time = origin_datetime - BOOKING_OPEN_LEAD_TIME

            # 如果开抢时间已过，立即开始
            if task.start_time <= datetime.now():
                task.start_time = datetime.now()
                task.message = '立即开始抢票'
            else:
                time_diff = task.start_time - datetime.now()
                hours = int(time_diff.total_seconds() // 3600)
                minutes = int((time_diff.total_seconds() % 3600) // 60)

                if hours > 0:
                    task.message = f'等待开抢（还需 {hours}小时{minutes}分钟）'
                else:
                    task.message = f'等待开抢（还需 {minutes}分钟）'

        except Exception as e:
            # 如果时间解析失败，立即开始
            task.start_time = datetime.now()
            task.message = f'时间解析失败，立即开始: {str(e)}'

        with self.lock:
            self.tasks[task_id] = task

        try:
            self._persist_task(task)
        except Exception:
            with self.lock:
                self.tasks.pop(task_id, None)
            raise

        # 启动任务线程
        thread = threading.Thread(target=self._run_task, args=(task,))
        thread.daemon = True
        task.thread = thread
        thread.start()

        return task_id

    def resume_task(self, task_id, config):
        """在发车时间前重新启动已暂停、失败或中断的任务。"""
        with self.lock:
            task = self.tasks.get(task_id)
            if task is None:
                return False, '任务不存在或已清理'
            if task.status not in RESUMABLE_TASK_STATUSES:
                return False, '当前任务状态不能重新启动'
            if task.departure_time is None:
                return False, '无法确认发车时间，不能重新启动'
            if task.thread and task.thread.is_alive():
                return False, '任务仍在停止中，请稍后再试'
            if datetime.now() >= task.departure_time:
                task.status = 'failed'
                task.message = '发车时间已过，不能重新启动任务'
                self._write_payload(self._task_payload(task))
                return False, task.message
            if not task.auto_mode and not task.seat_ids:
                return False, '任务没有保存选定的座位'
            if not config.get('USER_ID'):
                return False, '请先登录班车服务'
            if task.start_time is None:
                task.start_time = max(
                    datetime.now(),
                    task.departure_time - BOOKING_OPEN_LEAD_TIME,
                )

            previous_state = (
                task.config,
                task.status,
                task.message,
                task.stop_flag,
                task.stop_event,
                task.thread,
            )
            restart_config = dict(config)
            restart_config['task_settings'] = dict(task.task_settings)
            task.update_runtime_settings(restart_config)
            task.stop_flag = False
            task.stop_event = threading.Event()
            task.status = 'pending'
            task.message = '正在重新启动任务...'
            thread = threading.Thread(target=self._run_task, args=(task,), daemon=True)
            task.thread = thread
            try:
                self._write_payload(self._task_payload(task))
                thread.start()
            except Exception as error:
                (
                    task.config,
                    task.status,
                    task.message,
                    task.stop_flag,
                    task.stop_event,
                    task.thread,
                ) = previous_state
                task.update_runtime_settings(task.config)
                try:
                    self._write_payload(self._task_payload(task))
                except Exception:
                    logging.exception('Failed to restore task %s after restart error', task_id)
                return False, f'重新启动失败: {error}'

        return True, '任务已重新启动'

    def _run_task(self, task):
        """运行任务"""
        api = None
        execution_started_at = None
        route = (
            f"{task.bus_info.get('origin_address', '未知')} → "
            f"{task.bus_info.get('end_address', '未知')}"
        )
        departure = (
            self._format_datetime(task.departure_time)
            or task.bus_info.get('origin_time', '未知')
        )
        mode = '自动' if task.auto_mode else '手动'
        selected_seats = ', '.join(map(str, task.seat_ids)) or '无'
        self.logger.info(
            'Task worker started (task_id=%s, bus_id=%s, route=%s, departure=%s, '
            'mode=%s, target_count=%s, selected_seats=%s)',
            task.task_id,
            task.bus_id,
            route,
            departure,
            mode,
            task.target_count,
            selected_seats,
        )
        try:
            api = BusAPI(task.config)

            if task.auto_mode and task.departure_time is None:
                task.status = 'failed'
                task.message = '无法解析发车时间，自动模式无法确定任务截止时间'
                api.send_notification(
                    '❌ 抢票失败',
                    self._failure_notification_message(task, '抢票失败', task.message),
                )
                return

            # 等待到开抢时间
            task.status = 'waiting'
            while datetime.now() < task.start_time and not task.stop_flag:
                time_diff = task.start_time - datetime.now()
                minutes = int(time_diff.total_seconds() / 60)
                seconds = int(time_diff.total_seconds() % 60)
                task.message = f'等待开抢（{minutes}分{seconds}秒后开始）'
                time.sleep(1)

            if task.stop_flag:
                task.status = 'paused'
                task.message = '任务已暂停'
                return

            task.status = 'running'
            task.message = '正在抢票...'
            execution_started_at = datetime.now()
            self.logger.info(
                'Task execution started (task_id=%s, bus_id=%s, mode=%s)',
                task.task_id,
                task.bus_id,
                mode,
            )

            # 获取日期信息
            date = task.bus_info.get('date')
            if not date:
                # 尝试从 origin_time 提取日期
                origin_time = task.bus_info.get('origin_time', '')
                if ' ' in origin_time:
                    date = origin_time.split()[0]
                else:
                    date = datetime.now().strftime('%Y-%m-%d')

            if task.auto_mode:
                # 自动模式：并行抢票
                self._parallel_auto_reserve(task, date)
            else:
                # 手动模式：并行抢指定座位
                self._parallel_manual_reserve(task, date)

        except Exception as e:
            task.status = 'failed'
            task.message = f'抢票失败: {str(e)}'
            self.logger.exception(
                'Task failed (task_id=%s, bus_id=%s)', task.task_id, task.bus_id
            )
            if api:
                api.send_notification(
                    '⚠️ 抢票异常',
                    self._failure_notification_message(task, '任务异常', str(e)),
                )
        finally:
            if api:
                api.close()
            with task.lock:
                reserved_seats = ', '.join(map(str, task.reserved_seats)) or '无'
            execution_duration = (
                f'{(datetime.now() - execution_started_at).total_seconds():.2f}s'
                if execution_started_at else 'not-started'
            )
            summary = ' '.join((task.message or '').split())[:300] or '无'
            log_method = self.logger.warning if task.status == 'failed' else self.logger.info
            log_method(
                'Task finished (task_id=%s, bus_id=%s, route=%s, departure=%s, '
                'status=%s, reserved_seats=%s, execution_duration=%s, result=%s)',
                task.task_id,
                task.bus_id,
                route,
                departure,
                task.status,
                reserved_seats,
                execution_duration,
                summary,
            )
            self._persist_task(task)

    def _reserve_seat_worker(self, task, seat_id, date):
        """
        单个座位预订工作函数（用于并行执行）

        Returns:
            (success: bool, seat_id: int, message: str)
        """
        try:
            # 每个工作线程创建自己的 API 客户端
            api = BusAPI(task.config)

            result = api.reserve_seat(task.bus_id, seat_id, date)

            api.close()

            if result.get('success'):
                return True, seat_id, '预订成功'
            else:
                error = result.get('error', '未知错误')
                self.logger.warning(
                    'Seat reservation failed (task_id=%s, bus_id=%s, seat_id=%s, date=%s): %s',
                    task.task_id,
                    task.bus_id,
                    seat_id,
                    date,
                    error,
                )
                return False, seat_id, error

        except Exception as e:
            self.logger.exception(
                'Seat reservation raised an exception (task_id=%s, bus_id=%s, seat_id=%s, date=%s)',
                task.task_id,
                task.bus_id,
                seat_id,
                date,
            )
            return False, seat_id, str(e)

    @staticmethod
    def _wait_auto_poll(task, interval):
        """等待下一次自动查询，同时响应取消并避免睡过发车时刻。"""
        remaining = (task.departure_time - datetime.now()).total_seconds()
        wait_seconds = min(interval, max(0, remaining))
        if wait_seconds > 0:
            task.stop_event.wait(wait_seconds)

    def _parallel_auto_reserve(self, task, date):
        """
        并行自动抢票模式
        使用线程池同时尝试多个座位
        """
        api = BusAPI(task.config)
        with task.lock:
            reserved_count = len(task.reserved_seats)
        round_count = 0
        tried_seats = set(task.reserved_seats)
        failure_details = []
        failure_details_omitted = False

        try:
            while (
                reserved_count < task.target_count
                and not task.stop_flag
                and datetime.now() < task.departure_time
            ):
                round_count += 1

                # 获取座位信息
                try:
                    seats_info = api.get_seats(task.bus_id, date)
                except Exception as error:
                    if '未开启预约' not in str(error) and '预约尚未开放' not in str(error):
                        raise
                    task.message = '班车预约暂未开放，等待后继续检查...'
                    self._wait_auto_poll(task, task.auto_empty_poll_seconds)
                    continue

                if datetime.now() >= task.departure_time:
                    break

                # 筛选可用且未尝试过的座位
                available_seats = [
                    s['id'] for s in seats_info.get('seats', [])
                    if s.get('status') == 'available' and s['id'] not in tried_seats
                ]

                if not available_seats:
                    task.message = f'暂无可用座位，第 {round_count} 轮尝试...'

                    # 每 10 轮重置已尝试座位
                    if round_count % 10 == 0:
                        tried_seats.clear()
                        task.message = f'重置尝试记录，继续抢票（第 {round_count} 轮）'

                    self._wait_auto_poll(task, task.auto_empty_poll_seconds)
                    continue

                # 按优先级排序
                def get_priority(seat_id):
                    return task.seat_priorities.get(str(seat_id), 2)

                available_seats.sort(key=get_priority)

                # 取前 N 个座位进行并行抢票（N = min(并行数, 剩余需要数量, 可用座位数)）
                remaining = task.target_count - reserved_count
                batch_size = min(task.parallel_workers, remaining, len(available_seats))
                seats_to_try = available_seats[:batch_size]

                task.message = f'第 {round_count} 轮：并行尝试 {batch_size} 个座位... (已抢 {reserved_count}/{task.target_count})'

                # 使用线程池并行抢票
                with ThreadPoolExecutor(max_workers=batch_size) as executor:
                    futures = {
                        executor.submit(self._reserve_seat_worker, task, seat_id, date): seat_id
                        for seat_id in seats_to_try
                    }

                    for future in as_completed(futures):
                        seat_id = futures[future]
                        tried_seats.add(seat_id)

                        try:
                            success, sid, message = future.result(timeout=5)

                            if success:
                                send_progress_notification = False
                                with task.lock:
                                    if reserved_count < task.target_count:
                                        reserved_count += 1
                                        task.reserved_seats.append(sid)

                                        priority = get_priority(sid)
                                        priority_text = {1: '高', 2: '中', 3: '低'}.get(priority, '中')

                                        task.message = f'✅ 座位 {sid} 预订成功！(优先级:{priority_text}) 已抢 {reserved_count}/{task.target_count}'
                                        send_progress_notification = reserved_count < task.target_count
                                if send_progress_notification:
                                    api.send_notification(
                                        '✅ 座位预订成功',
                                        self._progress_notification_message(task),
                                    )
                            else:
                                task.message = f'❌ 座位 {sid} 失败: {message}'
                                failure_details_omitted = (
                                    self._remember_failure_detail(failure_details, sid, message)
                                    or failure_details_omitted
                                )

                        except Exception as e:
                            self.logger.exception(
                                'Seat reservation worker failed (task_id=%s, bus_id=%s, seat_id=%s)',
                                task.task_id,
                                task.bus_id,
                                seat_id,
                            )
                            task.message = f'❌ 座位 {seat_id} 异常: {str(e)}'
                            failure_details_omitted = (
                                self._remember_failure_detail(failure_details, seat_id, str(e))
                                or failure_details_omitted
                            )

                # 检查是否已完成
                if reserved_count >= task.target_count:
                    break

                # 等待后继续下一轮
                self._wait_auto_poll(task, task.auto_after_attempt_poll_seconds)

            # 任务完成判断
            if reserved_count == task.target_count:
                task.status = 'success'
                task.success_at = datetime.now()
                task.message = f'🎉 抢票成功！已预定座位: {", ".join(map(str, task.reserved_seats))}'
                api.send_notification(
                    '🎉 抢票成功',
                    self._success_notification_message(task),
                )
            elif task.stop_flag:
                task.status = 'paused'
                task.message = '任务已暂停'
            else:
                task.status = 'failed'
                task.message = f'已到发车时刻，自动抢票结束（检查 {round_count} 轮）'
                if task.reserved_seats:
                    task.message += f'\n✅ 已成功抢到部分座位: {", ".join(map(str, task.reserved_seats))}'
                reason = (
                    f'已到发车时刻，检查 {round_count} 轮，'
                    f'目标 {task.target_count} 个，实际预订 {reserved_count} 个。'
                )
                api.send_notification(
                    '⚠️ 抢票未完成',
                    self._failure_notification_message(
                        task,
                        '未达到目标',
                        reason,
                        failure_details,
                        failure_details_omitted,
                    ),
                )

        finally:
            api.close()

    def _parallel_manual_reserve(self, task, date):
        """
        并行手动抢票模式
        同时尝试所有指定座位
        """
        api = BusAPI(task.config)
        success_seats = []
        failed_seats = []
        failure_details = []
        failure_details_omitted = False

        try:
            if task.stop_flag:
                task.status = 'paused'
                task.message = '任务已暂停'
                return

            with task.lock:
                success_seats = list(task.reserved_seats)
            reserved_ids = {str(seat_id) for seat_id in success_seats}
            seats_to_try = [
                seat_id for seat_id in task.seat_ids
                if str(seat_id) not in reserved_ids
            ]
            task.message = f'并行抢票：同时尝试 {len(seats_to_try)} 个座位...'

            # 使用线程池并行抢所有指定座位
            with ThreadPoolExecutor(max_workers=max(1, min(len(seats_to_try), task.parallel_workers))) as executor:
                futures = {
                    executor.submit(self._reserve_seat_worker, task, seat_id, date): seat_id
                    for seat_id in seats_to_try
                }

                for future in as_completed(futures):
                    seat_id = futures[future]

                    try:
                        success, sid, message = future.result(timeout=5)

                        if success:
                            with task.lock:
                                success_seats.append(sid)
                                task.reserved_seats.append(sid)
                            task.message = f'✅ 座位 {sid} 预订成功！'
                        else:
                            failed_seats.append(sid)
                            task.message = f'❌ 座位 {sid} 失败: {message}'
                            failure_details_omitted = (
                                self._remember_failure_detail(failure_details, sid, message)
                                or failure_details_omitted
                            )

                    except Exception as e:
                        self.logger.exception(
                            'Manual seat reservation worker failed (task_id=%s, bus_id=%s, seat_id=%s)',
                            task.task_id,
                            task.bus_id,
                            seat_id,
                        )
                        failed_seats.append(seat_id)
                        task.message = f'❌ 座位 {seat_id} 异常: {str(e)}'
                        failure_details_omitted = (
                            self._remember_failure_detail(failure_details, seat_id, str(e))
                            or failure_details_omitted
                        )

            # 任务完成判断
            if task.stop_flag:
                task.status = 'paused'
                task.message = '任务已暂停'
                if success_seats:
                    task.message += f'；已成功预订座位: {", ".join(map(str, success_seats))}'
            elif success_seats:
                task.status = 'success'
                task.success_at = datetime.now()
                task.message = f'🎉 抢票成功！已预定座位: {", ".join(map(str, success_seats))}'
                if failed_seats:
                    task.message += f'\n⚠️ 未能预订: {", ".join(map(str, failed_seats))}'
                extra_lines = []
                if failed_seats:
                    extra_lines.append(f"未能预订座位：{', '.join(map(str, failed_seats))}")
                    extra_lines.extend(f"失败详情：{detail}" for detail in failure_details)
                    if failure_details_omitted:
                        extra_lines.append('其他失败详情已省略')
                api.send_notification(
                    '🎉 抢票成功',
                    self._success_notification_message(task, extra_lines),
                )
            else:
                task.status = 'failed'
                task.message = f'❌ 所选座位均未抢到: {", ".join(map(str, failed_seats))}'
                reason = f'所选座位均未预订成功：{", ".join(map(str, failed_seats)) or "无"}'
                api.send_notification(
                    '❌ 抢票失败',
                    self._failure_notification_message(
                        task,
                        '全部座位预订失败',
                        reason,
                        failure_details,
                        failure_details_omitted,
                    ),
                )

        finally:
            api.close()

    def get_all_tasks(self):
        """获取所有任务"""
        now = datetime.now()
        with self.lock:
            tasks = []
            for task in self.tasks.values():
                payment_deadline = (
                    task.success_at + PAYMENT_WINDOW if task.success_at else None
                )
                tasks.append({
                    'task_id': task.task_id,
                    'bus_id': task.bus_id,
                    'bus_info': {
                        'origin_address': task.bus_info.get('origin_address', ''),
                        'end_address': task.bus_info.get('end_address', ''),
                        'origin_time': task.bus_info.get('origin_time', '')
                    },
                    'departure_time': self._format_datetime(task.departure_time),
                    'seat_ids': list(task.seat_ids),
                    'status': task.status,
                    'message': task.message,
                    'created_at': task.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                    'success_at': self._format_datetime(task.success_at),
                    'payment_deadline_at': self._format_datetime(payment_deadline),
                    'payment_deadline_passed': bool(payment_deadline and now >= payment_deadline),
                    'is_departed': bool(task.departure_time and now >= task.departure_time),
                    'start_time': task.start_time.strftime('%Y-%m-%d %H:%M:%S') if task.start_time else '',
                    'reserved_seats': task.reserved_seats,
                    'target_count': task.target_count,
                    'auto_mode': task.auto_mode,
                    'can_resume': (
                        task.status in RESUMABLE_TASK_STATUSES
                        and task.departure_time is not None
                        and datetime.now() < task.departure_time
                        and not (task.thread and task.thread.is_alive())
                    )
                })

            def sort_key(item):
                departure_time = self._parse_datetime(item['departure_time'])
                if departure_time is None:
                    return 2, 0
                if departure_time >= now:
                    return 0, departure_time
                return 1, -departure_time.timestamp()

            return sorted(tasks, key=sort_key)

    def delete_task(self, task_id):
        """删除任务"""
        with self.lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                task.stop_flag = True
                task.stop_event.set()
                with sqlite3.connect(self.database_path, timeout=10) as database:
                    database.execute('DELETE FROM tasks WHERE task_id = ?', (task_id,))
                del self.tasks[task_id]

    def cancel_task(self, task_id):
        """暂停任务，保留配置以便之后重新启动。"""
        with self.lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                task.stop_flag = True
                task.stop_event.set()
                task.status = 'paused'
                task.message = '任务已暂停'
            else:
                task = None
        if task:
            self._persist_task(task)
