# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import time
from contextlib import contextmanager

try:
    import cPickle as pickle
except ImportError:
    import pickle

import json

from luigi.scheduler import TaskState, Scheduler, Worker
from luigi.task_status import PENDING, RUNNING

DEFAULT_LOCK_TIMEOUT = 10


class RedisScheduler(Scheduler):
    """
    Redis based scheduler. All scheduler state will be stored in redis instead of in memory
    or pickled files on startup and shudown. This should make the luigid daemon capable of
    being clustered. Care is taken to lock structures in redis to prevent issues with
    concurrent manipulation.
    """

    def __init__(self, redis, redis_prefix, **kwargs):
        """
        Initialize
        :param redis: The redis instance to connect to.
        :param redis_prefix: The redis key prefix for all scheduler keys.
        :param kwargs: Passed through to base class.
        """
        super(RedisScheduler, self).__init__(**kwargs)
        self._redis = redis
        self._redis_prefix = redis_prefix
        self._state = RedisTaskState(self)
        self._current_pipeline = None  # context local pipeline
        self._locks = set()  # context local locks

    def key(self, k):
        return self._redis_prefix + ':' + k

    @contextmanager
    def lock(self, key):
        # check if the lock is already owned by the current context. allows
        # for re-entrant locking
        if key in self._locks:
            yield
        else:
            with self._redis.lock(key, DEFAULT_LOCK_TIMEOUT):
                try:
                    self._locks.add(key)
                    yield
                finally:
                    self._locks.remove(key)

    @contextmanager
    def lock_task(self, task_id):
        # check if the lock is already owned by the current thread. allows
        # for re-entrant locking
        with self.lock(self.key('locks:task:' + task_id)):
            yield

    @contextmanager
    def pipeline(self):
        """ Re-entrant pipeline. Allows nested calls to pipelines to all execute a single
        redis pipeline for performance. """
        if self._current_pipeline is not None:
            yield self._current_pipeline
            return

        try:
            with self._redis.pipeline() as p:
                self._current_pipeline = p
                yield p
                p.execute()
        finally:
            self._current_pipeline = None

    def _prune_workers(self):
        # lock so there is only 1 prune at a time
        with self._redis.lock(self.key('locks:prune_workers'), DEFAULT_LOCK_TIMEOUT):
            super(RedisScheduler, self)._prune_workers()

    def _prune_tasks(self):
        # lock so there is only 1 prune at a time
        with self._redis.lock(self.key('locks:prune_tasks'), DEFAULT_LOCK_TIMEOUT):
            super(RedisScheduler, self)._prune_tasks()

    def add_task(self, task_id=None, *args, **kwargs):
        # lock this task id is manipulated by a single process
        with self.lock_task(task_id):
            # start a pipeline so nested pipelines are all part of this same transaction
            with self.pipeline():
                super(RedisScheduler, self).add_task(task_id, *args, **kwargs)

    def get_work(self, host=None, assistant=False, current_tasks=None, worker=None, **kwargs):
        # lock so that work can be fetched in a consistent manner
        with self._redis.lock(self.key('locks:get_work'), DEFAULT_LOCK_TIMEOUT):
            # start a pipeline so nested pipelines are all part of this same transaction
            with self.pipeline():
                return super(RedisScheduler, self).get_work(host, assistant, current_tasks, worker, **kwargs)


class RedisTaskState(TaskState):
    def __init__(self, scheduler):
        super(RedisTaskState, self).__init__()
        self._scheduler = scheduler

    def key(self, k):
        return self._scheduler.key('state:' + k)

    @property
    def redis(self):
        return self._scheduler._redis

    @contextmanager
    def pipeline(self):
        with self._scheduler.pipeline() as p:
            yield p

    def re_enable(self, task, config=None):
        with self._scheduler.lock_task(task.id):
            super(RedisTaskState, self).re_enable(task, config)

    def set_status(self, task, new_status, config=None):
        with self._scheduler.lock_task(task.id):
            super(RedisTaskState, self).set_status(task, new_status, config)

    def fail_dead_worker_task(self, task, config, assistants):
        with self._scheduler.lock_task(task.id):
            super(RedisTaskState, self).fail_dead_worker_task(task, config, assistants)

    def get_active_tasks(self):
        active_task_ids = self.redis.smembers(self.key('active_tasks'))
        return self.get_tasks(active_task_ids)

    def get_active_task_ids_by_status(self, *statuses):
        active_task_ids = set()
        for status in statuses:
            active_task_ids.update(self.redis.smembers(self.key('active_tasks:' + status)))
        return active_task_ids

    def get_active_tasks_by_status(self, *statuses):
        active_task_ids = self.get_active_task_ids_by_status(*statuses)
        return self.get_tasks(active_task_ids)

    def get_tasks(self, task_ids):
        if task_ids:
            tasks = self.redis.mget([self.key('task:' + task_id) for task_id in task_ids])
            return [pickle.loads(t) for t in tasks if t is not None]
        return []

    def save_task(self, task):
        with self.pipeline() as p:
            p.set(self.key('task:' + task.id), pickle.dumps(task))

    def set_batcher(self, worker_id, family, batcher_args, max_batch_size):
        self.redis.set(self.key('worker:' + worker_id + ':batcher:' + family),
                        pickle.dumps([batcher_args, max_batch_size]))

    def get_batcher(self, worker_id, family):
        batcher_data = self.redis.get(self.key('worker:' + worker_id + ':batcher:' + family))
        if batcher_data:
            batcher = json.loads(batcher_data)
            batcher_args, max_batch_size = batcher.get(family, (None, 1))
            return batcher_args, max_batch_size
        return None, 1

    def num_pending_tasks(self):
        return self.redis.scard(self.key('active_tasks:' + PENDING)) + \
               self.redis.scard(self.key('active_tasks:' + RUNNING))

    def get_task(self, task_id, default=None, setdefault=None):
        task = None
        tasks = self.get_tasks([task_id])
        if tasks:
            task = tasks[0]

        if task is None and setdefault:
            task = setdefault
            with self.pipeline() as p:
                p.sadd(self.key('active_tasks'), task.id)
                p.sadd(self.key('active_tasks:' + task.status), task.id)
                p.set(self.key('task:' + task_id), pickle.dumps(task))

        if task is None:
            task = default

        return task

    def has_task(self, task_id):
        return self.redis.exists(self.key('task:' + task_id))

    def change_status(self, task, new_status):
        with self._scheduler.lock_task(task.id):
            with self.pipeline() as p:
                p.srem(self.key('active_tasks:' + task.status), task.id)
                p.sadd(self.key('active_tasks:' + new_status), task.id)

    def inactivate_tasks(self, delete_task_ids):
        # The terminology is a bit confusing: we used to "delete" tasks when they became inactive,
        # but with a pluggable state storage, you might very well want to keep some history of
        # older tasks as well. That's why we call it "inactivate" (as in the verb)

        if len(delete_task_ids) == 0:
            return

        with self.redis.lock(self.key('locks:inactivate_tasks'), DEFAULT_LOCK_TIMEOUT):
            delete_tasks = self.get_tasks(delete_task_ids)
            with self.pipeline() as p:
                for task in delete_tasks:
                    p.delete(self.key('task:' + task.id))
                    p.srem(self.key('active_tasks'), task.id)
                    p.srem(self.key('active_tasks:' + task.status), task.id)

    def get_active_workers(self, last_active_lt=None, last_get_work_gt=None):
        for worker_id in self.get_worker_ids():
            worker = self.get_worker(worker_id)
            if last_active_lt is not None and worker.last_active >= last_active_lt:
                continue
            last_get_work = worker.last_get_work
            if last_get_work_gt is not None and (
                            last_get_work is None or last_get_work <= last_get_work_gt):
                continue
            yield worker

    def get_worker_ids(self):
        return self.redis.smembers(self.key('active_workers'))

    def get_worker(self, worker_id):
        self.redis.sadd(self.key('active_workers'), worker_id)
        return RedisWorker(self, worker_id=worker_id)

    def inactivate_workers(self, delete_worker_ids):
        if len(delete_worker_ids) == 0:
            return

        with self.redis.lock(self.key('locks:inactivate_workers'), DEFAULT_LOCK_TIMEOUT):
            with self.pipeline() as p:
                for worker_id in delete_worker_ids:
                    p.srem(self.key('active_workers'), worker_id)
                    p.delete(self.key('worker:' + worker_id))
                    p.delete(self.key('worker:' + worker_id + ':tasks'))
            self._remove_workers_from_tasks(delete_worker_ids)


class RedisWorker(Worker):
    def __init__(self, state, worker_id):
        super(RedisWorker, self).__init__(worker_id)
        self._state = state
        self._key = self._state.key('worker:' + self.id)

        if not self.redis.exists(self._key):
            self.redis.hmset(self._key, {
                'last_active': time.time(),
                'started': time.time(),
                'disabled': '0'
            })

    @property
    def redis(self):
        return self._state.redis

    @contextmanager
    def pipeline(self):
        with self._state.pipeline() as p:
            yield p

    def key(self, k):
        return self._key + ':' + k

    @property
    def last_active(self):
        last_active = self.redis.hget(self._key, 'last_active')
        return float(last_active) if last_active else time.time()

    @property
    def started(self):
        started = self.redis.hget(self._key, 'started')
        return float(started) if started else time.time()

    @property
    def last_get_work(self):
        last_get_work = self.redis.hget(self._key, 'last_get_work')
        return float(last_get_work) if last_get_work else time.time()

    @property
    def reference(self):
        reference_data = self.redis.hget(self._key, 'reference')
        return pickle.loads(reference_data) if reference_data else None

    @property
    def info(self):
        info_data = self.redis.hget(self._key, 'info')
        return pickle.loads(info_data) if info_data else {}

    @property
    def task_ids(self):
        return self.redis.smembers(self.key('tasks'))

    @property
    def disabled(self):
        return self.redis.hget(self._key, 'disabled') == '1'

    def add_info(self, info):
        merged_info = dict(self.info)
        merged_info.update(info)
        self.redis.hset(self._key, 'info', pickle.dumps(merged_info))

    def add_task(self, task):
        self.redis.sadd(self.key('tasks'), task.id)

    def add_rpc_message(self, name, **kwargs):
        self.redis.lpush(self.key('rpc'), pickle.dumps({'name': name, 'kwargs': kwargs}))

    def fetch_rpc_messages(self):
        return [pickle.loads(rpc) for rpc in self.redis.lrange(self.key('rpc'), 0, -1)]

    def get_tasks(self, state, *statuses):
        task_ids = self.task_ids
        task_ids_for_status = state.get_active_task_ids_by_status(*statuses)
        if len(task_ids) < len(task_ids_for_status):
            tasks = state.get_tasks(task_ids)
            return [t for t in tasks if t.status in statuses]
        else:
            tasks = state.get_tasks(task_ids_for_status)
            return [t for t in tasks if t.id in task_ids]

    def update(self, worker_reference, get_work=False):
        with self.pipeline() as p:
            p.hset(self._key, 'last_active', time.time())
            if worker_reference:
                p.hset(self._key, 'reference', pickle.dumps(worker_reference))
            if get_work:
                p.hset(self._key, 'last_get_work', time.time())

    def disable(self):
        with self.pipeline() as p:
            p.delete(self._key)
            p.hset(self._key, 'disabled', '1')
            p.delete(self.key('tasks'))
