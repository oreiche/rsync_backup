#!/usr/bin/env python3
#
# (c) 2021, Oliver Reiche <oliver.reiche@gmail.com>

# TODO:
#  - Test windows permissions when copying symlinks
#  - Add tests

import argparse
import errno
import json
import multiprocessing
import os
import platform
import shutil
import stat
import sys
import threading
import time
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor as executor
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Set, Optional, Tuple, Union, cast

INDENT_WIDTH = 2
PLATFORM_WINDOWS = platform.system() == 'Windows'

if 'Microsoft' in platform.uname().release:
    # monkey patch shutil in WSL (see https://bugs.python.org/issue38633)
    orig_copyxattr = shutil._copyxattr

    def patched_copyxattr(src, dst, *, follow_symlinks=True):
        try:
            orig_copyxattr(src, dst, follow_symlinks=follow_symlinks)
        except OSError as ex:
            if ex.errno != errno.EACCES: raise

    shutil._copyxattr = patched_copyxattr


class AtomicInt:
    def __init__(self):
        self.__lock = threading.Lock()
        self.__value = int(0)

    @property
    def value(self) -> int:
        with self.__lock:
            return self.__value

    @value.setter
    def value(self, to: int):
        with self.__lock:
            self.__value = to

    def increment(self, by: int = 1):
        with self.__lock:
            self.__value += by

    def get_and_set(self, to: int) -> int:
        with self.__lock:
            val = self.__value
            self.__value = to
        return val

    def get_and_inc(self, by: int = 1) -> int:
        with self.__lock:
            val = self.__value
            self.__value += by
        return val


class TaskSystem:
    """Simple queue-based task system."""
    Task = Tuple[Callable[..., None], Tuple[Any, ...], Dict[str, Any]]

    class RuntimeError(Exception):
        def __init__(self, msg: str):
            super().__init__(f'TaskSystem runtime error:\n{msg}')

    class Queue:
        def __init__(self):
            self.worker_idle = True
            self.cv = threading.Condition()
            self.tasks: List[TaskSystem.Task] = []

    def __init__(self,
                 *,
                 max_workers: int = multiprocessing.cpu_count(),
                 max_retries: int = 4,
                 queue_limit: int = 32):
        """Creates the task system with `max_workers` many threads and queues.
        Adding tasks will try-lock each queue `max_retries` many times before
        blocking. Adding tasks conditionally is subject to a maximum
        `queue_limit`."""
        self.__shutdown = False
        self.__num_workers = max(1, max_workers)
        self.__num_retries = max(0, max_retries)
        self.__queue_limit = max(1, queue_limit)
        self.__current_idx = AtomicInt()
        self.__qs = [TaskSystem.Queue() for _ in range(self.__num_workers)]
        self.__exceptions: List[Optional[Exception]] = [
            None for _ in range(self.__num_workers)
        ]

        def run(q: TaskSystem.Queue, idx: int):
            try:
                while not self.__shutdown:
                    task: Optional[TaskSystem.Task] = None
                    with q.cv:
                        if len(q.tasks) == 0:
                            q.worker_idle = True
                            q.cv.notify_all()
                            q.cv.wait_for(
                                lambda: len(q.tasks) > 0 or self.__shutdown)
                        if len(q.tasks) > 0:
                            q.worker_idle = False
                            task = q.tasks.pop(0)
                            q.cv.notify_all()
                    if task:
                        task[0](*task[1], **task[2])
            except Exception as e:
                self.__exceptions[idx] = TaskSystem.RuntimeError(
                    f'Thread {idx} exited with error:\n{e}')
                self.__shutdown = True
            finally:
                with q.cv:
                    q.cv.notify_all()

        self.__workers = [
            threading.Thread(target=run, args=(self.__qs[i], i))
            for i in range(self.__num_workers)
        ]
        for w in self.__workers:
            w.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, exc_traceback: Any):
        try:
            self.finish()
        finally:
            self.shutdown()

    def __try_add(self, idx: int, t: Task, honor_limit: bool = False) -> bool:
        """Try non-blocking add task. Returns `False` if task was not added."""
        end = idx + (self.__num_workers * (1 + self.__num_retries))
        for q in [self.__qs[i % self.__num_workers] for i in range(idx, end)]:
            if self.__shutdown: return False
            if q.cv.acquire(blocking=False):
                try:
                    if not honor_limit or len(q.tasks) < self.__queue_limit:
                        q.tasks.append(t)
                        q.cv.notify_all()
                        return True
                finally:
                    q.cv.release()
        return False

    def add(self, fn: Callable[..., None], *args: Any, **kw: Any):
        """Add task to task queue unconditionally. Might block."""
        idx = self.__current_idx.get_and_inc()
        if not self.__shutdown and not self.__try_add(idx, (fn, args, kw)):
            q = self.__qs[idx % self.__num_workers]
            with q.cv:  # force add
                q.tasks.append((fn, args, kw))
                q.cv.notify_all()

    def add_or_run(self, fn: Callable[..., None], *args: Any, **kw: Any):
        """Add task conditionally (queue limit is not reached) or run it."""
        idx = self.__current_idx.get_and_inc()
        if not self.__shutdown and not self.__try_add(idx, (fn, args, kw),
                                                      honor_limit=True):
            fn(*args, **kw)  # run it

    def finish(self):
        """Wait for finish (all queues empty and workers idle) or shutdown."""
        all_finished = False
        while not self.__shutdown and not all_finished:
            all_finished = True
            cur_idx = self.__current_idx.get_and_inc()
            cur_q = self.__qs[cur_idx % self.__num_workers]
            remaining_qs = [
                self.__qs[i % self.__num_workers]
                for i in range(cur_idx + 1, cur_idx + self.__num_workers)
            ]
            acquired_qs: List[TaskSystem.Queue] = []
            with cur_q.cv:  # lock current q and immediately wait for finish
                cur_q.cv.wait_for(lambda: self.__shutdown or (
                    cur_q.worker_idle and len(cur_q.tasks) == 0))
                try:
                    for q in remaining_qs:  # try_lock and check remaining qs
                        if q.cv.acquire(blocking=False):
                            acquired_qs.append(q)
                            if not (q.worker_idle and len(q.tasks) == 0):
                                all_finished = False
                                break
                        else:
                            all_finished = False
                            break
                except Exception as e:
                    raise TaskSystem.RuntimeError(
                        f'Finish failed with error:\n{e}')
                finally:
                    for q in acquired_qs:
                        q.cv.release()

    def shutdown(self):
        """Initiate shutdown of task system and wait for all threads to stop."""
        self.__shutdown = True  # signal shutdown
        for q in self.__qs:  # notify everyone about shutdown
            with q.cv:
                q.cv.notify_all()
        for w in self.__workers:  # wait for workers to shutdown
            w.join()
        for e in self.__exceptions:
            if e != None: raise e

    def running(self) -> bool:
        """Returns `True` if at least one thread is alive."""
        for w in self.__workers:
            if w.is_alive(): return True
        return False


# Type for tracking file changes, non-None in tuple indicates directory:
# { "foo": (CreateNode, None),
#   "dir": (UpdateStat,
#       { "bar": (UpdateNode, None)})}
FileChanges = Dict[str, Tuple['FileSystem.ChangeType',
                              Optional['FileChanges']]]


class FileSystem:
    """Common file system operations and sync/rmtree with progress."""
    class ChangeType(Enum):
        NoChange = 0
        RemoveNode = 1
        CreateNode = 2  # remove existing target node
        UpdateNode = 3  # update existing target node (node types must match)
        UpdateStat = 4

    class ProgressPrinter:
        """Print progress as bar or rotating indicator."""
        def __init__(self, *, bar_length: int = 40, indent: int = 0):
            self.__bar_length = bar_length
            self.__ind_idx = 0
            self.__ind_chars = ['-', '\\', '|', '/']
            self.__ind_value = 0
            self.__indent = ' ' * indent * INDENT_WIDTH

        def __indicator(self, msg: str, val: int, *, flush: bool = False):
            if self.__ind_idx == 0: self.__ind_value = val - 1
            if flush: token = "done!"
            else: token = self.__ind_chars[self.__ind_idx]
            print(f"{self.__indent + msg} {token}",
                  end='\r' if not flush else '\n')
            if self.__ind_value != val:
                self.__ind_idx = (self.__ind_idx + 1) % len(self.__ind_chars)
            self.__ind_value = val

        def __bar(self, msg: str, val: int, end: int, *, flush: bool = False):
            if not end: return
            fract = int((val / end) * self.__bar_length)
            bar_str = f'{msg}: [{"#"*fract}{"-"*(self.__bar_length - fract)}]'
            num_str = f'{val}/{end} {((val/end)*100):.2f}%'
            print(f'{self.__indent + bar_str} {num_str}',
                  end='\r' if not flush else '\n')

        def show_indicator(self, msg: str, ts: TaskSystem, val: AtomicInt):
            """While `ts` is running, print rotating indicator for `val`."""
            while ts.running():
                self.__indicator(msg, val.value)
                time.sleep(0.1)
            self.__indicator(msg, val.value, flush=True)

        def show_bar(self, msg: str, ts: TaskSystem, val: AtomicInt, end: int):
            """While `ts` is running, print progress bar for `val` to `end`."""
            while ts.running():
                self.__bar(msg, val.value, end)
                time.sleep(0.1)
            self.__bar(msg, val.value, end, flush=True)

    def __init__(self,
                 *,
                 jobs: int = multiprocessing.cpu_count(),
                 save_memory: bool = False):
        """Using `save_memory` is slightly slower but prevents sync/rmtree from
        building up large file trees in memory."""
        self.__jobs = jobs
        self.__save_memory = save_memory
        self.__fd_api_support = (
            {os.open, os.stat, os.unlink, os.rmdir} <= os.supports_dir_fd
            and os.scandir in os.supports_fd
            and os.stat in os.supports_follow_symlinks)

    @staticmethod
    def listdir(p: Path) -> List[Path]:
        """List content of directory (ignores system files on Windows)."""
        dirs = os.listdir(p)
        if PLATFORM_WINDOWS:

            def is_readable_dir(d: Path) -> bool:
                try:  # Windows does not correctly report user read permissions
                    os.listdir(d)
                except:
                    return False
                return True

            return [
                d for d in dirs
                if FileSystem.is_file(p / d) or is_readable_dir(p / d)
            ]
        return dirs

    @staticmethod
    def exists(node: Path) -> bool:
        """Node exists or is a dangling symlink."""
        return node.is_symlink() or node.exists()

    @staticmethod
    def is_dir(node: Path) -> bool:
        """Node is a directory and not a symlink."""
        return node.is_dir() and not node.is_symlink()

    @staticmethod
    def is_file(node: Path) -> bool:
        """Node is a file or a symlink."""
        return node.is_file() or node.is_symlink()

    @staticmethod
    def is_special(node: Path) -> bool:
        """Node is a special file (socket, pipe, device, etc.)."""
        return FileSystem.exists(node) and not FileSystem.is_dir(
            node) and not FileSystem.is_file(node)

    @staticmethod
    def node_stat(node: Path) -> os.stat_result:
        """Get node stat without follwoing symlinks."""
        return os.stat(node, follow_symlinks=False)

    @staticmethod
    def same_types(stat_a: os.stat_result, stat_b: os.stat_result) -> bool:
        """Stat of nodes indicate same node type."""
        def node_type(stat: os.stat_result) -> str:
            return oct(stat.st_mode)[:-3]

        return node_type(stat_a) == node_type(stat_b)

    @staticmethod
    def same_permissions(stat_a: os.stat_result,
                         stat_b: os.stat_result) -> bool:
        """Stat of nodes indicate same permissions."""
        def node_perm(stat: os.stat_result) -> str:
            return oct(stat.st_mode)[-3:]

        return node_perm(stat_a) == node_perm(stat_b)

    @staticmethod
    def remove_file(file: Path):
        """Remove if it is a file or else do nothing."""
        if FileSystem.is_file(file):
            if PLATFORM_WINDOWS:
                os.chmod(file, stat.S_IWUSR)
            os.remove(file)

    @staticmethod
    def remove_empty_dir(path: Path):
        """Remove empty directory, raises if directory is not empty."""
        if PLATFORM_WINDOWS:
            os.chmod(path, stat.S_IWUSR)
        os.rmdir(path)

    @staticmethod
    def remove_node(node: Path):
        """Remove node, no matter if file or directory."""
        if FileSystem.is_dir(node):
            shutil.rmtree(node)
        FileSystem.remove_file(node)

    @staticmethod
    def copy_file(src_file: Path, tgt_file: Path, *, link: bool = False):
        """Copy or hard link file (must exist), removes target first."""
        FileSystem.remove_node(tgt_file)
        if link:
            os.link(src_file, tgt_file, follow_symlinks=False)
        else:
            try:  # Windows does not correctly report user read permissions
                shutil.copy2(src_file, tgt_file, follow_symlinks=False)
            except Exception as e:
                if not PLATFORM_WINDOWS:
                    raise e

    @staticmethod
    def copy_stat(src_node: Path, tgt_node: Path):
        """Copy stat including permissions."""
        shutil.copystat(src_node, tgt_node, follow_symlinks=False)

    def sync(self,
             source_path: Path,
             target_path: Path,
             include_paths: List[Path] = [Path('.')],
             exclude_paths: List[Path] = [],
             *,
             create_hard_links: bool,
             indent: int = 0):
        """Recursively sync `source_path` to `target_path` while perserving
        modification time and permissions. Mimics behavior of `rsync --archive
        --delete`. If `include_paths` is not empty, the sync will restricted to
        these paths. `exclude_paths` will be excluded during sync. Both must be
        specified relative to the `source_path`. Prints progress bar."""
        if not include_paths: include_paths = [Path('.')]
        for p in include_paths:
            os.makedirs(target_path / p, exist_ok=True)

        def nodes_to_sync(src_path: Path, tgt_path: Path) -> Set[str]:
            nodes: Set[str] = set()
            if FileSystem.is_dir(src_path):
                nodes.update(FileSystem.listdir(src_path))
            elif FileSystem.is_file(src_path):
                nodes.update(src_path.name)
            if FileSystem.is_dir(tgt_path):
                nodes.update(FileSystem.listdir(tgt_path))
            elif FileSystem.is_file(tgt_path):
                nodes.update(tgt_path.name)
            return nodes

        def record_changes(ts: TaskSystem, subpath: Path, changes: FileChanges,
                           num_nodes: AtomicInt):
            def recursive(src_path: Path, tgt_path: Path,
                          changes: FileChanges):
                for node in nodes_to_sync(src_path, tgt_path):
                    src_node = src_path / node
                    tgt_node = tgt_path / node
                    change_type: Optional[FileSystem.ChangeType] = None
                    subchanges: Optional[FileChanges] = None
                    if src_node.relative_to(source_path) in exclude_paths:
                        continue
                    elif FileSystem.is_special(src_node):
                        continue  # skip special files
                    elif not FileSystem.exists(src_node):
                        # missing in source, delete in destination
                        change_type = FileSystem.ChangeType.RemoveNode
                    elif not FileSystem.exists(tgt_node):
                        # missing in destination, copy from source
                        if FileSystem.is_dir(src_node):
                            subchanges = {}
                            ts.add_or_run(recursive, src_node, tgt_node,
                                          subchanges)
                        change_type = FileSystem.ChangeType.UpdateNode
                    else:
                        sstat = FileSystem.node_stat(src_node)
                        tstat = FileSystem.node_stat(tgt_node)
                        if not FileSystem.same_types(sstat, tstat):
                            # different node type
                            if FileSystem.is_dir(src_node):
                                subchanges = {}
                                ts.add_or_run(recursive, src_node, tgt_node,
                                              subchanges)
                            change_type = FileSystem.ChangeType.CreateNode
                        elif FileSystem.is_dir(src_node):
                            # both are directories, step down
                            subchanges = {}
                            ts.add_or_run(recursive, src_node, tgt_node,
                                          subchanges)
                            change_type = FileSystem.ChangeType.NoChange
                            if not FileSystem.same_permissions(sstat, tstat):
                                # different permissions
                                change_type = FileSystem.ChangeType.UpdateStat
                        elif int(sstat.st_mtime) != int(tstat.st_mtime):
                            # both are non-directories with different mtime
                            change_type = FileSystem.ChangeType.UpdateNode
                    if change_type:
                        changes[node] = (change_type, subchanges)
                num_nodes.increment(len(changes))

            recursive(source_path / subpath, target_path / subpath, changes)

        def apply_changes(ts: TaskSystem, subpath: Path, changes: FileChanges,
                          num_nodes: AtomicInt):
            def copy_node(src_node: Path,
                          tgt_node: Path,
                          *,
                          is_dir: bool,
                          clear: bool = False):
                if is_dir:
                    if clear: FileSystem.remove_node(tgt_node)
                    os.makedirs(tgt_node, exist_ok=True)
                    FileSystem.copy_stat(src_node, tgt_node)
                else:
                    FileSystem.copy_file(src_node,
                                         tgt_node,
                                         link=create_hard_links)

            def recursive(src_path: Path, tgt_path: Path,
                          changes: FileChanges):
                for node, (change, subchanges) in changes.items():
                    src_node = src_path / node
                    tgt_node = tgt_path / node
                    is_dir = subchanges != None
                    if change == FileSystem.ChangeType.RemoveNode:
                        FileSystem.remove_node(tgt_node)
                    elif change == FileSystem.ChangeType.UpdateNode:
                        copy_node(src_node, tgt_node, is_dir=is_dir)
                    elif change == FileSystem.ChangeType.CreateNode:
                        copy_node(src_node,
                                  tgt_node,
                                  is_dir=is_dir,
                                  clear=True)
                    elif change == FileSystem.ChangeType.UpdateStat:
                        FileSystem.copy_stat(src_node, tgt_node)

                    if subchanges:
                        ts.add_or_run(recursive, src_node, tgt_node,
                                      subchanges)
                num_nodes.increment(len(changes.items()))

            recursive(source_path / subpath, target_path / subpath, changes)

        def sync_path(ts: TaskSystem,
                      subpath: Path,
                      num_nodes: AtomicInt,
                      dry_run: bool = False):
            def copy_node(src_node: Path,
                          tgt_node: Path,
                          *,
                          clear: bool = False) -> bool:
                if FileSystem.is_dir(src_node):
                    if not dry_run:
                        if clear: FileSystem.remove_node(tgt_node)
                        os.makedirs(tgt_node, exist_ok=True)
                        FileSystem.copy_stat(src_node, tgt_node)
                    return True  # directory needs sync
                elif not dry_run:
                    FileSystem.copy_file(src_node,
                                         tgt_node,
                                         link=create_hard_links)
                return False

            def recursive(src_path: Path, tgt_path: Path):
                nodes = nodes_to_sync(src_path, tgt_path)
                for node in nodes:
                    src_node = src_path / node
                    tgt_node = tgt_path / node
                    if src_node.relative_to(source_path) in exclude_paths:
                        continue
                    elif FileSystem.is_special(src_node):
                        continue  # skip special files
                    elif not FileSystem.exists(src_node):
                        # missing in source, delete in destination
                        if not dry_run: FileSystem.remove_node(tgt_node)
                    elif not FileSystem.exists(tgt_node):
                        # missing in destination, copy from source
                        if copy_node(src_node, tgt_node):
                            ts.add_or_run(recursive, src_node, tgt_node)
                    else:
                        # existing in source and destination
                        sstat = FileSystem.node_stat(src_node)
                        tstat = FileSystem.node_stat(tgt_node)
                        if not FileSystem.same_types(sstat, tstat):
                            # different node type
                            if copy_node(src_node, tgt_node, clear=True):
                                ts.add_or_run(recursive, src_node, tgt_node)
                            continue
                        elif FileSystem.is_dir(src_node):
                            # both are directories, step down
                            ts.add_or_run(recursive, src_node, tgt_node)
                        elif int(sstat.st_mtime) != int(tstat.st_mtime):
                            # both are non-directories with different mtime
                            if copy_node(src_node, tgt_node):
                                ts.add_or_run(recursive, src_node, tgt_node)
                            continue

                        if not FileSystem.same_permissions(sstat, tstat):
                            # different permissions
                            if not dry_run:
                                FileSystem.copy_stat(src_node, tgt_node)
                num_nodes.increment(len(nodes))

            recursive(source_path / subpath, target_path / subpath)

        changes: Dict[Path, FileChanges] = {p: {} for p in include_paths}
        num_nodes = AtomicInt()
        progress = FileSystem.ProgressPrinter(indent=indent)

        with executor() as e, TaskSystem(max_workers=self.__jobs) as ts:
            for p in include_paths:
                if self.__save_memory:
                    ts.add(sync_path, ts, p, num_nodes, dry_run=True)
                else:
                    ts.add(record_changes, ts, p, changes[p], num_nodes)
            e.submit(progress.show_indicator, "Discovering files...", ts,
                     num_nodes)

        max_nodes = num_nodes.get_and_set(0)

        with executor() as e, TaskSystem(max_workers=self.__jobs) as ts:
            for p in include_paths:
                if self.__save_memory:
                    ts.add(sync_path, ts, p, num_nodes)
                else:
                    ts.add(apply_changes, ts, p, changes[p], num_nodes)
            e.submit(progress.show_bar, "Progress", ts, num_nodes, max_nodes)

    def rmtree(self, root: Path, *, indent: int):
        """Recursively remove entire file tree. Prints progess bar."""
        def record_fds(num_nodes: AtomicInt):
            # Note: Single threaded without symlink race protection
            def recursive(topfd: int):
                node_count = 0
                for entry in os.scandir(topfd):
                    if entry.is_dir(follow_symlinks=False):
                        dirfd = os.open(entry.name, os.O_RDONLY, dir_fd=topfd)
                        recursive(dirfd)
                        os.close(dirfd)
                    node_count += 1
                num_nodes.increment(node_count)

            if not FileSystem.is_dir(root): return
            rootfd = os.open(root, os.O_RDONLY)
            recursive(rootfd)
            os.close(rootfd)

        def remove_fds(num_nodes: AtomicInt):
            # Note: Single threaded without symlink race protection
            def recursive(topfd: int, path: Path):
                node_count = 0
                for entry in os.scandir(topfd):
                    if entry.is_dir(follow_symlinks=False):
                        dirfd = os.open(entry.name, os.O_RDONLY, dir_fd=topfd)
                        recursive(dirfd, path / entry.name)
                        os.rmdir(entry.name, dir_fd=topfd)
                        os.close(dirfd)
                    else:
                        os.unlink(entry.name, dir_fd=topfd)
                    node_count += 1
                num_nodes.increment(node_count)

            if not FileSystem.is_dir(root): return
            rootfd = os.open(root, os.O_RDONLY)
            recursive(rootfd, root)
            os.close(rootfd)
            os.rmdir(root)

        def record_nodes(ts: TaskSystem, changes: FileChanges,
                         num_nodes: AtomicInt):
            def recursive(path: Path, changes: FileChanges):
                for entry in os.scandir(path):
                    change_type = FileSystem.ChangeType.NoChange
                    subchanges: Optional[FileChanges] = None
                    if entry.is_dir(follow_symlinks=False):
                        subchanges = {}
                        ts.add_or_run(recursive, path / entry.name, subchanges)
                    else:
                        change_type = FileSystem.ChangeType.RemoveNode
                    changes[entry.name] = (change_type, subchanges)
                num_nodes.increment(len(changes))

            recursive(root, changes)

        def try_remove_parents(path: Path):
            try:
                while True:
                    FileSystem.remove_empty_dir(path)
                    if path == root:
                        break
                    path = path.parent
            except:
                pass

        def remove_nodes(ts: TaskSystem, changes: FileChanges,
                         num_nodes: AtomicInt):
            def recursive(path: Path, changes: FileChanges):
                for node, (change, subchanges) in changes.items():
                    if change == FileSystem.ChangeType.RemoveNode:
                        FileSystem.remove_file(path / node)
                    if subchanges != None:
                        ts.add_or_run(recursive, path / node, subchanges)
                num_nodes.increment(len(changes.items()))
                try_remove_parents(path)

            recursive(root, changes)

        def remove_content(ts: TaskSystem,
                           num_nodes: AtomicInt,
                           *,
                           dry_run: bool = False):
            def recursive(path: Path):
                node_count = 0
                for entry in os.scandir(path):
                    node_path = path / entry.name
                    if entry.is_dir(follow_symlinks=False):
                        ts.add_or_run(recursive, node_path)
                    else:
                        if not dry_run: FileSystem.remove_file(node_path)
                    node_count += 1
                num_nodes.increment(node_count)
                if not dry_run: try_remove_parents(path)

            recursive(root)

        changes: FileChanges = {}
        num_nodes = AtomicInt()
        progress = FileSystem.ProgressPrinter(indent=indent)

        with executor() as e, TaskSystem(max_workers=self.__jobs) as ts:
            if self.__save_memory:
                ts.add(remove_content, ts, num_nodes, dry_run=True)
            elif self.__fd_api_support:
                ts.add(record_fds, num_nodes)
            else:
                ts.add(record_nodes, ts, changes, num_nodes)
            e.submit(progress.show_indicator, "Discovering files...", ts,
                     num_nodes)

        max_nodes = num_nodes.get_and_set(0)

        with executor() as e, TaskSystem(max_workers=self.__jobs) as ts:
            if self.__save_memory:
                ts.add(remove_content, ts, num_nodes)
            elif self.__fd_api_support:
                ts.add(remove_fds, num_nodes)
            else:
                ts.add(remove_nodes, ts, changes, num_nodes)
            e.submit(progress.show_bar, "Progress", ts, num_nodes, max_nodes)


LoggerFunc = Callable[[str, int], None]
Json = Union[str, int, float, bool, None, Dict[str, 'Json'], List['Json']]


class StageManager:
    """Manage snapshots in stages and parse stage configuration."""
    class Stage:
        def __init__(self, name: str, interval: int, keep: int):
            self.name = name
            self.interval = interval
            self.keep = keep

    class TimeStamp:
        """Handle read and write of time stamps."""
        def __init__(self, stages_path: Path, base_interval: int):
            """Create time stamps in `backup_path` with the `base_interval` of
            the initial stage."""
            self.__stages_path = stages_path
            self.__base_interval = base_interval
            self.__now = int(time.time())

        def __path(self, snapshot: str) -> Path:
            return self.__stages_path / f".{snapshot}.stamp"

        def read(self, snapshot: str) -> int:
            """Read time stamp for `snapshot`, raises on missing file."""
            with open(self.__path(snapshot), 'r') as f:
                return int(f.readline())

        def elapsed(self, snapshot: str) -> int:
            """Seconds since creation of `snapshot`, raises on missing file."""
            return self.__now - self.read(snapshot)

        def create(self, snapshot: str):
            """Create new time stamp for `snapshot`."""
            def align(val: int, *, to_multiple_of: int) -> int:
                return int(val / to_multiple_of) * to_multiple_of

            with open(self.__path(snapshot), 'w') as f:
                now = align(self.__now, to_multiple_of=self.__base_interval)
                f.write(str(now))

        def copy(self, src_snapshot: str, tgt_snapshot: str):
            """Copy time stamp from `src_snapshot` to `tgt_snapshot`."""
            spath = self.__path(src_snapshot)
            if FileSystem.is_file(spath):
                FileSystem.copy_file(spath, self.__path(tgt_snapshot))

        def remove(self, snapshot: str):
            """Remove time stamp for `snapshot`."""
            FileSystem.remove_file(self.__path(snapshot))

    def __init__(self,
                 source_path: Path,
                 stages_path: Path,
                 include_paths: List[Path],
                 exclude_paths: List[Path],
                 config: Path,
                 *,
                 jobs: int,
                 save_memory: bool,
                 logger: LoggerFunc,
                 indent: int = 0):
        def load_stages_from_config(
                file: Path) -> Tuple[List['StageManager.Stage'], int]:
            with open(file, 'r') as f:
                cfg: Dict[str, Json] = json.load(f)
            stages: List[StageManager.Stage] = []
            interval = cast(int, cfg["interval"])
            stage_interval = interval
            for s in cast(List[Dict[str, Union[str, int]]], cfg["stages"]):
                name = cast(str, s["name"])
                keep = cast(int, s["keep"])
                stages.append(StageManager.Stage(name, stage_interval, keep))
                stage_interval = stage_interval * keep
            return stages, interval

        self.__fs = FileSystem(jobs=jobs, save_memory=save_memory)
        self.__source_path = source_path
        self.__stages_path = stages_path
        self.__include_paths = include_paths
        self.__exclude_paths = exclude_paths
        self.__stages, interval = load_stages_from_config(config)
        self.__timestamp = StageManager.TimeStamp(stages_path, interval)
        self.__logger = logger
        self.__indent = indent
        self.__dname = '.delete'
        self.__recover = False
        try:
            self.__timestamp.read(self.snapshot_names()[0])
        except:
            # time stamp for first snapshot was not successfully created
            self.__recover = True

    def __log(self, msg: str, *, indent: int = 0):
        self.__logger(msg, self.__indent + indent)

    def __snapshot_name(self, stage_name: str, stage_num: str) -> str:
        return f'{stage_name}.{stage_num}'

    def __path(self, snapshot: str) -> Path:
        return self.__stages_path / snapshot

    def __has(self, snapshot: str) -> bool:
        return FileSystem.is_dir(self.__path(snapshot))

    def __rm(self, snapshot: str, *, indent: int, delete_eager: bool = False):
        if self.__has(snapshot):
            if delete_eager:
                self.__fs.rmtree(self.__path(snapshot),
                                 indent=self.__indent + indent)
            else:
                if FileSystem.is_dir(self.__path(self.__dname)):
                    self.__fs.rmtree(self.__path(self.__dname),
                                     indent=self.__indent + indent)
                os.rename(self.__path(snapshot), self.__path(self.__dname))
            self.__timestamp.remove(snapshot)

    def __mv(self, src_snapshot: str, tgt_snapshot: str):
        self.__timestamp.copy(src_snapshot, tgt_snapshot)
        os.rename(self.__path(src_snapshot), self.__path(tgt_snapshot))
        self.__timestamp.remove(src_snapshot)

    def __cp(self, src_snapshot: str, tgt_snapshot: str, *, indent: int):
        self.__fs.sync(self.__path(src_snapshot),
                       self.__path(tgt_snapshot),
                       self.__include_paths,
                       self.__exclude_paths,
                       create_hard_links=True,
                       indent=self.__indent + indent)

    def __next_after(self, stage_id: int, num: int) -> Optional[str]:
        """Get new snapshot name for stage with id `stage_id` and number `num`.
        Returns `None` if no valid snapshot name can be determined."""
        name = self.__snapshot_name(self.__stages[stage_id].name, str(num))
        try:
            elapsed = self.__timestamp.elapsed(name)
        except:
            return None  # timestamp missing, invalid
        for i in range(stage_id, len(self.__stages)):
            stage = self.__stages[i]
            num = int(elapsed / stage.interval)
            if num >= 0 and num < stage.keep:
                return self.__snapshot_name(stage.name, str(num))
        return None

    def snapshot_names(self) -> List[str]:
        """Obtain all possible snapshot names in ascending order."""
        return [
            self.__snapshot_name(s.name, str(i)) for s in self.__stages
            for i in range(s.keep)
        ]

    def create(self):
        """Create new snapshot for initial stage."""
        init_name = self.__stages[0].name
        init_snapshot = self.__snapshot_name(init_name, '0')

        if self.__recover:
            if self.__has(init_snapshot):
                self.__log("* Removing partial snapshot from interrupted run.")
                self.__rm(init_snapshot, indent=1, delete_eager=True)
            if self.__has(self.__dname):
                self.__log("* Cleanup pending removal from interrupted run.")
                self.__rm(self.__dname, indent=1, delete_eager=True)

        if self.__has(init_snapshot):
            self.__log(f"Stage '{init_name}' still up-to-date, nothing to do.")
            return

        if self.__has(self.__dname):
            self.__log("* Reusing previously deleted snapshot.")
            self.__mv(self.__dname, init_snapshot)
            for p in self.__exclude_paths:
                # remove current excludes from previous snapshot
                FileSystem.remove_node(self.__path(init_snapshot) / p)
            for name in self.snapshot_names():
                if name == init_snapshot: continue
                if self.__has(name):
                    self.__cp(name, init_snapshot, indent=1)
                    break
        else:
            for name in self.snapshot_names():
                if self.__has(name):
                    self.__log(
                        f"* Creating hard copy from previous backup '{name}'.")
                    self.__cp(name, init_snapshot, indent=1)
                    break

        self.__log("* Running sync to create the actual backup.")
        self.__fs.sync(self.__source_path,
                       self.__path(init_snapshot),
                       self.__include_paths,
                       self.__exclude_paths,
                       create_hard_links=False,
                       indent=self.__indent + 1)
        self.__timestamp.create(init_snapshot)  # indicates successful creation
        self.__recover = False

    def rotate(self):
        """Rotate existing snapshots in stages."""
        num_stages = len(self.__stages)
        for stage_id in range(num_stages - 1, -1, -1):
            stage = self.__stages[stage_id]
            self.__log(f"* Rotating stage '{stage.name}'.")
            for i in range(stage.keep - 1, -1, -1):
                src_name = self.__snapshot_name(stage.name, str(i))
                if self.__has(src_name):
                    tgt_name = self.__next_after(stage_id, i)
                    if not tgt_name:
                        self.__log(f"- Removing {stage.name}.{i}", indent=1)
                        self.__rm(src_name, indent=2)
                    elif src_name != tgt_name:
                        if not self.__has(tgt_name):
                            self.__log(f"- Moving {src_name} -> {tgt_name}",
                                       indent=1)
                            self.__mv(src_name, tgt_name)
                        else:
                            self.__log(f"- Removing {src_name}", indent=1)
                            self.__rm(src_name, indent=2)


def rsync_backup(source_path: Path,
                 backup_path: Path,
                 include_paths: List[Path],
                 exclude_paths: List[Path],
                 config: Optional[Path] = None,
                 *,
                 jobs: int = multiprocessing.cpu_count(),
                 save_memory: bool = False) -> bool:
    """Create backup snapshot from `source_path` in `backup_path`. The backup is
    restricted to `include_paths` if not empty. `exclude_paths` are ignored
    during sync. Both must be relative to the `source_path`. If no `config` path
    is provided, the configuration is assumed to be found in
    `backup_path`/config.json. `save_memory` prevents building up large
    in-memory file trees for sync and rmtree but is slightly slower."""
    if not config:
        config = backup_path / "config.json"

    config = config.resolve()
    source_path = source_path.resolve()
    backup_path = backup_path.resolve()

    if not FileSystem.is_dir(source_path):
        print(f"Error: Source path '{source_path}' is not a valid directory.")
        return False
    if not config.is_file():
        print(f"Error: Config file '{config}' is not a valid file.")
        return False

    def normalize_and_relpath(paths: List[Path]) -> List[Path]:
        rel_paths: List[Path] = []
        for p in paths:
            norm_path = (source_path / p).resolve()
            if os.path.commonpath([norm_path,
                                   source_path]) == str(source_path):
                rel_paths.append(norm_path.relative_to(source_path))
            else:  # norm_path points to outside of source_path
                print(f"Warning: Path '{p}' is outside of source_path.")
        return rel_paths

    def remove_shadowed_paths(paths: List[Path]) -> List[Path]:
        if len(paths) <= 1: return paths
        rest = list(paths)
        first = rest.pop(0)
        for i in range(len(rest)):
            common = Path(os.path.commonpath([first, rest[i]]))
            if common in [first, rest[i]]:
                remove = rest[i] if common == first else first
                print(f"Warning: Path '{remove}' is shadowed by '{common}'")
                rest[i] = common
                return remove_shadowed_paths(rest)
        return [first] + remove_shadowed_paths(rest)

    restricted = bool(include_paths)
    include_paths = remove_shadowed_paths(normalize_and_relpath(include_paths))
    exclude_paths = remove_shadowed_paths(normalize_and_relpath(exclude_paths))

    if restricted and not include_paths:
        print("Error: Malformed include paths.")
        return False

    if backup_path.drive == source_path.drive and os.path.commonpath(
        [backup_path, source_path]) == str(source_path):
        exclude = backup_path.relative_to(source_path)
        if exclude not in exclude_paths:
            print(f"Warning: Excluding backup_path, which is in source_path.")
            exclude_paths.append(exclude)

    os.makedirs(backup_path, exist_ok=True)

    @contextmanager
    def create_progress_marker(backup_path: Path):
        created = False
        marker = backup_path / '.inprogress'
        if not FileSystem.is_file(marker):
            with open(marker, 'x') as f:
                f.write(str(time.time()))
            created = True
        try:
            yield marker, created
        finally:
            if created:
                FileSystem.remove_file(marker)

    def log(msg: str, indent_level: int = 0):
        msg = (' ' * indent_level * INDENT_WIDTH) + msg
        print(msg)
        with open(backup_path / 'backup.log', 'a') as file:
            timestamp = time.strftime('[%Y/%m/%d %H:%M:%S]')
            for line in msg.splitlines():
                file.write(f'{timestamp} {line}\n')

    with create_progress_marker(backup_path) as (marker, created):
        if created:
            try:
                log("[RUN] Starting backup process.")
                stages = StageManager(source_path,
                                      backup_path,
                                      include_paths,
                                      exclude_paths,
                                      config,
                                      jobs=jobs,
                                      save_memory=save_memory,
                                      logger=log,
                                      indent=1)
                init = stages.snapshot_names()[0]
                log("[1/2] Rotating stages:")
                stages.rotate()
                log(f"[2/2] Creating new snapshot for initial stage '{init}':")
                stages.create()
                log("[END] Finished backup process.")
            except Exception as e:
                log(f"[ERR] Backup process failed with error:\n{e}")
                return False
        else:
            log(f"[ERR] Backup process already running.\nRemove {marker}.")
            return False
    return True


if __name__ == '__main__':

    def str2bool(v: Any):
        if isinstance(v, bool):
            return v
        elif isinstance(v, str):
            if v.lower() in ('yes', 'true', 't', 'y', '1'):
                return True
            elif v.lower() in ('no', 'false', 'f', 'n', '0'):
                return False
        raise argparse.ArgumentTypeError('Boolean value expected.')

    script_name = os.path.basename(__file__)
    parser = ArgumentParser(
        prog=script_name,
        description='Create incremental backups.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f'''Example usage:
  - Backup '/' to '/backup'
    {script_name} / /backup
  - Backup '/' to '/backup', but exclude '/mnt' and '/tmp'
    {script_name} / /backup -e mnt -e tmp
  - Backup '/' to '/backup', but only include '/etc' and '/usr'
    {script_name} / /backup -i etc -i usr
  - Backup '/' to '/backup', only include '/etc' and '/usr' without '/usr/local'
    {script_name} / /backup -i etc -i usr -e usr/local''')

    parser.add_argument(
        '-c',
        '--config',
        metavar='config_file',
        type=Path,
        help='Configuration file (default: <backup_path>/config.json).')
    parser.add_argument(
        '-s',
        '--save-memory',
        metavar='true|false',
        type=str2bool,
        nargs='?',
        const=True,
        default=False,
        help='Do not keep entire file tree in memory (slightly slower).')
    parser.add_argument(
        '-j',
        '--jobs',
        metavar='n',
        type=int,
        default=multiprocessing.cpu_count(),
        help='Number of parallel jobs (default: number of logical cores).')
    parser.add_argument('source_path', type=Path, help='Directory to backup.')
    parser.add_argument('backup_path',
                        type=Path,
                        help='Directory to store backups in.')
    parser.add_argument(
        '-i',
        '--include-paths',
        metavar='include_path',
        type=Path,
        action='append',
        default=[],
        help='Include path for backup (must be relative to <source_path>).')
    parser.add_argument(
        '-e',
        '--exclude-paths',
        metavar='exclude_path',
        type=Path,
        action='append',
        default=[],
        help='Exclude path from backup (must be relative to <source_path>).')
    args = parser.parse_args()
    sys.exit(0 if rsync_backup(**vars(args)) else 1)
