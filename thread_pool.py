from threading import Thread, Lock, Event
from collections import deque
from typing import Deque, Callable, Any, List, Self


# python sentinel object, see PEP 661
_INCOMPLETE = object()


class Task:
    def __init__(
        self,
        fn: Callable[..., Any],
        args: List[Any],
        callback: Callable[[], None],
        cancelled: bool = False,
    ) -> None:
        self.result_val: Any = _INCOMPLETE
        self.finished_event = Event()
        self.wrapped_fn = self._get_wrapped_fn(fn, args)
        self._callback = callback
        self._is_cancalled = cancelled

    def _get_wrapped_fn(self, fn: Callable[..., Any], args: List[Any]) -> None:
        def wrapper():
            self.result_val = fn(*args)
            self.finished_event.set()
            self._callback()

        return wrapper

    def _spawn_thread_and_run(self) -> None:
        thread = Thread(target=self.wrapped_fn)
        thread.start()

    def cancelled(self) -> bool:
        return self._is_cancalled

    def finished(self) -> bool:
        return self.result_val is not _INCOMPLETE

    def result(self) -> Any:
        if self.finished():
            return self.result_val

        if self.cancelled():
            return None

        self.finished_event.wait()
        return self.result_val


class ThreadPool:
    def __init__(self, workers: int) -> None:
        self.workers = workers
        self.tasks: Deque[Task] = deque()
        self.lock = Lock()
        self.is_shutdown = False

    def __enter__(self) -> Self:
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.shutdown()

    def _task_done(self) -> None:
        with self.lock:
            if self.tasks:
                task = self.tasks.popleft()
                task._spawn_thread_and_run()
            else:
                self.workers += 1

    def submit(self, fn: Callable[..., Any], args: List[Any] | None = None) -> Task:
        if self.is_shutdown:
            return Task(fn, args, self._task_done, cancelled=True)

        task = Task(fn, args, self._task_done)

        with self.lock:
            if self.workers > 0:
                self.workers -= 1
                task._spawn_thread_and_run()
            else:
                self.tasks.append(task)

        return task

    def shutdown(self) -> None:
        self.is_shutdown = True
