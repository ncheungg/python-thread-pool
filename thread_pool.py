from threading import Thread, Lock, Event
from collections import deque
from typing import Deque, Callable, Any, List


# python sentinel object, see PEP 661
_INCOMPLETE = object()


class Task:
    def __init__(
        self, fn: Callable[..., Any], args: List[Any], callback: Callable[[], None]
    ) -> None:
        self.result_val: Any = _INCOMPLETE
        self.finished_event = Event()
        self.wrapped_fn = self._get_wrapped_fn(fn, args)
        self._callback = callback

    def _get_wrapped_fn(self, fn: Callable[..., Any], args: List[Any]) -> None:
        def wrapper():
            self.result_val = fn(*args)
            self.finished_event.set()
            self._callback()

        return wrapper

    def _spawn_thread_and_run(self) -> None:
        thread = Thread(target=self.wrapped_fn)
        thread.start()

    def finished(self) -> bool:
        return self.result_val is not _INCOMPLETE

    def result(self) -> Any:
        if self.finished():
            return self.result_val

        self.finished_event.wait()
        return self.result_val


class ThreadPool:
    def __init__(self, workers: int) -> None:
        self.workers = workers
        self.tasks: Deque[Task] = deque()
        self.lock = Lock()

    def _task_done(self) -> None:
        with self.lock:
            if self.tasks:
                task = self.tasks.popleft()
                task._spawn_thread_and_run()
            else:
                self.workers += 1

    def submit(self, fn: Callable[..., Any], args: List[Any] | None = None) -> Task:
        task = Task(fn, args, self._task_done)

        with self.lock:
            if self.workers > 0:
                self.workers -= 1
                task._spawn_thread_and_run()
            else:
                self.tasks.append(task)

        return task
