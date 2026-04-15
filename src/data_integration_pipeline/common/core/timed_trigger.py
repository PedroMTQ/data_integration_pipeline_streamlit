import time
from typing import Callable

from data_integration_pipeline.common.io.logger import logger


class TimedTrigger:
    """
    this is just a simple class that waits a certain time before triggering a certain call
    this is useful for logging or metric tracking purposes
    """

    def __init__(self, delay: int, function: Callable, raise_exception: bool = True, **kwargs) -> None:
        self.last_trigger_time = time.time()
        self.current_time = time.time()
        self.delay = delay
        self.function = function
        self.kwargs = kwargs
        self.raise_exception = raise_exception

    def trigger(self, **kwargs):
        force = kwargs.pop('force', False)
        self.current_time = time.time()
        if self.current_time - self.last_trigger_time > self.delay or force:
            try:
                if kwargs:
                    self.function(**kwargs)
                else:
                    self.function(**self.kwargs)
            except Exception as e:
                if self.raise_exception:
                    raise e
                else:
                    logger.exception(e)
            self.reset_time()

    def reset_time(self):
        self.last_trigger_time = time.time()


if __name__ == '__main__':

    def hello(input_str):
        print(f'hello {input_str}')

    timed_trigger = TimedTrigger(delay=2, function=hello)
    timed_trigger.trigger(input_str='world!', force=True)
    while True:
        timed_trigger.trigger(input_str='world!', force=False)
        timed_trigger.trigger(input_str='world!')
