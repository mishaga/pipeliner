from abc import ABC, abstractmethod
from collections.abc import Sequence


class PipelineError(Exception): ...


def _pipe_operator(first: 'BasePipe | Pipeline', second: 'BasePipe | Pipeline') -> 'Pipeline':
    if isinstance(first, BasePipe) and isinstance(second, BasePipe):
        return Pipeline([first, second])

    if isinstance(first, Pipeline) and isinstance(second, BasePipe):
        return Pipeline([*first.pipes, second])

    if isinstance(first, BasePipe) and isinstance(second, Pipeline):
        return Pipeline([first, *second.pipes])

    if isinstance(first, Pipeline) and isinstance(second, Pipeline):
        return Pipeline([*first.pipes, *second.pipes])

    raise PipelineError


class BasePipe(ABC):
    def __or__(self, other: 'BasePipe | Pipeline') -> 'Pipeline':
        return _pipe_operator(self, other)

    @abstractmethod
    def __call__(self, *args, **kwargs):  # ... annotate
        ...


class Pipe(BasePipe):

    def __init__(
        self,
        func,  # ... annotate (callable)
        *args,  # ... annotate
        **kwargs,  # ... annotate
    ) -> None:
        self.__func = func
        self.__args = args
        self.__kwargs = kwargs

    def __call__(self, *args, **kwargs):  # ... annotate
        args = *args, *self.__args
        kwargs = {**kwargs, **self.__kwargs}
        print('Calling', self.__func.__name__, f'{args=}', f'{kwargs=}')
        return self.__func(*args, **kwargs)


class Split(BasePipe):

    def __init__(
        self,
        *,
        condition,  # ... annotate (callable that returns bool)
        if_true: Pipe,
        if_false: Pipe,
    ) -> None:
        self.__condition = condition
        self.__if_true = if_true
        self.__if_false = if_false

    def __call__(self, *args, **kwargs):  # ... annotate
        print('Splitting pipeline, condition:', self.__condition(*args, **kwargs))
        if self.__condition(*args, **kwargs):
            return self.__if_true(*args, **kwargs)
        else:
            return self.__if_false(*args, **kwargs)


class Pipeline:

    def __init__(
        self,
        pipes: Sequence[BasePipe],
        *,
        logger = ...,  # ... annotate
        ctx = ...,  # ... annotate
    ) -> None:
        self.__pipes = pipes
        self.__logger = logger
        self.__ctx = ctx

    def __or__(self, other: 'Pipe | Pipeline') -> 'Pipeline':
        return _pipe_operator(self, other)

    @property
    def pipes(self) -> Sequence[BasePipe]:
        return self.__pipes

    def run(self, *args, **kwargs):
        res = None

        for task in self.__pipes:
            res = task(*args, *kwargs)
            args = (res,)
            kwargs = {}

        return res
