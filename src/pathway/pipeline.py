from pathway.runtime.dataset import AbstractDataObject
from sagemaker.workflow.steps import Step
from sagemaker.workflow.pipeline import Pipeline
from typing import Optional, List, Callable, Any
from dataclasses import dataclass


class _PipelineDataObject(AbstractDataObject):
    def __init__(self, step: Step, path: str):
        self._step = step
        self._path = path

    def get(self) -> Any:
        raise NotImplementedError

    @property
    def step(self):
        return self._step

    @property
    def path(self):
        return self._path


class StepDelegation:
    def __init__(self, delegated: Step, results: _PipelineDataObject):
        self._delegated = delegated
        self._results = results

    @property
    def results(self):
        return self._results


def pipeline(name: str):
    def inner(func: Callable):
        def wrapper(*args, **kwargs):
            # validate the arguments

            # run under pipeline context
            with PipelineSession():
                func(*args, **kwargs)
                steps = PipelineContext.get_current_pipeline_session().steps
                return Pipeline(name=name, steps=steps)

        return wrapper
    return inner


class PipelineSession:
    def __init__(self):
        self.steps: List[Step] = list()

    # /Context Manager ----------------------------------------------
    def __enter__(self):
        PipelineContext.push_pipeline_session(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        PipelineContext.pop_pipeline_session()

    def append_step(self, step: Step):
        self.steps.append(step)


class PipelineContext:
    _pipeline_session: Optional[PipelineSession] = None
    _previous_pipeline_sessions: List[PipelineSession] = []

    @classmethod
    def push_pipeline_session(cls, pipeline_session: PipelineSession):
        if cls._pipeline_session:
            cls._previous_pipeline_sessions.append(cls._pipeline_session)
        cls._pipeline_session = pipeline_session

    @classmethod
    def pop_pipeline_session(cls) -> Optional[PipelineSession]:
        old_pipeline_session = cls._pipeline_session
        if cls._previous_pipeline_sessions:
            cls._pipeline_session = cls._previous_pipeline_sessions.pop()
        else:
            cls._pipeline_session = None
        return old_pipeline_session

    @classmethod
    def get_current_pipeline_session(cls) -> Optional[PipelineSession]:
        return cls._pipeline_session
