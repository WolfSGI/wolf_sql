from contextlib import contextmanager
from collections.abc import Iterator, Collection
from orjson import loads, dumps
from sqlmodel import Session, SQLModel, create_engine
from wolf.app import Application
from wolf.app.pluggability import Installable


class SQLDatabase(Installable, Collection[type[SQLModel]]):
    _models: set
    _finalized: bool

    def __init__(self, engine, models: Collection[type[SQLModel]]):
        self.engine = engine
        self._models = set(models)
        self._finalized = False

    def __contains__(self, model: type[SQLModel]):
        return model in self._models

    def __iter__(self):
        return iter(self._models)

    def __len__(self):
        return len(self._models)

    def __bool__(self):
        return self._finalized

    def add(self, model: type[SQLModel]):
        if self._finalized:
            raise RuntimeError('Database is already finalized.')
        self._models.add(model)

    def discard(self, model: type[SQLModel]):
        if self._finalized:
            raise RuntimeError('Database is already finalized.')
        self._models.discard(model)

    @classmethod
    def from_url(
            cls,
            url: str,
            models: Collection[type[SQLModel]]=(),
            echo: bool=False
    ):
        engine = create_engine(
            url,
            echo=echo,
            json_serializer=dumps,
            json_deserializer=loads
        )
        return cls(engine, models)

    def install(self, application: Application):
        application.services.register_factory(Session, self.sqlsession)
        application.listen('finalize', self.finalize)

    def finalize(self):
        if self._finalized:
            raise RuntimeError('Database is already finalized.')
        try:
            for model in self:
                model.metadata.create_all(self.engine)
        finally:
            self._finalized = True
        return self

    @contextmanager
    def sqlsession(self) -> Iterator[Session]:
        with Session(self.engine) as session:
            try:
                yield session
            except Exception:
                # maybe log.
                raise
            else:
                session.commit()
