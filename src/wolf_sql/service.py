from contextlib import contextmanager
from collections.abc import Iterator, Collection
from orjson import loads, dumps
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.main import default_registry
from wolf.app import Application
from wolf.app.pluggability import Installable
from sqlalchemy.orm import registry
from sqlalchemy.engine import Engine


class SQLDatabase(Installable, Collection[registry]):
    __slots__ = ('_initialized', '_registries', 'engine')

    _initialized: bool
    _registries: frozenset[registry]

    def __init__(
            self,
            url: str, *,
            echo: bool = False,
            registries: Collection[registry] = (default_registry,)
    ):
        self.engine: Engine = create_engine(
            url,
            echo=echo,
            json_serializer=dumps,
            json_deserializer=loads
        )
        self._initialized = False
        self._registries = frozenset(registries)

    def __contains__(self, model: type[SQLModel]):
        return model in self._registries

    def __iter__(self):
        return iter(self._registries)

    def __len__(self):
        return len(self._registries)

    def install(self, application: Application):
        application.services.register_factory(Session, self.sqlsession)
        application.listen('init', self.initialize)

    def initialize(self):
        if self._initialized:
            raise RuntimeError('Database is already initialized.')
        try:
            for registry in self:
                registry.metadata.create_all(self.engine)
        finally:
            self._initialized = True
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
