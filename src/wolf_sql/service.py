from contextlib import contextmanager
from collections.abc import Iterator, Collection

import structlog
from orjson import loads, dumps
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.main import default_registry
from wolf.app import Application
from wolf.app.pluggability import Installable
from sqlalchemy.orm import registry
from sqlalchemy.engine import Engine


logger = structlog.get_logger("wolf_sql.service")


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

    def install(self, app: Application):
        app.services.register_factory(Session, self.sqlsession)
        app.events.lifecycle.on_init.connect(self.initialize)

    def initialize(self, _, *, config: dict | None = None):
        if self._initialized:
            logger.error('Database is already initialized.')
            raise RuntimeError('Database is already initialized.')
        try:
            for registry in self:
                registry.metadata.create_all(self.engine)
        finally:
            logger.info(f'Database {self} has been initialized.')
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
