from orjson import loads, dumps
from contextlib import contextmanager
from dataclasses import dataclass
from collections.abc import Iterator
from sqlmodel import Session, SQLModel, create_engine
from wolf.app import Application
from wolf.app.pluggability import Installable


@dataclass(kw_only=True)
class SQLDatabase(Installable):
    url: str
    echo: bool = False
    models_registries: tuple[type[SQLModel], ...] = (SQLModel,)

    def __post_init__(self):
        engine = create_engine(
            self.url,
            echo=self.echo,
            json_serializer=dumps,
            json_deserializer=loads
        )
        for registry in self.models_registries:
            registry.metadata.create_all(engine)
        self.engine = engine

    def install(self, application: Application):
        application.services.register_factory(Session, self.sqlsession)

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
