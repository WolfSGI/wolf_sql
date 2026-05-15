import pytest
from sqlmodel import Field, SQLModel, select
from sqlmodel.main import default_registry
from wolf_sql import SQLDatabase
from sqlalchemy.orm import registry
from sqlalchemy.exc import OperationalError


other_registry = registry()


class Hero(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    secret_name: str
    age: int | None = None


class Citizen(SQLModel, table=True, registry=other_registry):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    age: int | None = None


def test_empty_database_init():
    db = SQLDatabase(url="sqlite://", registries=())
    assert len(db) == 0
    assert db.initialize() is db


def test_database_init():
    db = SQLDatabase(url="sqlite://")
    assert Hero._sa_registry in db
    assert len(db) == 1
    assert tuple(db) == (default_registry,)


def test_database_init_other_registries():
    db = SQLDatabase(url="sqlite://", registries=(other_registry,))
    assert Hero._sa_registry not in db
    assert len(db) == 1
    assert tuple(db) == (other_registry,)


def test_context_manager_success():
    hero_1 = Hero(name="Deadpond", secret_name="Dive Wilson")
    hero_2 = Hero(name="Spider-Boy", secret_name="Pedro Parqueador")
    hero_3 = Hero(name="Rusty-Man", secret_name="Tommy Sharp", age=48)

    db = SQLDatabase("sqlite://")
    db.initialize()
    with db.sqlsession() as session:
        session.add(hero_1)
        session.add(hero_2)
        session.add(hero_3)

    with db.sqlsession() as session:
        statement = select(Hero).where(Hero.name == "Spider-Boy")
        hero = session.exec(statement).first()
        assert hero.secret_name == "Pedro Parqueador"

    Hero.metadata.drop_all(bind=db.engine)


def test_context_manager_failure():
    hero = Hero(name="Deadpond", secret_name="Dive Wilson")

    db = SQLDatabase("sqlite://")
    db.initialize()

    with pytest.raises(NotImplementedError):
        with db.sqlsession() as session:
            session.add(hero)
            raise NotImplementedError('This is bad omen.')

    with db.sqlsession() as session:
        statement = select(Hero).where(Hero.name == "Deadpond")
        hero = session.exec(statement).first()
        assert hero is None


def test_context_manager_wrong_registry():
    hero = Hero(name="Deadpond", secret_name="Dive Wilson")

    db = SQLDatabase("sqlite://", registries=(other_registry,))
    db.initialize()

    with pytest.raises(OperationalError):
        with db.sqlsession() as session:
            session.add(hero)

    other_registry.metadata.drop_all(bind=db.engine)
