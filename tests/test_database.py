import pytest
from sqlmodel import Field, SQLModel, select
from wolf_sql import SQLDatabase


class Hero(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    secret_name: str
    age: int | None = None



def test_empty_database():
    db = SQLDatabase.from_url(url="sqlite://")
    assert len(db) == 0
    assert bool(db) is False

    assert db.finalize() is db
    assert bool(db) is True


def test_database_init():
    db = SQLDatabase.from_url(url="sqlite://")
    assert db.engine is not None
    assert len(db) == 0
    assert bool(db) is False

    db = SQLDatabase.from_url(url="sqlite://", models=(Hero,))
    assert Hero in db
    assert len(db) == 1
    assert bool(db) is False
    assert set(db) == set((Hero,))


def test_database_add_discard():
    db = SQLDatabase.from_url(url="sqlite://")
    assert db.engine is not None
    assert len(db) == 0
    assert bool(db) is False

    db.add(Hero)
    assert len(db) == 1
    assert set(db) == set((Hero,))

    db.add(Hero)
    assert len(db) == 1
    assert set(db) == set((Hero,))

    db.discard(Hero)
    assert len(db) == 0
    assert set(db) == set()


def test_finalize_prevents_adding():
    db = SQLDatabase.from_url(url="sqlite://")
    assert len(db) == 0
    db.finalize()

    with pytest.raises(RuntimeError):
        db.add(Hero)

    assert len(db) == 0


def test_finalize_prevents_discarding():
    db = SQLDatabase.from_url(url="sqlite://", models=(Hero,))
    assert len(db) == 1
    db.finalize()

    with pytest.raises(RuntimeError):
        db.discard(Hero)

    assert len(db) == 1
    Hero.metadata.drop_all(bind=db.engine)


def test_context_manager_success():
    hero_1 = Hero(name="Deadpond", secret_name="Dive Wilson")
    hero_2 = Hero(name="Spider-Boy", secret_name="Pedro Parqueador")
    hero_3 = Hero(name="Rusty-Man", secret_name="Tommy Sharp", age=48)

    db = SQLDatabase.from_url(url="sqlite://", models=(Hero,))
    db.finalize()
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

    db = SQLDatabase.from_url(url="sqlite://", models=(Hero,))
    db.finalize()

    with pytest.raises(NotImplementedError):
        with db.sqlsession() as session:
            session.add(hero)
            raise NotImplementedError('This is bad omen.')

    with db.sqlsession() as session:
        statement = select(Hero).where(Hero.name == "Deadpond")
        hero = session.exec(statement).first()
        assert hero is None
