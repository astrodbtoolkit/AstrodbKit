# Tests for views
# Adapted from https://github.com/sqlalchemy/sqlalchemy/wiki/Views

import sqlalchemy as sa
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import Session
from astrodbkit2.views import *


def test_views():
    engine = sa.create_engine("sqlite://", echo=True)
    metadata = sa.MetaData()
    stuff = sa.Table(
        "stuff",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("data", sa.String(50)),
    )

    more_stuff = sa.Table(
        "more_stuff",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("stuff_id", sa.Integer, sa.ForeignKey("stuff.id")),
        sa.Column("data", sa.String(50)),
    )

    # the .label() is to suit SQLite which needs explicit label names
    # to be given when creating the view
    # See http://www.sqlite.org/c3ref/column_name.html
    stuff_view = view(
        "stuff_view",
        metadata,
        sa.select(
            stuff.c.id.label("id"),
            stuff.c.data.label("data"),
            more_stuff.c.data.label("moredata"),
        )
        .select_from(stuff.join(more_stuff))
        .where(stuff.c.data.like(("%orange%"))),
    )

    assert stuff_view.primary_key == [stuff_view.c.id]

    with engine.begin() as conn:
        metadata.create_all(conn)

    with engine.begin() as conn:
        conn.execute(
            stuff.insert(),
            [
                {"data": "apples"},
                {"data": "pears"},
                {"data": "oranges"},
                {"data": "orange julius"},
                {"data": "apple jacks"},
            ],
        )

        conn.execute(
            more_stuff.insert(),
            [
                {"stuff_id": 3, "data": "foobar"},
                {"stuff_id": 4, "data": "foobar"},
            ],
        )

    with engine.connect() as conn:
        assert conn.execute(
            sa.select(stuff_view.c.data, stuff_view.c.moredata)
        ).all() == [("oranges", "foobar"), ("orange julius", "foobar")]

    # illustrate ORM usage
    Base = declarative_base(metadata=metadata)

    class MyStuff(Base):
        __table__ = stuff_view

        def __repr__(self):
            return f"MyStuff({self.id!r}, {self.data!r}, {self.moredata!r})"

    with Session(engine) as s:
        assert s.query(MyStuff).count() == 2