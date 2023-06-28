import json
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.orm import Mapped, mapped_column

with open("okved_2.json", "r", encoding="utf-8") as f:
    data = json.load(f)
# print(data)

DB_USER = "postgres"
DB_NAME = "hw1"
DB_PASSWORD = "*****"  # Необходимо подставить свой пароль
DB_HOST = "127.0.0.1"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"
print(DATABASE_URL)


class Base(DeclarativeBase):
    pass


class Okved(Base):
    __tablename__ = "okved"
    code: Mapped[str] = mapped_column(primary_key=True)
    parent_code: Mapped[str]
    section: Mapped[str]
    name: Mapped[str]
    comment: Mapped[str]

    def __repr__(self):
        return f"{self.id}, {self.code}, {self.parent_code}, {self.section}, {self.name}, {self.comment}"


engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)
print()

with Session(engine) as session:
    for record in data:
        data_ins = Okved(
            code=record["code"],
            parent_code=record["parent_code"],
            section=record["section"],
            name=record["name"],
            comment=record["comment"],
        )
        session.add(data_ins)
    session.commit()
print("Завершено!")