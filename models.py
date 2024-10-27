import os
import asyncio
from dotenv import load_dotenv
from sqlalchemy import Integer, String
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
import httpx  # Для выполнения HTTP-запросов к API SWAPI

load_dotenv()

# Настройки базы данных
PG_PASSWORD = os.getenv("PG_PASSWORD", "mypassword")
PG_USER = os.getenv("PG_USER", "myuser")
PG_DB = os.getenv("PG_DB", "swapi_db")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")

PG_DSN = f"postgresql+asyncpg://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Создание асинхронного движка
engine = create_async_engine(PG_DSN, echo=True)
Session = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase, AsyncAttrs):
    pass


class SwapiPerson(Base):
    __tablename__ = 'swapi_person'

    id: Mapped[int] = mapped_column(primary_key=True)
    birth_year: Mapped[str] = mapped_column(String(1000), nullable=True)
    eye_color: Mapped[str] = mapped_column(String(1000), nullable=True)
    films: Mapped[str] = mapped_column(String(1000), nullable=True)
    gender: Mapped[str] = mapped_column(String(1000), nullable=True)
    hair_color: Mapped[str] = mapped_column(String(1000), nullable=True)
    height: Mapped[str] = mapped_column(String(1000), nullable=True)
    homeworld: Mapped[str] = mapped_column(String(1000), nullable=True)
    mass: Mapped[str] = mapped_column(String(1000), nullable=True)
    name: Mapped[str] = mapped_column(String(1000))
    skin_color: Mapped[str] = mapped_column(String(1000), nullable=True)
    species: Mapped[str] = mapped_column(String(1000), nullable=True)
    starships: Mapped[str] = mapped_column(String(1000), nullable=True)
    vehicles: Mapped[str] = mapped_column(String(1000), nullable=True)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


async def close_db():
    await engine.dispose()


async def fetch_person_data(url):
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        response.raise_for_status()  # Проверка на ошибки HTTP
        return response.json()


async def process_person_data(person_data):
    if person_data is None:
        print("Данные персонажа отсутствуют.")
        return

    try:
        person = SwapiPerson(**person_data)
        async with Session() as session:
            session.add(person)
            await session.commit()
            print(f"Персонаж {person.name} добавлен в базу данных.")
    except Exception as e:
        print(f"Ошибка при обработке персонажа: {e}")


async def get_people_from_api():
    url = "https://swapi.dev/api/people/"
    data = await fetch_person_data(url)
    return data.get('results', [])


async def main():
    await init_db()

    try:
        people = await get_people_from_api()
        for person_data in people:
            await process_person_data(person_data)
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
