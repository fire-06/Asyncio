import asyncio
import aiohttp

from more_itertools import chunked

from models import Session, SwapiPerson, init_db, close_db

CHUNK_SIZE = 10
MAX_RETIES = 3

# список полей модели персонаже, для которых надо получить названия дочерних записей через запятую
DETAIL_FIELDS = [
    {'field': 'films', 'detail_field': 'title'},
    {'field': 'species', 'detail_field': 'name'},
    {'field': 'starships', 'detail_field': 'name'},
    {'field': 'vehicles', 'detail_field': 'name'},
]


# вставить данные о персонаже в БД
async def insert_people(people_list):
    swapi_people_list = []
    for person in people_list:
        try:
            swapi_person = SwapiPerson(**person)
            swapi_people_list.append(swapi_person)
        except Exception as err:
            print(f'Возникла ошибка при обработке персонажа {person}:\n{err}')

    async with Session() as session:
        session.add_all(swapi_people_list)
        await session.commit()


# получить JSON-данные по заданному URL
async def get_json_by_url(url):
    async with aiohttp.ClientSession() as session:
        retires = 0
        while retires < MAX_RETIES:
            try:
                response = await session.get(url)
                json_data = await response.json()
                return json_data
            except Exception as err:
                print(f'Ошибка при обращении к swapi (попытка {retires}):\n{err}')
                retires += 1
        print(f'Возникла ошибка при обращении к swapi по следующей ссылке:\n{url}')


# получить строку с названиями дочерних записей через запятую
# (названия фильмов, кораблей и т.п.)
async def get_details(url_list, field_name):
    coros = [get_json_by_url(detail_url) for detail_url in url_list]
    details_response_list = await asyncio.gather(*coros)
    details = [detail.get(field_name) for detail in details_response_list]

    return ','.join(details)


async def get_person_info(person_id):
    print(f'Получение сведений о персонаже с id={person_id}...')
    json_response = await get_json_by_url(
        f'https://swapi.dev/api/people/{person_id}/'
    )
    if json_response is None or (json_response.get('detail', False) == 'Not found'):
        return None

    json_response.pop('url', None)
    json_response.pop('created', None)
    json_response.pop('edited', None)

    # получить названия дочерних записей
    for details in DETAIL_FIELDS:
        if details['field'] in json_response:
            json_response[details['field']] = await get_details(
                json_response[details['field']],
                details['detail_field']
            )
    print(f'Сведения о персонаже с id={person_id} получены')

    return json_response


async def main():
    await init_db()
    try:
        for person_id_chunk in chunked(range(1, 100), CHUNK_SIZE):
            coros = [get_person_info(person_id) for person_id in person_id_chunk]
            people_list = await asyncio.gather(*coros)
            asyncio.create_task(insert_people(people_list))
        tasks = asyncio.all_tasks() - {asyncio.current_task()}
        await asyncio.gather(*tasks)
    finally:
        await close_db()


if __name__ == '__main__':
    asyncio.run(main())