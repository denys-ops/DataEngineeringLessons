import asyncio

import uvicorn
from fastapi import FastAPI

from lesson_02.apis.collect_api.router import collect_router
from lesson_02.apis.transform_api.router import transform_router

get_app = FastAPI(
    title="DataEngineeringAPI1",
    version="0.0.1"
)
get_app.include_router(collect_router)

transform_app = FastAPI(
    title="DataEngineeringAPI1",
    version="0.0.1"
)
transform_app.include_router(transform_router)


async def run_server(app, host, port):
    config = uvicorn.Config(app=app, host=host, port=port)
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    # Створюємо завдання для кожного сервера
    task1 = asyncio.create_task(run_server(get_app, "127.0.0.1", 8081))
    task2 = asyncio.create_task(run_server(transform_app, "127.0.0.1", 8082))

    # Виконуємо завдання одночасно
    await asyncio.gather(task1, task2)


# Викликаємо асинхронну головну функцію
if __name__ == "__main__":
    asyncio.run(main())
