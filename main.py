from aiogram.fsm.storage.memory import MemoryStorage
from config import API_TOKEN
from aiogram import Bot, Dispatcher
from handlers import router


# Создание объектов бота и диспетчера
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
dp.include_router(router)


async def main():
    await dp.start_polling(bot)


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())