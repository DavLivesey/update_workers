import openpyxl, asyncio
from main import DBCommands

db = DBCommands()




async def verify_mis_permissions():
    await db.nullify_mis()
    await db.nullify_tis()
    await db.change_mis()


if __name__ == "__main__":
    asyncio.run(verify_mis_permissions())