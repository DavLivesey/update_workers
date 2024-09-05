import openpyxl, asyncio
from main import DBCommands

db = DBCommands()




async def verify_mis_permissions():
    await db.nullify_tis_spk()
    await db.change_tis_spk()


if __name__ == "__main__":
    asyncio.run(verify_mis_permissions())