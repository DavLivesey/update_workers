import openpyxl, asyncio
from main import DBCommands

db = DBCommands()

path = ('/home/BLOOD.LOCAL/admin_sz/Projects/test_uploads/')

exel = openpyxl.load_workbook(path+'TIS_users.xlsx')
tis = exel['Лист1']


async def verify_tis_permissions():
    for number_row in range(1, len(tis['A'])):
            worker_id = await db.get_worker_id_with_ad(tis.cell(row=number_row, column=4).value)
            if len(worker_id)>0:
                await db.plus_TIS(int(worker_id[0][0]))


if __name__ == "__main__":
    asyncio.run(verify_tis_permissions())