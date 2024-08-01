import csv
import openpyxl, asyncio
from main import DBCommands

db = DBCommands()

path = ('/home/BLOOD.LOCAL/admin_sz/Общая папка с виндой/ad/')



async def verify_active_directory_data():
    with open(path+'users.csv', encoding='UTF8') as r_file:
        ad = csv.reader(r_file, delimiter=',')
        for row in ad:
            try:
                worker_id = None                
                if row[0].count(' ') == 2:                    
                    name = row[0]
                    worker = await db.get_worker_id(name)
                    if len(worker) > 0:
                        worker_id = worker[0][0]
                elif 0 < row[0].count(' ') < 2:                    
                    email = row[1]
                    worker = await db.get_worker_with_email(email)
                    if len(worker) > 0:
                        worker_id = worker[0][0]
                if worker_id != None:                    
                    email = row[1]
                    ad_account = row[4]
                    phone = row[3]
                    name = row[0]
                    if email != '':
                        await db.edit_email(int(worker_id), email)
                    if phone != '':
                        phone = phone.replace('-', '')
                        await db.add_telephone(int(worker_id), phone, name)
                    await db.add_ad(int(worker_id), ad_account)
            except IndexError:
                pass
                


if __name__ == "__main__":
    asyncio.run(verify_active_directory_data())