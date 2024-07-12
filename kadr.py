import glob
import os
import datetime
import time
from main import DBCommands
import asyncio
import xml.etree.ElementTree as ET

db = DBCommands()

list_of_files = glob.glob('/home/BLOOD.LOCAL/admin_sz/Projects/etl/*.xml')
latest_file = max(list_of_files, key=os.path.getctime)

def remove_upper_lines(doc, n):
    if n <= len(doc):
        return doc[n::]
    else:
        return doc

tree = ET.parse(latest_file)
root = tree.getroot()
async def import_workers():     
    circle = 0
    while len(root) > circle:
        per_i = 0
        while per_i < 10:
            name = root[per_i+circle].get('ФИО')
            per_i+=1
            birthday = root[per_i+circle].get('ДатаРождения')[:-9]
            per_i+=1
            position = root[per_i+circle].get('Должность')
            if position == '':
                break
            per_i+=1
            department = root[per_i+circle].get('Подразделение')
            if department == '':
                break
            per_i+=1
            employment = root[per_i+circle].get('ВидЗанятости')
            per_i+=1
            date_start = root[per_i+circle].get('ДатаПриема')[:-9]
            per_i+=1
            date_expire = root[per_i+circle].get('ДатаУвольнения')[:-9]
            per_i+=3
            status = root[per_i+circle+1].get('Статус')
            workman_id = await db.get_worker_id(name)
            exist_workman = await db.check_worker(name)
            if date_start == '0001-01-01':
                    date_start = '1980-01-01'
            date_start = datetime.datetime.strptime(date_start.replace('-', ''), '%Y%m%d').date()
            birthday = datetime.datetime.strptime(birthday.replace('-', ''), '%Y%m%d').date()
            if exist_workman and status == 'Работающий':
                exist_position = await db.check_worker_position(name, position, department)
                if exist_position:
                    await db.check_workplace_data(name, position, department, date_start, employment)
                    break                    
                else:
                    await db.join_position(workman_id[0][0], \
                                            position, department, date_start,\
                                            employment)
            elif not exist_workman and status != 'Уволен':
                await db.add_new_worker(name, birthday, \
                                           position,department, \
                                            date_start, employment)
            if exist_workman and status == 'Уволен':
                date_expire = datetime.datetime.strptime(date_expire.replace('-', ''), '%Y%m%d').date()
                await db.leave_position(workman_id[0][0], position, department, date_expire)
            per_i+=1
        circle+=11


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(import_workers())
    end_time = time.time()
    elapsed_time = end_time - start_time
    print('Elapsed time: ', elapsed_time)