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
    await db.pre_expire()
    circle = 0
    while len(root) > circle:        
        per_i = 0
        while per_i < 10:
            name = root[per_i+circle].get('ФИО')
            per_i+=2
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
            per_i+=1
            old_snils = root[per_i+circle].get('СНИЛС')
            per_i+=1
            email = root[per_i+circle+1].get('АдресЭлектроннойПочты')
            per_i+=1
            status = root[per_i+circle+1].get('Статус')
            workman_id = await db.get_worker_id(name)
            try:
                snils = hex(int((old_snils.replace('-', '')).replace(' ', '')))
            except ValueError:
                pass
            if date_start == '0001-01-01':
                    date_start = '1980-01-01'
            date_start = datetime.datetime.strptime(date_start.replace('-', ''), '%Y%m%d').date()
            if status == 'Работающий':
                exist_position = await db.check_worker_position(name, position, department, employment)      
                if exist_position:
                    await db.prolongate_working(workman_id[0][0], department, position, employment)                  
                else:
                    if snils != '':
                        employer = await db.get_worker_with_snils(snils)
                        if len(employer) != 0:
                            if name != employer[0][1]:
                                print(name, snils, position,department, date_start, employment, email)
                                print(exist_position)
                                await db.edit_fio(employer[0][0], name, employer[0][1])
                            else:
                                await db.join_position(snils, \
                                                    position, department, date_start,\
                                                    employment, name)
                        else:                            
                            await db.add_new_worker(name, snils, \
                                                       position,department, \
                                                        date_start, employment, email)
                    else:
                        exist_worker = await db.check_worker(name)
                        if exist_worker:
                            await db.join_position(name, \
                                                    position, department, date_start,\
                                                    employment, name)
                        else:
                            await db.add_new_worker(name, snils, \
                                                       position,department, \
                                                        date_start, employment, email)
            elif status in ('Отпуск по уходу за ребенком', 'Отпуск по беременности и родам'):              
                try:
                    new_date_expire = datetime.datetime.strptime(date_expire.replace('-', ''), '%Y%m%d').date()
                    await db.dekret(workman_id[0][0], position, department, new_date_expire, employment)
                except:
                    pass
            elif status == 'Уволен':
                try:
                    new_date_expire = datetime.datetime.strptime(date_expire.replace('-', ''), '%Y%m%d').date()
                    await db.add_expire(workman_id[0][0], position, department, new_date_expire, employment)
                except:
                    pass
            per_i+=1
        circle+=11
    await db.result_expired()
    #await db.create_message_expire()
    





if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(import_workers())
    end_time = time.time()
    elapsed_time = end_time - start_time
    print('Elapsed time: ', elapsed_time)