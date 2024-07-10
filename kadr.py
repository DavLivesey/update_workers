import glob
import os
import datetime, time
from main import DBCommands
import asyncio

db = DBCommands()

list_of_files = glob.glob('/home/BLOOD.LOCAL/admin_sz/Projects/etl/*.xml')
latest_file = max(list_of_files, key=os.path.getctime)

def remove_upper_lines(doc, n):
    if n <= len(doc):
        return doc[n::]
    else:
        return doc



with open(latest_file, "r") as document:
    lines = document.readlines()
    new_doc = remove_upper_lines(lines, 2)
    person = 1
    index = 0
    list_workers = []
  
    while index < len(new_doc)-1:
            worker = {'ФИО': '',              
              'Дата рождения': '',
              'Должность': '',
              'Подразделение': '',
              'Вид занятости': '',              
              'Дата приёма': '',
              'Дата увольнения': '',
              'Статус': '',}
            worker['ФИО'] = new_doc[index][11:-4]
            worker['Дата рождения'] = new_doc[index+1][29:-13]
            worker['Должность'] = new_doc[index+2][23:-4]
            worker['Подразделение'] = new_doc[index+3][31:-4]
            worker['Вид занятости'] = new_doc[index+4][29:-4]
            worker['Дата приёма'] = new_doc[index+5][25:-13]
            worker['Дата увольнения'] = new_doc[index+6][33:-13]
            worker['Статус'] = new_doc[index+10][17:-4]
            list_workers.append(worker)
            index += 11
            person += 1

async def import_workers(): 
    for workman in list_workers:
        name = workman['ФИО']
        exist_workman = await db.check_worker(name)
        workman_id = await db.get_worker_id(name)
        if exist_workman and workman['Статус'] == 'Работающий':
            if workman['Дата приёма'] == '0001-01-01':
                workman['Дата приёма'] = '1980-01-01'
            date_start = datetime.datetime.strptime(workman['Дата приёма'].replace('-', ''), '%Y%m%d').date()
            birthday = datetime.datetime.strptime(workman['Дата рождения'].replace('-', ''), '%Y%m%d').date()
            positions = await db.view_worker_position(name)
            dep = [pos for pos in positions if workman['Подразделение'] in pos[1]]
            try:
                if dep[0][2] != workman['Должность'] and dep[0][1] == workman['Подразделение']:
                    await db.join_position(workman_id[0][0], \
                                           workman['Должность'], dep[0][1], date_start,\
                                            workman['Вид занятости'])
                elif dep[0][1] != workman['Подразделение'] and dep[0][2] == workman['Должность']:
                    await db.join_position(workman_id[0][0], \
                                           dep[0][1], workman['Подразделение'], date_start,\
                                            workman['Вид занятости'])
                else:
                    await db.join_position(workman_id[0][0], \
                                           workman['Должность'], workman['Подразделение'], date_start,\
                                            workman['Вид занятости'])
                if dep[0][3] == None or dep[0][4] == None:
                    await db.edit_position(workman_id[0][0], \
                                           workman['Должность'], workman['Подразделение'], date_start,\
                                            workman['Вид занятости'])
            except IndexError:
                pass
            if dep == []:
                await db.join_position(workman_id[0][0], \
                                       workman['Должность'],workman['Подразделение'], \
                                        date_start, workman['Вид занятости'])
        elif not exist_workman:
            await db.add_new_worker(workman['ФИО'], birthday, \
                                       workman['Должность'],workman['Подразделение'], \
                                        date_start, workman['Вид занятости'])
        if exist_workman and workman['Статус'] == 'Уволен':
            date_expire = datetime.datetime.strptime(workman['Дата увольнения'].replace('-', ''), '%Y%m%d').date()
            await db.leave_position(workman_id[0][0], workman['Должность'], workman['Подразделение'], date_expire)




if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(import_workers())
    end_time = time.time()
    elapsed_time = end_time - start_time
    print('Elapsed time: ', elapsed_time)