#Здесь код взаимодействия непосредственно с БД
from sql import DataBase
from logging import getLogger
from datetime import datetime as dt

LOG = getLogger()

class DBCommands:
    #Блок изменения информации о работниках
    ADD_NEW_WORKER = 'INSERT INTO workers (fullname, birthday) VALUES ($1, $2)'
    ADD_DEP = 'UPDATE workers w SET "department"=$2 WHERE id=$1'
    DELETE_WORKER = 'DELETE FROM workers WHERE id=$1'
    GET_WORKER_ID = 'SELECT id FROM workers w WHERE w.fullname=$1'
    VIEW_WORKER = 'SELECT * FROM workers w WHERE w.fullname LIKE $1' 
    VIEW_WORKER_ON_ID = 'SELECT * FROM workers WHERE id=$1'
    VIEW_WORKER_POSITIONS = 'SELECT w.fullname, d.dep_name, p.pos_name, wp.date_start, wp.employment FROM workplaces wp ' \
                            'JOIN workers w ON wp.worker_id = w.id ' \
                            'JOIN departments d ON wp.dep_id =d.id ' \
                            'JOIN positions p ON wp.pos_id =p.id ' \
                            'WHERE w.fullname = $1'
    VIEW_WORKER_POSITIONS_ID = 'SELECT w.fullname, d.dep_name, p.pos_name FROM workplaces wp ' \
                            'JOIN workers w ON wp.worker_id = w.id ' \
                            'JOIN departments d ON wp.dep_id =d.id ' \
                            'JOIN positions p ON wp.pos_id =p.id ' \
                            'WHERE w.id = $1'    
    CHECK_WORKER = 'SELECT EXISTS (SELECT * FROM workers w WHERE w.fullname=$1)'
    FIND_WORKER = 'SELECT fullname FROM workers WHERE id = $1'
    EDIT_WORKER_FIO = 'UPDATE workers w SET "fullname"=$2 WHERE id=$1'
    ADD_APTEKA = "UPDATE workers SET APTEKA='Да' where id=$1"
    ADD_HR = "UPDATE workers SET ZKGU='Да' where id=$1"
    ADD_BGU_1 = "UPDATE workers SET BGU_1='Да' where id=$1"
    ADD_BGU_2 = "UPDATE workers SET BGU_2='Да' where id=$1"
    ADD_DIETA = "UPDATE workers SET DIETA='Да' where id=$1"
    ADD_MIS = "UPDATE workers SET MIS='Да' where id=$1"
    ADD_TIS = "UPDATE workers SET TIS='Да' where id=$1"
    ADD_SED = "UPDATE workers SET SED='Да' where id=$1"
    DELETE_APTEKA = "UPDATE workers SET APTEKA='Нет' where id=$1"
    DELETE_HR = "UPDATE workers SET ZKGU='Нет' where id=$1"
    DELETE_BGU_1 = "UPDATE workers SET BGU_1='Нет' where id=$1"
    DELETE_BGU_2 = "UPDATE workers SET BGU_2='Нет' where id=$1"
    DELETE_DIETA = "UPDATE workers SET DIETA='Нет' where id=$1"
    DELETE_MIS = "UPDATE workers SET MIS='Нет' where id=$1"
    DELETE_TIS = "UPDATE workers SET TIS='Нет' where id=$1"
    DELETE_SED = "UPDATE workers SET SED='Нет' where id=$1"
    EDIT_EMAIL = 'UPDATE workers SET EMAIL=$2 where id=$1'
    ADD_AD = 'UPDATE workers SET "ad"=$2 WHERE id=$1'
    ADD_NEW_POSITION = 'INSERT INTO positions (pos_name) VALUES ($1)'
    JOIN_POSITION = 'INSERT INTO workplaces (worker_id, pos_id, dep_id, date_start, employment) ' \
                    'VALUES ($1, (select id FROM positions p WHERE p.pos_name=$2), '\
                    '(SELECT id FROM departments d WHERE d.dep_name=$3), $4, $5)'
    CHECK_WORKER_POSITION = 'SELECT EXISTS (SELECT * from workplaces wp where '\
                            'wp.worker_id=(SELECT id FROM workers w WHERE w.fullname=$1) '\
                            'and wp.pos_id=(SELECT id FROM positions p WHERE p.pos_name=$2) '\
                            'and wp.dep_id=(SELECT id FROM departments d WHERE d.dep_name=$3))'
    CHECK_WORKPLACE_DATA = 'SELECT * from workplaces wp where '\
                            'wp.worker_id=(SELECT id FROM workers w WHERE w.fullname=$1) '\
                            'and wp.pos_id=(SELECT id FROM positions p WHERE p.pos_name=$2) '\
                            'and wp.dep_id=(SELECT id FROM departments d WHERE d.dep_name=$3)'
    UPDATE_WORKPLACE = 'UPDATE workplaces wp SET date_start=$2, employment=$3 '\
                        'WHERE wp.id=$1'
    CHECK_IS_POSITION = 'SELECT EXISTS (SELECT * FROM positions p WHERE p.pos_name=$1)'
    EDIT_POSITION = 'update workplaces wp set date_start=$4, employment=$5 '\
                    'WHERE wp.worker_id=$1 and '\
                    'wp.pos_id=(SELECT id FROM positions p WHERE p.pos_name=$2) and '\
                    'wp.dep_id=(SELECT id FROM departments d WHERE d.dep_name=$3)'
    LEAVE_POSITION = 'UPDATE workplaces wp set expired=True, date_expire=$4 '\
                     'WHERE wp.worker_id=$1 and '\
                     'wp.pos_id=(SELECT id FROM positions p WHERE p.pos_name=$2) and '\
                     'wp.dep_id=(SELECT id FROM departments d WHERE d.dep_name=$3)'
    ADD_NEW_DEP = 'INSERT INTO departments (dep_name) VALUES ($1)'
    CHECK_IS_DEP = 'SELECT EXISTS (SELECT * FROM departments WHERE dep_name LIKE $1)'
    
    #Блок работы с сертификатами
    ADD_NEW_SERT = 'INSERT INTO sertificates (worker_id, center_name, serial_number, date_start, date_finish)' \
                        'VALUES ($1, $2, $3, $4, $5)'
    CHECK_SERT = 'SELECT * FROM sertificates s JOIN workers w ON w.id=$1 WHERE s.worker_id=$1'
    CHECK_SERT_FIN = "SELECT worker_id, center_name, serial_number, DATE_FINISH FROM sertificates "\
                     "WHERE current_date + interval '30 day' > DATE_FINISH"    
    PASSED_FLESH = 'UPDATE sertificates SET "presence"=false where worker_id=$1'
    #Блок работы с пользователями
    ADD_NEW_USER = 'INSERT INTO users (first_name, last_name, username, user_id, role) VALUES ($1, $2, $3, $4, $5)'
    CHECK_USER = 'SELECT EXISTS (SELECT * FROM users u WHERE u.user_id=$1)'
    CHECK_USER_ROLE = 'SELECT role from users u WHERE u.user_id=$1'
    UPDATE_USER_ROLE = 'UPDATE users u SET "role"=$2 where u.user_id=$1'
    BAN_USER = 'UPDATE users u SET "ban"=true where u.user_id=$1'
    UNBAN_USER = 'UPDATE users u SET "ban"=false where u.user_id=$1'
    CHECK_USER_BAN = 'SELECT ban from users u WHERE u.user_id=$1'
    
    async def get_all_workers(self):
        workers_list = await DataBase.execute(self.GET_ALL_WORKERS, fetch=True)
        return workers_list
    
    async def read_worker(self, person):
        #Функция получает лист информации о работнике и формирует несколько списков:
        #1. person - тот же список, что входит в функцию
        #2.list_person - список доступов работника
        #3.list_sert - список сертификатов работника
        #4.contacts_list - список контактов открытого доступа (телефон, электронная почта)
        #5.sec_list - список закрытых контактов (учетка в компьютер, участие в почтовых рассылках)
            access_dict = {
                '1С_Аптека': 'Нет',
                '1С_Диетпитание': 'Нет',
                '1С_ЗКГУ': 'Нет',
                '1С_БГУ 1.0': 'Нет',
                '1С_БГУ 2.0': 'Нет',
                'СЭД': 'Нет',
                'МИС': 'Нет',
                'ТИС': 'Нет',
                'email': 'Нет',
                'ad': 'Нет',
                }
            phone_arg = person[1]
            phone_command = self.GET_PHONE
            phone_list = []
            telephones = await DataBase.execute(phone_command, phone_arg, fetch=True)
            mailbox_arg = person[1]
            mailbox_command = self.GET_MAILBOX
            mailbox_list = []
            mailboxes = await DataBase.execute(mailbox_command, mailbox_arg, fetch=True)
            if len(telephones) == 1:
                phone_list.append(f'телефон:')
            elif len(telephones) > 1:
                phone_list.append(f'телефоны:')
            for phone in telephones:
                phone_list.append(phone[0])
            if len(mailboxes) == 1:
                mailbox_list.append(f'почтовая рассылка:')
            elif len(mailboxes) > 1:
                mailbox_list.append(f'почтовые рассылки:')
            for mail in mailboxes:
                mailbox_list.append(mail[0])
            person_list = []
            list_sert = []
            list_person = []
            arg_sert = int(person[0])
            command_sert = self.CHECK_SERT
            worker_sert = await DataBase.execute(command_sert, arg_sert, fetch=True)                    #Проверяем сертификаты работника
            sert_list = []                                                                              #Список сертификатов работника
            contacts_list = []                                                                          #Список общедоступных контактов работника
            sec_list = []                                                                               #Список контактов работника, скрытых от обычных пользователей
            sert_num = 1
            for sert in worker_sert:                                                                    #Для каждого сертификта формируем отдельную читабельную карточку
                if sert[6]==True:
                    date_start = dt.strftime(sert[4], '%d-%m-%Y')
                    date_finish = dt.strftime(sert[5], '%d-%m-%Y')
                    if sert[5] < dt.today().date():
                        date_item = '❌'
                    else:
                        date_item = '✅'
                    sert_list.append(f'{date_item}№{sert_num}')
                    sert_list.append(f'УЦ - {sert[2]}')
                    sert_list.append(f'серийный номер - {sert[3]}')
                    sert_list.append(f'Начало действия - {date_start}')
                    sert_list.append(f"Окончание действия - {date_finish}\n")
                    sert_num += 1
            access_dict['email'] = person[10]
            access_dict['1С-Аптека'] = person[2]
            access_dict['1С_Диетпитание'] = person[6]
            access_dict['1С_ЗКГУ'] = person[3]
            access_dict['1С_БГУ 1.0'] = person[4]
            access_dict['1С_БГУ 2.0'] = person[5]
            access_dict['СЭД'] = person[9]
            access_dict['МИС'] = person[7]
            access_dict['ТИС'] = person[8]
            access_dict['ad'] = person[13]
            for key, value in access_dict.items():                                                 #Если в словаре access_dict значение было изменено,
                if value != 'Нет' and value != '':                                                 #добавляет в список название ключа
                    person_list.append(key)
            if 'email' in person_list:                                                             #Если в списке оказалась почта, в список contacts_list
                contacts_list.append(f'email -  {access_dict["email"]}')                           #добавляется текст с ее значением, а из списка person_list email удаляется
                person_list.remove('email')
            if 'ad' in person_list:                                                                 #Если в списке имеется запись об учетке Active Directory, в список sec_list
                sec_list.append(f'AD: <code>{access_dict["ad"]}</code>')                            #добавляется текст с его значением, а из списка person_list ad удаляется
                person_list.remove('ad')
            list_sert = '\n'.join(sert_list)
            list_person = '\n'.join(person_list)
            return person, list_person, list_sert, contacts_list, sec_list, phone_list, mailbox_list
            
    async def make_answer(self, person, list_person, list_sert, contacts_list, sec_list, phone_list,\
                           mailbox_list, position):
        #Создание карточки ответа по сотруднику
        full_roles = ['admin', 'security', 'superuser']
        result_contacts = ''
        result_sec = ''
        result_positions = ''
        result_phones = ''
        result_mailboxes = ''
        for mail in mailbox_list:
            result_mailboxes +=f'{mail}\n'
        for phone in phone_list:
            result_phones += f'{phone}\n'
        for contact in contacts_list:
            result_contacts += f'{contact}\n'
        for sec in sec_list:
            result_sec += f'{sec}\n'
        for pos in position:
            result_positions += f'{pos[1]}\n<i>{pos[2]}</i>\n\n'
        message = f'{person[1]}\n{result_positions}\n{result_contacts}\n{result_phones}\n{result_mailboxes}\n'\
                                            f'{result_sec}\n\n<b>Доступы:</b>\n{list_person}\n\n' \
                                           f'Сертификаты:\n{list_sert}'
        return message

    async def view_worker_position(self, fullname):
        arg = fullname
        command = self.VIEW_WORKER_POSITIONS
        result = await DataBase.execute(command, arg, fetch=True)
        return result
    
    async def get_worker_id(self, fullname):
        arg = fullname
        command = self.GET_WORKER_ID
        result = await DataBase.execute(command, arg, fetch=True)
        return result
    
    async def add_new_worker(self, fullname, birthday, pos_name, dep_name, date_start, employment):
        worker_args = (fullname, birthday)                       
        worker_command = self.ADD_NEW_WORKER
        workplace_command = self.JOIN_POSITION
        await DataBase.execute(worker_command, *worker_args, execute=True)
        worker_id = await DataBase.execute(self.GET_WORKER_ID, fullname, fetch=True)
        workplace_args = (int(worker_id[0][0]), pos_name, dep_name, date_start, employment)
        await DataBase.execute(workplace_command, *workplace_args, execute=True)

        

    async def check_worker(self, fullname):
        #Проверка наличия записи о сотруднике по ФИО
        arg = fullname
        command = self.CHECK_WORKER
        result = await DataBase.execute(command, arg, fetchval=True)
        return result

    async def view_worker(self, fullname):
        #Просмотр карточки работника без ID
        arg = f"%{fullname}%"
        view_command = self.VIEW_WORKER
        position_command = self.VIEW_WORKER_POSITIONS
        worker = await DataBase.execute(view_command, arg, fetch=True) 
        if len(worker) > 0:                       
            for work in worker:
                position = await DataBase.execute(position_command, work[1], fetch=True)
                reading_result = await self.read_worker(work)
                message = await self.make_answer(reading_result[0], reading_result[1], reading_result[2], \
                                                 reading_result[3], reading_result[4], reading_result[5], \
                                                reading_result[6], position)                
            return True        
    
    async def view_worker_with_id(self, fullname):
        #Просмотр карточки работника с ID. Требуется для функций редактирования
        arg = f"%{fullname}%"
        view_command = self.VIEW_WORKER
        position_command = self.VIEW_WORKER_POSITIONS
        worker = await DataBase.execute(view_command, arg, fetch=True)         
        if len(worker) > 0:
            persons_id_list = []
            for work in worker:
                position = await DataBase.execute(position_command, work[1], fetch=True)        
                reading_result = await self.read_worker(work)
                message = await self.make_answer(reading_result[0], reading_result[1], reading_result[2], \
                                                 reading_result[3], reading_result[4], reading_result[5],\
                                                     reading_result[6], position)
                persons_id_list.append(reading_result[0][0])
            return persons_id_list
        else:
            return False

#Блок работы с информацией о работнике
    async def edit_email(self, id, email):
        args = int(id), email
        command = self.EDIT_EMAIL
        await DataBase.execute(command, *args, execute=True)
    
    async def edit_fio(self, worker_id, fio):
        command = self.EDIT_WORKER_FIO
        args = (int(worker_id), fio)
        await DataBase.execute(command, *args, execute=True)

    async def add_department(self, worker_id, dep):
        command = self.ADD_DEP
        args = (int(worker_id), dep)
        await DataBase.execute(command, *args, execute=True)

    
    async def del_worker(self, id):
        arg = int(id)
        command = self.DELETE_WORKER
        await DataBase.execute(command, arg, execute=True)
        return True
    
    async def check_position(self, pos_name):
        arg = pos_name
        command = self.CHECK_IS_POSITION
        result = await DataBase.execute(command, arg, fetchval=True)
        return result
    
    async def check_dep(self, dep_name):
        arg = f"%{dep_name}%"
        command = self.CHECK_IS_DEP
        result = await DataBase.execute(command, arg, fetchval=True)
        return result

    async def check_worker_position(self, name, position, department):
        command = self.CHECK_WORKER_POSITION
        args = (name, position, department)
        result = await DataBase.execute(command, *args, fetchval=True)
        return result
    
    async def check_workplace_data(self, name, position, department, date_start, employment):
        command = self.CHECK_WORKPLACE_DATA
        args = (name, position, department)
        result = await DataBase.execute(command, *args, fetch=True)
        if result[0][5] == None or result[0][7] == '':
                        command = self.UPDATE_WORKPLACE
                        args = (result[0][0], date_start, employment)
                        await DataBase.execute(command, *args, execute=True)


    async def join_position(self, worker_id, pos_name, dep_name, date_start, employment):
        # Функция добавляет сотруднику должность в подразделении, если таковые существуют
        check_pos_arg = pos_name
        check_dep_arg = dep_name
        check_pos_command = self.CHECK_IS_POSITION
        check_dep_command = self.CHECK_IS_DEP
        dep_exist = await DataBase.execute(check_dep_command, check_dep_arg, fetchval=True)
        pos_exist = await DataBase.execute(check_pos_command, check_pos_arg, fetchval=True)
        print(dep_name, dep_exist)
        if dep_exist == False:
            print(f'dep {dep_name} added')
            await DataBase.execute(self.ADD_NEW_DEP, dep_name, execute=True)
        if pos_exist == False:
            await DataBase.execute(self.ADD_NEW_POSITION, pos_name, execute=True)
        args = (int(worker_id), pos_name, dep_name, date_start, employment)
        command = self.JOIN_POSITION
        await DataBase.execute(command, *args, execute=True)

    async def edit_position(self, worker_id, pos_name, dep_name, date_start, employment):
        args = (worker_id, pos_name, dep_name, date_start, employment)
        command = self.EDIT_POSITION
        await DataBase.execute(command, *args, execute=True)
    
    async def leave_position(self, worker_id, pos_name, dep_name, date_finish):
        # Функция убирает сотруднику должность в подразделении
        existing = 0
        pos_exist = await self.check_position(pos_name)
        dep_exist = await self.check_dep(dep_name)
        if pos_exist:
            if dep_exist:
                args = (int(worker_id), pos_name, dep_name, date_finish)
                command = self.LEAVE_POSITION
                await DataBase.execute(command, *args, execute=True)
            else:
                existing = 1
                return existing
        else:
            existing = 2
        return existing
    
    async def add_new_dep(self, dep_name):
        arg = dep_name
        command = self.ADD_NEW_DEP
        dep_exist = self.check_dep(dep_name)
        if not dep_exist:
            await DataBase.execute(command, arg, execute=True)
            return True
        else:
            return False
        
    async def add_new_pos(self, pos_name):
        arg = pos_name
        command = self.ADD_NEW_POSITION
        dep_exist = self.check_dep(pos_name)
        if not dep_exist:
            await DataBase.execute(command, arg, execute=True)
            return True
        else:
            return False
