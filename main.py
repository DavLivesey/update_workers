#Здесь код взаимодействия непосредственно с БД
from sql import DataBase
from logging import getLogger
from datetime import datetime as dt
from config import bot, ADMIN_CHAT
from aiogram.enums import ParseMode

LOG = getLogger()

class DBCommands:
    GET_ALL_WORKERS = 'SELECT fullname, email FROM workers'
    #Блок изменения информации о работниках
    ADD_NEW_WORKER = 'INSERT INTO workers (fullname, email, snils) VALUES ($1, $2, $3)'
    ADD_DEP = 'UPDATE workers w SET "department"=$2 WHERE id=$1'
    DELETE_WORKER = 'DELETE FROM workers WHERE id=$1'
    GET_MIS_EMPLOYERS = "SELECT * FROM mis_employers"
    GET_TIS_EMPLOYERS = "SELECT * FROM tis_employers"
    GET_WORKER_ID_WITH_FIO = 'SELECT id FROM workers w WHERE w.fullname=$1'
    GET_WORKER_ID_WITH_SNILS = 'SELECT id FROM workers w WHERE w.snils=$1'
    GET_WORKER_ID_WITH_AD = 'SELECT id FROM workers w WHERE w.ad=$1'
    VIEW_WORKER = 'SELECT * FROM workers w WHERE w.fullname LIKE $1' 
    VIEW_WORKER_ON_ID = 'SELECT * FROM workers WHERE id=$1'
    VIEW_WORKER_POSITIONS = 'SELECT w.fullname, d.dep_name, p.pos_name, wp.date_start, wp.employment FROM workplaces wp ' \
                            'JOIN workers w ON wp.worker_id = w.id ' \
                            'JOIN departments d ON wp.dep_id =d.id ' \
                            'JOIN positions p ON wp.pos_id =p.id ' \
                            'WHERE w.fullname = $1'
    VIEW_EX_WORKER_POSITIONS = 'SELECT w.fullname, d.dep_name, p.pos_name, wp.date_start, wp.employment FROM workplaces wp ' \
                            'JOIN workers w ON wp.worker_id = w.id ' \
                            'JOIN departments d ON wp.dep_id =d.id ' \
                            'JOIN positions p ON wp.pos_id =p.id ' \
                            'WHERE w.fullname = $1 and wp.expired=True'
    VIEW_WORKER_POSITIONS_ID = 'SELECT w.fullname, d.dep_name, p.pos_name FROM workplaces wp ' \
                            'JOIN workers w ON wp.worker_id = w.id ' \
                            'JOIN departments d ON wp.dep_id =d.id ' \
                            'JOIN positions p ON wp.pos_id =p.id ' \
                            'WHERE w.id = $1'    
    CHECK_WORKER = 'SELECT EXISTS (SELECT * FROM workers w WHERE w.fullname=$1)'
    GET_WORKER_WITH_EMAIL = 'SELECT id, fullname FROM workers WHERE email=$1'
    GET_WORKER_WITH_SNILS = 'SELECT id, fullname FROM workers WHERE snils=$1'
    FIND_WORKER = 'SELECT fullname FROM workers WHERE id = $1'
    SAVE_OLD_FIO = 'INSERT INTO old_names (worker_id, name) VALUES ($1, $2)'
    EDIT_WORKER_FIO = 'UPDATE workers w SET "fullname"=$2 WHERE id=$1'
    EDIT_WORKER_DATA = "UPDATE workers w SET snils=$2 WHERE id=$1"
    PRE_EXPIRE = "UPDATE workplaces SET expired=True"
    NULLIFY_MIS = "UPDATE workers w SET mis=False"
    NULLIFY_TIS = "UPDATE workers w SET tis=False"
    NULLIFY_TIS_SPK = "UPDATE workers w SET tis_spk=False"
    ADD_APTEKA = "UPDATE workers SET APTEKA='True' where id=$1"
    ADD_HR = "UPDATE workers SET ZKGU='True' where id=$1"
    ADD_BGU_1 = "UPDATE workers SET BGU_1='True' where id=$1"
    ADD_BGU_2 = "UPDATE workers SET BGU_2='True' where id=$1"
    ADD_DIETA = "UPDATE workers SET DIETA='True' where id=$1"
    ADD_MIS = "UPDATE workers SET MIS='True' where id=$1"
    ADD_TIS_SPK = "UPDATE workers SET TIS_SPK='True' where id=$1"
    ADD_TIS = "UPDATE workers SET TIS='True' where id=$1"
    ADD_SED = "UPDATE workers SET SED='True' where id=$1"
    DELETE_APTEKA = "UPDATE workers SET APTEKA='False' where id=$1"
    DELETE_HR = "UPDATE workers SET ZKGU='False' where id=$1"
    DELETE_BGU_1 = "UPDATE workers SET BGU_1='False' where id=$1"
    DELETE_BGU_2 = "UPDATE workers SET BGU_2='False' where id=$1"
    DELETE_DIETA = "UPDATE workers SET DIETA='False' where id=$1"
    DELETE_MIS = "UPDATE workers SET MIS='False' where id=$1"
    DELETE_TIS = "UPDATE workers SET TIS='False' where id=$1"
    DELETE_SED = "UPDATE workers SET SED='False' where id=$1"
    EDIT_EMAIL = 'UPDATE workers SET EMAIL=$2 where id=$1'
    ADD_AD = 'UPDATE workers SET "ad"=$2 WHERE id=$1'
    CHECK_PHONE_LIST = 'SELECT EXISTS (SELECT p.id FROM phones p WHERE p.phone_number = $1)'
    ADD_NEW_PHONE = 'INSERT INTO phones (phone_number) VALUES ($1)'
    ADD_PHONE = 'INSERT INTO connections (worker_id, phone_id) '\
                'VALUES ($1, (select id FROM phones WHERE phones.phone_number=$2))'
    GET_PHONE = 'SELECT p.phone_number FROM connections c '\
                  'JOIN workers w ON c.worker_id = w.id '\
                  'JOIN phones p ON c.phone_id =p.id '\
                  'WHERE w.fullname = $1'
    GET_MAILBOX = 'SELECT m.mailbox_name FROM workmails wm '\
                  'JOIN workers w ON wm.worker_id = w.id '\
                  'JOIN mailbox m ON wm.mail_id =m.id '\
                  'WHERE w.fullname = $1'
    ADD_NEW_POSITION = 'INSERT INTO positions (pos_name) VALUES ($1)'
    JOIN_POSITION_SNILS = 'INSERT INTO workplaces (worker_id, pos_id, dep_id, date_start, employment) ' \
                    'VALUES ((SELECT id FROM workers WHERE snils=$1), (select id FROM positions p WHERE p.pos_name=$2), '\
                    '(SELECT id FROM departments d WHERE d.dep_name=$3), $4, $5)'
    JOIN_POSITION_NAME = 'INSERT INTO workplaces (worker_id, pos_id, dep_id, date_start, employment) ' \
                    'VALUES ((SELECT id FROM workers WHERE fullname=$1), (select id FROM positions p WHERE p.pos_name=$2), '\
                    '(SELECT id FROM departments d WHERE d.dep_name=$3), $4, $5)'
    CHECK_WORKER_POSITION = 'SELECT EXISTS (SELECT * from workplaces wp where '\
                            'wp.worker_id=(SELECT id FROM workers w WHERE w.fullname=$1) '\
                            'and wp.pos_id=(SELECT id FROM positions p WHERE p.pos_name=$2) '\
                            'and wp.dep_id=(SELECT id FROM departments d WHERE d.dep_name=$3) and wp.employment=$4)'
    CHECK_WORKPLACE_DATA = 'SELECT * from workplaces wp where '\
                            'wp.worker_id=(SELECT id FROM workers w WHERE w.fullname=$1) '\
                            'and wp.pos_id=(SELECT id FROM positions p WHERE p.pos_name=$2) '\
                            'and wp.dep_id=(SELECT id FROM departments d WHERE d.dep_name=$3) and wp.employment=$4'
    UPDATE_WORKPLACE = 'UPDATE workplaces wp SET date_start=$2, employment=$3, expired=$4,  date_blocking=NULL '\
                        'WHERE wp.id=$1'
    DELETE_BLOCKING = 'UPDATE workplaces wp SET date_expire=NULL, date_blocking=NULL '\
                        'WHERE wp.id=$1'
    ADD_DATE_EXPIRE = "UPDATE workplaces wp set date_expire=$4, date_blocking=$6 "\
                     "WHERE wp.worker_id=$1 and "\
                     "wp.pos_id=(SELECT id FROM positions p WHERE p.pos_name=$2) and "\
                     "wp.dep_id=(SELECT id FROM departments d WHERE d.dep_name=$3) and wp.employment=$5"
    RESULT_EXPIRED = "UPDATE workplaces wp SET date_blocking=$1, date_expire=$1 WHERE date_blocking is NULL AND date_expire is NULL AND expired=True"
    GET_FRESH_BLOCKED_WP = 'SELECT w.fullname, p.pos_name, d.dep_name, wp.date_start, wp.date_expire, wp.employment FROM workplaces wp '\
                            'JOIN workers w ON wp.worker_id = w.id '\
                            'JOIN positions p ON wp.pos_id = p.id '\
                            'JOIN departments d ON wp.dep_id = d.id '\
                            'WHERE expired=TRUE and date_blocking=$1'
    CHECK_IS_POSITION = 'SELECT EXISTS (SELECT * FROM positions p WHERE p.pos_name=$1)'
    EDIT_POSITION = 'update workplaces wp set date_start=$4, employment=$5 '\
                    'WHERE wp.worker_id=$1 and '\
                    'wp.pos_id=(SELECT id FROM positions p WHERE p.pos_name=$2) and '\
                    'wp.dep_id=(SELECT id FROM departments d WHERE d.dep_name=$3)'
    DEKRET = 'UPDATE workplaces wp set expired=True, date_expire=$4, date_blocking=$6 '\
                     'WHERE wp.worker_id=$1 and '\
                     'wp.pos_id=(SELECT id FROM positions p WHERE p.pos_name=$2) and '\
                     'wp.dep_id=(SELECT id FROM departments d WHERE d.dep_name=$3) and wp.employment=$5 and date_blocking=NULL'
    ADD_NEW_DEP = 'INSERT INTO departments (dep_name) VALUES ($1)'
    CHECK_IS_DEP = 'SELECT EXISTS (SELECT * FROM departments WHERE dep_name LIKE $1)'
    PROLONGATE_WORKPLACE = "UPDATE workplaces wp set expired=False, date_expire=NULL, date_blocking=NULL "\
                     "WHERE wp.worker_id=$1 and "\
                     "wp.pos_id=(SELECT id FROM positions p WHERE p.pos_name=$3) and "\
                     "wp.dep_id=(SELECT id FROM departments d WHERE d.dep_name=$2) and wp.employment=$4"
    
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
    
    async def get_worker_with_email(self, email):
        command = self.GET_WORKER_WITH_EMAIL
        arg = email
        worker = await DataBase.execute(command, arg, fetch=True)
        return worker
    
    async def get_worker_with_snils(self, snils):
        command = self.GET_WORKER_WITH_SNILS
        arg = snils
        worker = await DataBase.execute(command, arg, fetch=True)
        return worker
    
    async def read_worker(self, person):
        #Функция получает лист информации о работнике и формирует несколько списков:
        #1. person - тот же список, что входит в функцию
        #2.list_person - список доступов работника
        #3.list_sert - список сертификатов работника
        #4.contacts_list - список контактов открытого доступа (телефон, электронная почта)
        #5.sec_list - список закрытых контактов (учетка в компьютер, участие в почтовых рассылках)
            access_dict = {}
            phone_arg = str(person[0])
            phone_command = self.GET_PHONE
            phone_list = []
            telephones = await DataBase.execute(phone_command, phone_arg, fetch=True)
            mailbox_arg = str(person[0])
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
            access_dict['1С_Аптека'] = person[2]
            access_dict['1С_Диетпитание'] = person[6]
            access_dict['1С_ЗКГУ'] = person[3]
            access_dict['1С_БГУ 1.0'] = person[4]
            access_dict['1С_БГУ 2.0'] = person[5]
            access_dict['СЭД'] = person[9]
            access_dict['МИС'] = person[7]
            access_dict['ТИС'] = person[8]
            access_dict['ad'] = person[11]
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
        result_contacts = ''
        result_sec = ''
        result_positions = ''
        result_phones = ''
        result_mailboxes = ''
        result = ''
        for mail in mailbox_list:
            result_mailboxes += f'{mail}\n'
        for phone in phone_list:
            result_phones += f'{phone}\n'
        for contact in contacts_list:
            result_contacts += f'{contact}\n'
        for sec in sec_list:
            result_sec += f'{sec}'
        for pos in position:
            result_positions += f'{pos[1]} {pos[0]}'
        fullname = person[1]
        if len(list_person) > 0:
            result += f'{fullname} {result_positions} {result_sec} {list_person}'
        else:
            result += f'{fullname} {result_positions} {result_sec}'
        return result

    async def view_worker_position(self, fullname):
        arg = fullname
        command = self.VIEW_WORKER_POSITIONS
        result = await DataBase.execute(command, arg, fetch=True)
        return result
    
    async def get_worker_id(self, fullname):
        arg = fullname
        command = self.GET_WORKER_ID_WITH_FIO
        result = await DataBase.execute(command, arg, fetch=True)
        return result
    
    async def get_worker_id_with_ad(self, ad):
        arg = ad
        command = self.GET_WORKER_ID_WITH_AD
        result = await DataBase.execute(command, arg, fetch=True)
        return result
    
    async def add_new_worker(self, fullname, snils, pos_name, dep_name, date_start, employment, email):
        worker_args = (fullname, email, snils)                       
        worker_command = self.ADD_NEW_WORKER
        workplace_command = self.JOIN_POSITION_SNILS
        await DataBase.execute(worker_command, *worker_args, execute=True)
        check_position_command = self.CHECK_IS_POSITION
        check_department_command = self.CHECK_IS_DEP
        existing_position = await DataBase.execute(check_position_command, pos_name, fetchval=True)
        existing_department = await DataBase.execute(check_department_command, dep_name, fetchval=True)
        if existing_department == False:
            await DataBase.execute(self.ADD_NEW_DEP, dep_name, execute=True)
        if existing_position == False:
            await DataBase.execute(self.ADD_NEW_POSITION, pos_name, execute=True)
        worker_id = await DataBase.execute(self.GET_WORKER_ID_WITH_FIO, fullname, fetch=True)
        workplace_args = (snils, pos_name, dep_name, date_start, employment)
        await DataBase.execute(workplace_command, *workplace_args, execute=True)

        

    async def check_worker(self, fullname):
        #Проверка наличия записи о сотруднике по ФИО
        arg = fullname
        command = self.CHECK_WORKER
        result = await DataBase.execute(command, arg, fetchval=True)
        return result

    async def view_ex_worker(self, name, position):        
        #Просмотр карточки работника без ID
        reading_result = await self.read_worker(name)
        message = await self.make_answer(reading_result[0], reading_result[1], reading_result[2], \
                                         reading_result[3], reading_result[4], reading_result[5], \
                                        reading_result[6], position)                
        return message        
    
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
    
    async def edit_fio(self, worker_id, fio, old_name):
        save_command = self.SAVE_OLD_FIO
        save_args = (worker_id, old_name)
        await DataBase.execute(save_command, *save_args, execute=True)
        command = self.EDIT_WORKER_FIO
        args = (int(worker_id), fio)
        await DataBase.execute(command, *args, execute=True)
        await bot.send_message(chat_id=ADMIN_CHAT, text=f'Сотрудница {old_name} сменила ФИО на {fio}',  parse_mode=ParseMode.HTML)

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

    async def check_worker_position(self, name, position, department, employment):
        command = self.CHECK_WORKER_POSITION
        args = (name, position, department, employment)
        result = await DataBase.execute(command, *args, fetchval=True)
        return result
    
    async def check_workplace_data(self, name, position, department, date_start, employment, status):
        command = self.CHECK_WORKPLACE_DATA
        args = (name, position, department, employment)
        result = await DataBase.execute(command, *args, fetch=True)
        if result[0][5] == None or result[0][7] == ''or result[0][4] == True:
                        command = self.UPDATE_WORKPLACE
                        args = (result[0][0], date_start, employment, status)
                        await DataBase.execute(command, *args, execute=True)
        if result[0][8] != None:
            arg = result[0][0]
            delete_command = self.DELETE_BLOCKING
            await DataBase.execute(delete_command, arg, execute=True)


    async def join_position(self, snils, pos_name, dep_name, date_start, employment, name):
        # Функция добавляет сотруднику должность в подразделении, если таковые существуют
        check_pos_arg = pos_name
        check_dep_arg = dep_name
        check_pos_command = self.CHECK_IS_POSITION
        check_dep_command = self.CHECK_IS_DEP
        dep_exist = await DataBase.execute(check_dep_command, check_dep_arg, fetchval=True)
        pos_exist = await DataBase.execute(check_pos_command, check_pos_arg, fetchval=True)
        if dep_exist == False:
            await DataBase.execute(self.ADD_NEW_DEP, dep_name, execute=True)
        if pos_exist == False:
            await DataBase.execute(self.ADD_NEW_POSITION, pos_name, execute=True)
        if snils != '':
            args = (snils, pos_name, dep_name, date_start, employment)
            command = self.JOIN_POSITION_SNILS
        else:
            args = (name, pos_name, dep_name, date_start, employment)
            command = self.JOIN_POSITION_NAME
        await DataBase.execute(command, *args, execute=True)

    async def edit_position(self, worker_id, pos_name, dep_name, date_start, employment):
        args = (worker_id, pos_name, dep_name, date_start, employment)
        command = self.EDIT_POSITION
        await DataBase.execute(command, *args, execute=True)
    
    async def dekret(self, worker_id, pos_name, dep_name, date_finish, employment):
        today = dt.now()
        args = (int(worker_id), pos_name, dep_name, date_finish, employment, today)
        command = self.DEKRET
        await DataBase.execute(command, *args, execute=True)
    
    async def add_expire(self, worker_id, pos_name, dep_name, date_finish, employment):
        today = dt.now()
        args = (int(worker_id), pos_name, dep_name, date_finish, employment, today)
        command = self.ADD_DATE_EXPIRE
        await DataBase.execute(command, *args, execute=True)
    
    async def create_message_expire(self):
        today = dt.now()
        command = self.GET_FRESH_BLOCKED_WP
        arg = today
        fresh_expired_list = await DataBase.execute(command, arg, fetch=True)
        if len(fresh_expired_list) > 0:
            for fired in fresh_expired_list:
                fullname = fired[0]
                arg_view = f"%{fullname}%"
                view_command = self.VIEW_WORKER
                person_data = await DataBase.execute(view_command, arg_view, fetch=True)
                person = person_data[0]
                position = [fired[1::]]
                fired_message = f'Увольнение с {fired[4]}'
                access_dict = {}
                phone_arg = str(person[0])
                phone_command = self.GET_PHONE
                phone_list = []
                person_list = []
                telephones = await DataBase.execute(phone_command, phone_arg, fetch=True)
                mailbox_arg = str(person[0])
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
                access_dict['email'] = person[10]
                access_dict['1С_Аптека'] = person[2]
                access_dict['1С_Диетпитание'] = person[6]
                access_dict['1С_ЗКГУ'] = person[3]
                access_dict['1С_БГУ 1.0'] = person[4]
                access_dict['1С_БГУ 2.0'] = person[5]
                access_dict['СЭД'] = person[9]
                access_dict['МИС'] = person[7]
                access_dict['ТИС'] = person[8]
                access_dict['ad'] = person[11]
                contacts_list = []
                sec_list = []
                for key, value in access_dict.items():                                                 #Если в словаре access_dict значение было изменено,
                    if value == True:                                                 #добавляет в список название ключа
                        person_list.append(key)
                if 'email' in person_list:                                                             #Если в списке оказалась почта, в список contacts_list
                    contacts_list.append(access_dict["email"])                           #добавляется текст с ее значением, а из списка person_list email удаляется
                    person_list.remove('email')
                if 'ad' in person_list:                                                                 #Если в списке имеется запись об учетке Active Directory, в список sec_list
                    sec_list.append(f'AD: {access_dict["ad"]}')                            #добавляется текст с его значением, а из списка person_list ad удаляется
                    person_list.remove('ad')
                list_person = '\n'.join(person_list)
                result_sec = ''
                result_phones = ''
                result_mailboxes = ''
                result_contacts = ''
                for contact in contacts_list:
                    result_contacts += f'{contact}\n'
                for mail in mailbox_list:
                    result_mailboxes += f'{mail}\n'
                for phone in phone_list:
                    result_phones += f'{phone}\n'
                for sec in sec_list:
                    result_sec += f'{sec}'
                if dt.strftime(fired[4], '000%Y-%m-%d') == '0001-01-01':
                    fired_message = f'Ушла в декрет'
                message = f'{person[1]}\n\n{position[0][1]}\n{position[0][0]}\n\n{fired[5]}.\nТелефоны: {result_phones}\n'\
                            f'{result_sec}\nПочта: {result_contacts}\nРассылки: {result_mailboxes}\nДоступы: {list_person}\n{fired_message}'
                await bot.send_message(chat_id=ADMIN_CHAT, text=message,  parse_mode=ParseMode.HTML)
    
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

#Далее идет блок добавления/удаления информации о наличии доступа к ИС
    async def plus_MIS(self, id):
        arg = int(id)
        command = self.ADD_MIS
        await DataBase.execute(command, arg, execute=True)

    async def plus_TIS(self, id):
        arg = int(id)
        command = self.ADD_TIS
        await DataBase.execute(command, arg, execute=True)

    async def plus_SED(self, id):
        arg = int(id)
        command = self.ADD_SED
        await DataBase.execute(command, arg, execute=True)

    async def plus_apteka(self, id):
        arg = int(id)
        command = self.ADD_APTEKA
        await DataBase.execute(command, arg, execute=True)

    async def plus_zkgu(self, id):
        arg = int(id)
        command = self.ADD_HR
        await DataBase.execute(command, arg, execute=True)

    async def plus_bgu1(self, id):
        arg = int(id)
        command = self.ADD_BGU_1
        await DataBase.execute(command, arg, execute=True)

    async def plus_bgu2(self, id):
        arg = int(id)
        command = self.ADD_BGU_2
        await DataBase.execute(command, arg, execute=True)

    async def plus_dieta(self, id):
        arg = int(id)
        command = self.ADD_DIETA
        await DataBase.execute(command, arg, execute=True)

    async def del_MIS(self, id):
        arg = int(id)
        command = self.DELETE_MIS
        await DataBase.execute(command, arg, execute=True)

    async def del_TIS(self, id):
        arg = int(id)
        command = self.DELETE_TIS
        await DataBase.execute(command, arg, execute=True)

    async def del_SED(self, id):
        arg = int(id)
        command = self.DELETE_SED
        await DataBase.execute(command, arg, execute=True)

    async def del_apteka(self, id):
        arg = int(id)
        command = self.DELETE_APTEKA
        await DataBase.execute(command, arg, execute=True)

    async def del_zkgu(self, id):
        arg = int(id)
        command = self.DELETE_HR
        await DataBase.execute(command, arg, execute=True)

    async def del_bgu1(self, id):
        arg = int(id)
        command = self.DELETE_BGU_1
        await DataBase.execute(command, arg, execute=True)

    async def del_bgu2(self, id):
        arg = int(id)
        command = self.DELETE_BGU_2
        await DataBase.execute(command, arg, execute=True)

    async def del_dieta(self, id):
        arg = int(id)
        command = self.DELETE_DIETA
        await DataBase.execute(command, arg, execute=True)
#Конец блока о доступе к ИС

    async def nullify_mis(self):
        await DataBase.execute(self.NULLIFY_MIS, execute=True)
    
    async def nullify_tis(self):
        await DataBase.execute(self.NULLIFY_TIS, execute=True)

    async def nullify_tis_spk(self):
        await DataBase.execute(self.NULLIFY_TIS_SPK, execute=True)
    
    async def change_mis(self):
        employers = await DataBase.execute(self.GET_MIS_EMPLOYERS, fetch=True)
        file = open('./dangers.docx', 'w+')
        for medic in employers:
            if medic[3] != None:
                employer_id_fetch = await DataBase.execute(self.GET_WORKER_ID_WITH_SNILS, medic[3], fetch=True)
            else:
                employer_id_fetch = await DataBase.execute(self.GET_WORKER_ID_WITH_FIO, medic[1], fetch=True)
            if employer_id_fetch != []:
                employer_id = int(employer_id_fetch[0][0])
                await DataBase.execute(self.ADD_MIS, employer_id, execute=True)
                if medic[4]:
                    await DataBase.execute(self.ADD_TIS, employer_id, execute=True)
            else:
                file.write(medic[1])
    
    async def change_tis_spk(self):
        employers = await DataBase.execute(self.GET_TIS_EMPLOYERS, fetch=True)
        for medic in employers:
            if medic[3] != None:
                employer_id_fetch = await DataBase.execute(self.GET_WORKER_ID_WITH_SNILS, medic[3], fetch=True)
            else:
                employer_id_fetch = await DataBase.execute(self.GET_WORKER_ID_WITH_FIO, medic[1], fetch=True)
            if employer_id_fetch != []:
                employer_id = int(employer_id_fetch[0][0])
                await DataBase.execute(self.ADD_TIS_SPK, employer_id, execute=True)


    async def add_telephone(self, worker_id, telephone, name):
        try:
            int(telephone)
            check_com = self.CHECK_PHONE_LIST
            check_arg = telephone
            add_com = self.ADD_NEW_PHONE
            add_arg = telephone
            command = self.ADD_PHONE
            check_emp_phone_command = self.GET_PHONE
            check_emp_phone_arg = name
            args = (int(worker_id), telephone)
            check_result = await DataBase.execute(check_com, check_arg, fetchval=True)
            if check_result == False:
                await DataBase.execute(add_com, add_arg, execute=True)
            phones = await DataBase.execute(check_emp_phone_command, check_emp_phone_arg, fetch=True)
            phones_list = []
            for ph in phones:
                phones_list.append(ph[0])
            if telephone not in phones_list:
                await DataBase.execute(command, *args, execute=True)
            return True
        except ValueError:
            return False
    
    async def add_ad(self, worker_id, ad):
        command = self.ADD_AD
        args = (int(worker_id), ad)
        await DataBase.execute(command, *args, execute=True)
    
    async def edit_worker_data(self, id, snils):
        args = (id, snils)
        data_command = self.EDIT_WORKER_DATA
        await DataBase.execute(data_command, *args, execute=True)
    
    async def pre_expire(self):
        await DataBase.execute(self.PRE_EXPIRE, execute=True)
    
    async def prolongate_working(self, worker_id, dep_name, pos_name, employment):
        args = (worker_id, dep_name, pos_name, employment)
        command = self.PROLONGATE_WORKPLACE
        await DataBase.execute(command, *args, execute=True)
    
    async def result_expired(self):
        arg = dt.today()
        command = self.RESULT_EXPIRED
        await DataBase.execute(command, arg, execute=True)
