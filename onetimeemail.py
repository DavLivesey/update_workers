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

async def add_email():
    index = 0
    for child in root:        
        fullname = child.get('ФИО')
        if fullname != None:
            email = root[index+9].get('АдресЭлектроннойПочты')
            worker_id = await db.get_worker_id(fullname)
        index += 1
        try:
            await db.edit_email(worker_id[0][0], email)
        except IndexError:
            pass
            


if __name__ == "__main__":
    asyncio.run(add_email())