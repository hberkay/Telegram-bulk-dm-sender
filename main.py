import traceback

import telethon
from telethon.errors import InvalidBufferError
from telethon.tl.types import InputPeerUser
from telethon.errors.rpcerrorlist import PeerFloodError, FloodWaitError, AuthKeyUnregisteredError, UsernameInvalidError
from telethon.sync import TelegramClient
from telethon import TelegramClient
import sys
import csv
import random
import time
import asyncio


api_id = 0
api_hash = '0'

SLEEP_TIME = 30

file = open('users.csv') #create users.csv file 
users = []
with open("users.csv", 'r') as file:
    rows = csv.reader(file)
    next(rows, None)
    for row in rows:
        user = {}
        user['username'] = row[0]
        users.append(user)
file = open('sessions.csv')
sessions = []
with open("sessions.csv", 'r') as file:
    rows = csv.reader(file)
    next(rows, None)
    for row in rows:
        session = {}
        session['num'] =  row[0]
        sessions.append(session)

message = 'Simple Message'

i = 0
j = 0
autherror = 0
error = 0
client = TelegramClient(sessions[j]['num'], api_id, api_hash)
print(sessions[j]['num'])
client.connect()
while users:
    try:
        print("Sending Message to:", users[i]['username'])
        client.send_message(users[i]['username'], message)
        print("Waiting {} seconds".format(SLEEP_TIME))
        time.sleep(30)
        i = i + 1
    except FloodWaitError:
        print("FloodWaitError")
        client.disconnect()
        j = j + 1
        i = i + 1
        client = TelegramClient(sessions[j]['num'], api_id, api_hash)
        client.connect()
        continue
    except PeerFloodError:
        print("PeerFloodError")
        client.disconnect()
        i = i + 1
        j = j + 1
        print(sessions[j]['num'])
        client = TelegramClient(sessions[j]['num'], api_id, api_hash)
        client.connect()
        continue
    except InvalidBufferError:
        print("InvalidBufferError")
        client.disconnect()
        j = j + 1
        i = i + 1
        print(sessions[j]['num'])
        client = TelegramClient(sessions[j]['num'], api_id, api_hash)
        client.connect()
        continue
    except AuthKeyUnregisteredError:
        print("AuthKeyUnregisteredError")
        client.disconnect()
        i = i + 1
        autherror = autherror + 1
        if autherror == 3:
            j = j + 1
            autherror = 0
        print(sessions[j]['num'])
        client = TelegramClient(sessions[j]['num'], api_id, api_hash)
        client.connect()
        continue
    except UsernameInvalidError:
        print("name error")
        i = i + 1
        continue
    except:
        traceback.print_exc()
        print(i)
        error = error + 1
        i = i + 1
        if 3 == error:
            client.disconnect()
            j = j + 1
            i = i + 1
            error = 0
            print(sessions[j]['num'])
            client = TelegramClient(sessions[j]['num'], api_id, api_hash)
            client.connect()
        continue

print("Done. Message sent to all users.")
