





# --------------------------------------------------------------------------------------------------  Тест (Добавлем SQL запросы)
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from telebot.handler_backends import State, StatesGroup  # Импортируем State и StatesGroup
from datetime import datetime, timedelta, date
from threading import Thread
from io import BytesIO
from tabulate import tabulate
from telebot import types
from telebot.apihelper import ApiTelegramException
from types import SimpleNamespace
import datetime as dt
import time
import sqlite3
import telebot
import logging
import pandas as pd  # Импортируем pandas для работы с DataFrame
import matplotlib.pyplot as plt
import io
import sys
import seaborn
import seaborn as sns
import random
import matplotlib
import os
import numpy as np
import calendar
matplotlib.use('Agg')  # Используем Agg для работы без GUI
from requests.exceptions import RequestException
from telebot import TeleBot

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


#-------------------------------------------------------------------------------------------------------------------------------------- Вид и путь к боту
# bot = telebot.TeleBot('8108478102:AAFKqakt1ZNKAAsNQ2foEaVcYkfFchRCmvM') # Test бот 
# db_path1 = r'C:\Users\PERS\Desktop\Разное\Anaconda\VS CODE\Prodect\Prog_bots.db' # Test бот 
# db_path2 = r'C:\Users\PERS\Desktop\Разное\Anaconda\VS CODE\Prodect\Prog_bots2.db' # Test бот (Путь к базе данных (План))
## BOT_TOKEN = "8108478102:AAFKqakt1ZNKAAsNQ2foEaVcYkfFchRCmvM" # Test бот для перезапуска

bot = telebot.TeleBot('7851730501:AAFNiNCe2AE_iDUbgU5QHHnwMn14QYwXdic')  # Prod бот
db_path1 = '/home/Telebotfinanc12trev/botfin/Prog_bots.db'  # Prod бот
db_path2 = '/home/Telebotfinanc12trev/botfin/Prog_bots2.db'  # Prod бот (Путь к базе данных (План))


#--------------------------------------------------------------------------------------------------------------------------------------

# Подключение к базе данных
db = sqlite3.connect(db_path1, check_same_thread=False)
c = db.cursor()


# Создание таблицы (если она еще не существует)
c.execute("""CREATE TABLE IF NOT EXISTS sav_bot1(
          ID INTEGER PRIMARY KEY AUTOINCREMENT
          ,Date TEXT
          ,"Groupe" TEXT
          ,Price REAL
          ,Description TEXT
          )""")
db.commit()


# Создание таблицы (План)
db_rs = sqlite3.connect(db_path2)
c1 = db_rs.cursor()

c1.execute("""CREATE TABLE IF NOT EXISTS sav_bot_rs(
            ID INTEGER PRIMARY KEY AUTOINCREMENT
            ,"Groupe" TEXT
            ,Plan REAL
           )""")
db_rs.commit()


# Переменные для хранения временных данных
user_data = {}
user_states = {}

# Добавляем в словарь user_states новое состояние
user_states['waiting_for_date_fail'] = 'waiting_for_date_fail'

# -------------------------------------------------------------------------------------------
# 🌟 БЛОК ГОРЯЧИХ СЛОВ (HOT WORDS) — ДОБАВЛЕНО СОГЛАСНО ЗАДАЧЕ
# Словарь горячих слов: ключ — текст команды (в нижнем регистре), значение — описание и обработчик
HOT_WORDS = {
    "меню": {
        "description": "Открывает главное меню",
        "handler": lambda message: handle_menu_button(message)
    },
    "внести": {
        "description": "Начинает процесс внесения суммы",
        "handler": lambda message: bot.send_message(
            message.chat.id,
            "Выберите опцию:",
            reply_markup=types.InlineKeyboardMarkup()
            .add(types.InlineKeyboardButton('Текущая дата', callback_data='hands'))
            .add(types.InlineKeyboardButton('Любая дата', callback_data='custom_date'))
            .add(types.InlineKeyboardButton('← Назад', callback_data='back_add_summ1'))
        )
    },
    "история": {
        "description": "Историчность",
        "handler": lambda message: bot.send_message(
            message.chat.id,
            "Выберите опцию:",
            reply_markup=types.InlineKeyboardMarkup()
            .row(types.InlineKeyboardButton('За сегодня', callback_data='today_history')
                   ,types.InlineKeyboardButton('За вчера', callback_data='yesterday_history')
                   ,types.InlineKeyboardButton('За неделю', callback_data='week_history')
                   ,types.InlineKeyboardButton('За период', callback_data='period_history'))
            .add(types.InlineKeyboardButton('← Назад', callback_data='back_history1'))
        )
    },
    "банк": {
        "description": "Показывает текущий остаток в банке",
        "handler": lambda message: (
            lambda fake_call: callback_query(fake_call)
        )(
            SimpleNamespace(
                data='savings_summ',
                message=SimpleNamespace(
                    chat=SimpleNamespace(id=message.chat.id),
                    message_id=None
                )
            )
        )
    },
    "траты": {
        "description": "Траты за месяц",
        "handler": lambda message: (
            lambda fake_call: callback_query(fake_call)
        )(
            SimpleNamespace(
                data='general',
                message=SimpleNamespace(
                    chat=SimpleNamespace(id=message.chat.id),
                    message_id=None
                )
            )
        )
    }
}

# Универсальный обработчик горячих слов
@bot.message_handler(func=lambda message: message.text.lower() in HOT_WORDS)
def handle_hot_words(message):
    word = message.text.lower()
    handler = HOT_WORDS[word]["handler"]
    if handler:
        handler(message)
    else:
        bot.reply_to(message, f"Команда '{message.text}' распознана, но обработчик пока не реализован.")

# -------------------------------------------------------------------------------------------

# Словарь для сопоставления callback_data с названиями групп
group_names = {
    'group_zp_nik': 'Зп',
    'group_zp_ego': 'Зп доп',
    'group_profit': 'Прибыль',
    'group_food': 'Продукты',
    'group_apartment': 'Квартира',
    'group_car': 'Машина',
    'group_entertainment': 'Развлечение',
    'group_transfer': 'Перевод',
    'group_extra': 'Дополнительное',
    'group_personal': 'Личные траты',
    'group_clothes': 'Одежда',
    'group_losses': 'Потери'
}

# Словарь для сопоставления callback_data с названиями групп
names_tr_1 = {
    'foot_1': 'Продукты',
    'apartment_1': 'Квартира',
    'entertainment_1': 'Развлечение',
    'extra_1': 'Дополнительное',
    'personal_1': 'Личные траты',
    'car_1': 'Машина',
    'clothes_1': 'Одежда',
    'transfer_1': 'Перевод',
    'losses_1': 'Потери'
}


names_tr_s2 = {
    'foot_s2': 'Продукты',
    'apartment_s2': 'Квартира',
    'entertainment_s2': 'Развлечение',
    'extra_s2': 'Дополнительное',
    'personal_s2': 'Личные траты',
    'car_s2': 'Машина',
    'clothes_s2': 'Одежда',
    'transfer_s2': 'Перевод',
    'losses_s2': 'Потери'
}


# Функция для сброса состояния пользователя
def reset_user_data(user_id):
    if user_id in user_data:
        del user_data[user_id]
    # Можно также добавить сброс других состояний, если они есть


# Функция для создания всплывающего меню с кнопками "Menu" и "Stop"
def create_main_menu():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btn_menu = types.KeyboardButton('Menu')
    btn_stop = types.KeyboardButton('Stop')
    markup.add(btn_menu, btn_stop)
    return markup


# Обработчик команды /start и /go
@bot.message_handler(commands=['go', 'start','menu'])
def get_ph(message):
    user_id = message.chat.id
    markup = create_main_menu()  # Создаем основное меню с кнопками "Menu" и "Stop"
    bot.reply_to(message, 'Нажмите кнопку Menu', reply_markup=markup)

# Обработчик нажатия на кнопку "Menu"
@bot.message_handler(func=lambda message: message.text == 'Menu')
def handle_menu_button(message):
    user_id = message.chat.id 
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('Посмотреть бюджет', callback_data='view_budget'))
    markup.row(types.InlineKeyboardButton('Внести сумму', callback_data='add_amount'),
               types.InlineKeyboardButton('История внесения', callback_data='history_add'))
    markup.add(types.InlineKeyboardButton('Удалить сумму', callback_data='delete_summ'))
    markup.add(types.InlineKeyboardButton('План/Факт', callback_data='plan_fact'))
    markup.add(types.InlineKeyboardButton('Дополнительный расчет', callback_data='additional_calculation'))
    bot.send_message(user_id, 'Выберите опцию:', reply_markup=markup)

# Обработчик нажатия на кнопку "Stop"
@bot.message_handler(func=lambda message: message.text.lower() == 'stop')
def handle_stop_button(message):
    user_id = message.chat.id
    bot.clear_step_handler_by_chat_id(user_id)
    if user_id in user_states:
        del user_states[user_id]  # Полностью удаляем состояние
    bot.send_message(user_id, "🛑 Операция отменена. Вы можете начать новый запрос.")
    
# # Обработчик команды /start и /go
# @bot.message_handler(commands=['go', 'start'])
# def get_ph(message):
#     user_id = message.chat.id
#     markup = types.InlineKeyboardMarkup()
#     markup.add(types.InlineKeyboardButton('Посмотреть бюджет', callback_data='view_budget'))
#     markup.row(types.InlineKeyboardButton('Внести сумму', callback_data='add_amount')
#                 ,types.InlineKeyboardButton('История внесения', callback_data='history_add'))
#     markup.add(types.InlineKeyboardButton('Дополнительный расчет', callback_data='additional_calculation'))
#     bot.reply_to(message, 'Выбор', reply_markup=markup)
# /home/Telebotfinanc12trev/botfin/Progect_bot.py
#-------------------------------------------------------------------------------------------------------------------------------------- 
# Обработчик команды /help
@bot.message_handler(commands=['help'])
def help_bot(message):
    user_id = message.chat.id
    reset_user_data(user_id)  # Сбрасываем состояние пользователя
    
    help_text = """
📚 *Меню помощи* 📚

Вот основные команды, которые вы можете использовать:

🔹 */go*, */start* - Запускают главное меню бота
    🔹 Кнопка "Menu" состоит из 4 основных направлений 
    🔹 Кнопка "Stop" останавливает текущий процесс бота 
    
🔹 */help* - Показывает это меню основных команд
🔹 */help1* - Всё о работе с бюджетом
🔹 */help2* - Как вносить суммы
🔹 */help3* - Всё о прогнозах

Выберите нужную команду для получения подробной информации.
"""
    bot.reply_to(message, help_text, parse_mode='Markdown')


#-------------------------------------------------------------------------------------------------------------------------------------- 
# Обработчик команды /help1
@bot.message_handler(commands=['help1'])
def help1_bot(message):
    user_id1 = message.chat.id
    reset_user_data(user_id1)
    
    help1_text = """
💰 *Работа с бюджетом* 💰

1. Для просмотра бюджета (Накопления/Траты):
   - Нажмите на *"Посмотреть бюджет"* в главном меню
   
2. Выбор направления между:
     ▪ Накопления
     ▪ Траты

3. Детализация:
    Выберите из списка интересующие Вас условие
     
"""
    bot.reply_to(message, help1_text, parse_mode='Markdown')

#-------------------------------------------------------------------------------------------------------------------------------------- 
# Обработчик команды /help2
@bot.message_handler(commands=['help2'])
def help2_bot(message):
    user_id2 = message.chat.id
    reset_user_data(user_id2)
    
    help2_text = """
📥 *Внесение суммы* 📥

*Этапы внесения суммы:*

1. *Выбор группы*:
   - Выберите соответствующую группу (траты/накопления)
   - Укажите конкретную категорию

2. *Ввод данных*:
   - Внесите сумму (целое число, без копеек)
   - Напишите комментарий или поставьте прочерк "-"

3. *Подтверждение*:
   - Проверьте правильность введенных данных
   - Если есть ошибки - вернитесь к выбору группы
   - Если всё верно - нажмите на кнопку добавить в БД
   - После данные будут добавлены в БД

"""
    bot.reply_to(message, help2_text, parse_mode='Markdown')

#-------------------------------------------------------------------------------------------------------------------------------------- Обработчик команды /hel3

@bot.message_handler(commands=['help3'])
def help1_bot(message):
    user_id3 = message.chat.id
    reset_user_data(user_id3)
    help3_text = """
    В разработке
    """
    bot.reply_to(message, help3_text)



# Обработчик любых других сообщений
@bot.message_handler(func=lambda message: True)
def handle_other_messages(message):
    if message.text not in ['/go', '/start', '/help', '/stop']:
        bot.reply_to(message, "Некорректные данные, попробуйте написать /help")

# Обработчик команды /stop
@bot.message_handler(commands=['stop'])
def stop_bot(message):
    bot.send_message(message.chat.id, "Бот остановлен")
    logger.info("Бот остановлен по команде /stop")
    # Завершаем работу бота
    bot.stop_polling()
    sys.exit()  # Завершаем выполнение программы

# Обработчик callback-запросов
@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    
    user_id = call.message.chat.id  # Получаем ID пользователя
    # Проверяем, находится ли пользователь в состоянии "idle" (после нажатия "Stop")
    if user_states.get(user_id) == 'idle':
        bot.answer_callback_query(call.id, "❌ Текущая операция была отменена. Начните заново.")
        
    # -- БЛОК "БЮДЖЕТ"
    if call.data == 'view_budget':
        markup = types.InlineKeyboardMarkup()
        markup.row(types.InlineKeyboardButton('Накопления', callback_data='savings'),
                   types.InlineKeyboardButton('Траты', callback_data='expenses'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget1'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выберите опцию:', reply_markup=markup)
    elif call.data == 'savings':
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton('Накопленная сумма', callback_data='savings_summ'),
            types.InlineKeyboardButton('Накопления по месяцам', callback_data='savings_month')
        )
        markup.row(
            types.InlineKeyboardButton('Прибыль по месяцам', callback_data='profit_month'),
            types.InlineKeyboardButton('Прибыль по лицам', callback_data='profit_personal')
        )
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget2'))
        bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text='Выберите опцию:',
            reply_markup=markup
        )
    elif call.data == 'expenses':
        markup = types.InlineKeyboardMarkup()
        markup.row(types.InlineKeyboardButton('За текущий месяц', callback_data='general')
                   ,types.InlineKeyboardButton('Граппа за месяц', callback_data='in_terms_group')
                   ,types.InlineKeyboardButton('1 группа за месяц',callback_data='one_group_month')
                   )
        markup.row(types.InlineKeyboardButton('Текущий, прошлый месяц', callback_data='in_terms_groupe')
                    ,types.InlineKeyboardButton('За 6 месяцев', callback_data='in_six_groupes'))
        markup.row(types.InlineKeyboardButton('За любой месяц.год', callback_data='contr_month_table1')
                   ,types.InlineKeyboardButton('1 группа за любой месяц.год',callback_data='one_contr_group_month'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget3'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выберите опцию:', reply_markup=markup)
# ------------------------------------------------------------------------------------------------------------------------------------------------------(plan_fact)
    elif call.data == 'plan_fact':  
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Анализ', callback_data='analitics1'))
        markup.add(types.InlineKeyboardButton('План', callback_data='plan_rs'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_add_dop_rs1'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выбор', reply_markup=markup)
# ------------------------------------------------------------------------------------------------------------------------------------------------------(#Работа с планом)
    elif call.data == 'plan_rs':  
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Таблица плана', callback_data='table_plan_rs'))
        markup.add(types.InlineKeyboardButton('Корректировка плана', callback_data='correct_plan'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget_pl_fl'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выбор', reply_markup=markup)
# =====================================================================================================================================================================================================   (#Корректировка плана)
    elif call.data == 'correct_plan':  
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('По названию', callback_data='table_plan'))
        markup.add(types.InlineKeyboardButton('По ID', callback_data='correct_plan_id'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_burger1'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выбор', reply_markup=markup)

    elif call.data == 'table_plan':
        # Меню выбора группы для корректировки по названию
        markup = types.InlineKeyboardMarkup(row_width=2)
        groups = ["Продукты", "Квартира", "Развлечение", "Дополнительное", 
                "Личные траты", "Машина", "Одежда", "Перевод", "Потери"]
        
        # Изменяем префикс на 'plan_group_' чтобы избежать конфликта
        buttons = [types.InlineKeyboardButton(group, callback_data=f'plan_group_{group}') for group in groups]
        markup.add(*buttons)
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='correct_plan'))
        
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, 
                            text='Выберите группу для корректировки:', reply_markup=markup)

    elif call.data.startswith('plan_group_'):
        # Пользователь выбрал группу, запрашиваем сумму
        group_name = call.data.split('_', 2)[2]  # Изменяем split для нового префикса
        msg = bot.send_message(call.message.chat.id, f'Вы выбрали группу: {group_name}\nВведите новую сумму плана:')
        
        # Сохраняем группу в временных данных пользователя
        bot.register_next_step_handler(msg, process_plan_amount, group_name=group_name)

    elif call.data == 'correct_plan_id':
        # Запрашиваем ID для корректировки
        msg = bot.send_message(call.message.chat.id, 'Введите ID записи для корректировки:')
        bot.register_next_step_handler(msg, process_plan_id)

    elif call.data.startswith('confirm_'):
        # Обработка подтверждения для изменения по названию
        parts = call.data.split('_')
        if parts[1] == 'id':
            # Это подтверждение по ID
            record_id = int(parts[2])
            new_amount = float(parts[3])
            
            db_rs = sqlite3.connect(db_path2)
            c1 = db_rs.cursor()
            c1.execute("UPDATE sav_bot_rs SET Plan = ? WHERE ID = ?", (new_amount, record_id))
            db_rs.commit()
            db_rs.close()
            
            success_text = f"План для записи ID {record_id} успешно обновлен на {new_amount}"
        else:
            # Это подтверждение по названию группы
            group_name = parts[1]
            new_amount = float(parts[2])
            
            db_rs = sqlite3.connect(db_path2)
            c1 = db_rs.cursor()
            c1.execute("UPDATE sav_bot_rs SET Plan = ? WHERE Groupe = ?", (new_amount, group_name))
            db_rs.commit()
            db_rs.close()
            
            success_text = f"План для группы '{group_name}' успешно обновлен на {new_amount}"
        
        # ✅ Создаём клавиатуру (как у тебя было)
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('По названию', callback_data='table_plan'))
        markup.add(types.InlineKeyboardButton('По ID', callback_data='correct_plan_id'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_burger1'))
        
        # ✅ Отправляем ОДНО сообщение: текст + кнопки под ним
        bot.send_message(
            chat_id=call.message.chat.id,
            text=success_text,
            reply_markup=markup
        )

    # -- Вернутся назад БЮДЖЕТ
    elif call.data == 'back_buget1':  # Вернуться с "Посмотреть бюджет" в главное меню
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Посмотреть бюджет', callback_data='view_budget'))
        markup.row(types.InlineKeyboardButton('Внести сумму', callback_data='add_amount'),
                types.InlineKeyboardButton('История внесения', callback_data='history_add'))
        markup.add(types.InlineKeyboardButton('Удалить сумму', callback_data='delete_summ'))
        markup.add(types.InlineKeyboardButton('План/Факт', callback_data='plan_fact'))
        markup.add(types.InlineKeyboardButton('Дополнительный расчет', callback_data='additional_calculation'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выбор', reply_markup=markup)
    elif call.data == 'back_buget2':  # Вернуться с "Накопления" в бюджет
        markup = types.InlineKeyboardMarkup()
        markup.row(types.InlineKeyboardButton('Накопления', callback_data='savings'),
                   types.InlineKeyboardButton('Траты', callback_data='expenses'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget1'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выберите опцию:', reply_markup=markup)
    elif call.data == 'back_buget3':  # Вернуться с "Накопления" в бюджет
        markup = types.InlineKeyboardMarkup()
        markup.row(types.InlineKeyboardButton('Накопления', callback_data='savings'),
                   types.InlineKeyboardButton('Траты', callback_data='expenses'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget1'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выберите опцию:', reply_markup=markup)
        
    # ----------------------------------------------------------------------------------------------------------------------------------- БЛОК "Внести сумму"
    elif call.data == 'add_amount':
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Текущая дата', callback_data='hands'))
        markup.add(types.InlineKeyboardButton('Любая дата', callback_data='custom_date'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_add_summ1'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выберите опцию:', reply_markup=markup)
    elif call.data == 'hands':
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton('Зп', callback_data='group_zp_nik'),
            types.InlineKeyboardButton('Зп доп', callback_data='group_zp_ego'),
            types.InlineKeyboardButton('Прибыль', callback_data='group_profit')
        )
        markup.row(
            types.InlineKeyboardButton('Продукты', callback_data='group_food'),
            types.InlineKeyboardButton('Квартира', callback_data='group_apartment'),
            types.InlineKeyboardButton('Развлечение', callback_data='group_entertainment')
        )
        markup.row(
            types.InlineKeyboardButton('Дополнительное', callback_data='group_extra'),
            types.InlineKeyboardButton('Личные траты', callback_data='group_personal'),
            types.InlineKeyboardButton('Машина', callback_data='group_car')
        )
        markup.row(
            types.InlineKeyboardButton('Одежда', callback_data='group_clothes'),
            types.InlineKeyboardButton('Перевод', callback_data='group_transfer'),
            types.InlineKeyboardButton('Потери', callback_data='group_losses')
        )
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_add_buget1'))
        bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text='Выберите опцию:',
            reply_markup=markup
        )
    # Обработка выбора группы
    elif call.data.startswith('group_'):
        user_data[call.from_user.id] = {'group': group_names.get(call.data, call.data)}
        bot.send_message(call.message.chat.id, "Введите сумму:")
        bot.register_next_step_handler(call.message, process_amount_step)
        
        # ----------------------------------------------------------------------------------------------------------------------------------- БЛОК "Внести сумму за конкретную дату"
        
    elif call.data == 'custom_date':
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton('Зп', callback_data='custom_group_zp_nik'),
            types.InlineKeyboardButton('Зп доп', callback_data='custom_group_zp_ego'),
            types.InlineKeyboardButton('Прибыль', callback_data='custom_group_profit')
        )
        markup.row(
            types.InlineKeyboardButton('Продукты', callback_data='custom_group_food'),
            types.InlineKeyboardButton('Квартира', callback_data='custom_group_apartment'),
            types.InlineKeyboardButton('Развлечение', callback_data='custom_group_entertainment')
        )
        markup.row(
            types.InlineKeyboardButton('Дополнительное', callback_data='custom_group_extra'),
            types.InlineKeyboardButton('Личные траты', callback_data='custom_group_personal'),
            types.InlineKeyboardButton('Машина', callback_data='custom_group_car')
        )
        markup.row(
            types.InlineKeyboardButton('Одежда', callback_data='custom_group_clothes'),
            types.InlineKeyboardButton('Перевод', callback_data='custom_group_transfer'),
            types.InlineKeyboardButton('Потери', callback_data='custom_group_losses')
        )
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_add_buget1'))
        bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text='Выберите опцию:',
            reply_markup=markup
    )
    elif call.data.startswith('custom_group_'):
        group_key = call.data.replace('custom_group_', 'group_')  # Приводим к стандартному формату
        user_data[call.from_user.id] = {
            'group': group_names.get(group_key, group_key),
            'needs_date': True  # Флаг, что нужно запросить дату
        }
        bot.send_message(call.message.chat.id, "Введите дату в формате ДД.ММ.ГГГГ (например, 10.06.2025):")
        bot.register_next_step_handler(call.message, process_custom_date_step)




        
    # ----------------------------------------------------------------------------------------------------------------------------------- Вернутся назад "Внести сумму"
    elif call.data == 'back_add_summ1':  # Вернуться с "Внесения суммы" в главное меню
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Посмотреть бюджет', callback_data='view_budget'))
        markup.row(types.InlineKeyboardButton('Внести сумму', callback_data='add_amount'),
                types.InlineKeyboardButton('История внесения', callback_data='history_add'))
        markup.add(types.InlineKeyboardButton('Удалить сумму', callback_data='delete_summ'))
        markup.add(types.InlineKeyboardButton('План/Факт', callback_data='plan_fact'))
        markup.add(types.InlineKeyboardButton('Дополнительный расчет', callback_data='additional_calculation'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выбор', reply_markup=markup)
    elif call.data == 'back_add_buget1':
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Текущая дата', callback_data='hands'))
        markup.add(types.InlineKeyboardButton('Любая дата', callback_data='fail1'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_add_summ1'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выбор', reply_markup=markup)
# =============================================================================================================================================================================================    История       
    elif call.data == 'history_add':
        markup = types.InlineKeyboardMarkup()
        markup.row(types.InlineKeyboardButton('За сегодня', callback_data='today_history')
                   ,types.InlineKeyboardButton('За вчера', callback_data='yesterday_history')
                   ,types.InlineKeyboardButton('За неделю', callback_data='week_history')
                   ,types.InlineKeyboardButton('За период', callback_data='period_history'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_history1'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выбор', reply_markup=markup)
        
    elif call.data == 'back_history1':  # Вернуться с "Историчности" в главное меню
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Посмотреть бюджет', callback_data='view_budget'))
        markup.row(types.InlineKeyboardButton('Внести сумму', callback_data='add_amount'),
                types.InlineKeyboardButton('История внесения', callback_data='history_add'))
        markup.add(types.InlineKeyboardButton('Удалить сумму', callback_data='delete_summ'))
        markup.add(types.InlineKeyboardButton('План/Факт', callback_data='plan_fact'))
        markup.add(types.InlineKeyboardButton('Дополнительный расчет', callback_data='additional_calculation'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выбор', reply_markup=markup)
# =============================================================================================================================================================================================          


    elif call.data == 'additional_calculation':
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Прогноз на 12 месяцев (Фик)', callback_data='prog_fiks'))
        markup.add(types.InlineKeyboardButton('Прогноз на 12 месяцев (Дин)', callback_data='prog_dinam'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_add_dop_rs1'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выбор', reply_markup=markup)


    # ----------------------------------------------------------------------------------------------------------------------------------- Вернутся назад "C Дополнительный расчет"
    elif call.data == 'back_add_dop_rs1':  # Вернуться с "Внесения суммы" в главное меню
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Посмотреть бюджет', callback_data='view_budget'))
        markup.row(types.InlineKeyboardButton('Внести сумму', callback_data='add_amount'),
                types.InlineKeyboardButton('История внесения', callback_data='history_add'))
        markup.add(types.InlineKeyboardButton('Удалить сумму', callback_data='delete_summ'))
        markup.add(types.InlineKeyboardButton('План/Факт', callback_data='plan_fact'))
        markup.add(types.InlineKeyboardButton('Дополнительный расчет', callback_data='additional_calculation'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выбор', reply_markup=markup)



    elif call.data == 'back_buget_pl_fl':  # Вернуться с "План"
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Анализ', callback_data='analitics1'))
        markup.add(types.InlineKeyboardButton('План', callback_data='plan_rs'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_add_dop_rs1'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выбор', reply_markup=markup)


    elif call.data == 'back_burger1':  # Вернуться с "Корректировки"
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Таблица плана', callback_data='table_plan_rs'))
        markup.add(types.InlineKeyboardButton('Корректировка плана', callback_data='correct_plan'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget_pl_fl'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выбор', reply_markup=markup)


# =====================================================================================================================================================================================================   
    # ----------------------------------------------------------------------------------------------------------------------------------- Прогноз на 12 месяцев (Дин)
    # elif call.data == 'prog_dinam':
    #     bot.answer_callback_query(call.id)
    #     bot.send_message(call.message.chat.id, "В разработке")
    elif call.data == 'prog_dinam':
        forecast_dinam(call)

    elif call.data == 'forecast_method_1':
        build_forecast(call, "method_1")

    elif call.data == 'forecast_method_2':
        build_forecast(call, "method_2")

    elif call.data == 'forecast_method_3':
        build_forecast(call, "method_3")

    # -----------------------------------------------------------------------------------------------------------------------------------  "За конкретный месяц.год"
# Обработчик нажатия на кнопку "contr_month_table1"
    elif call.data == 'contr_month_table1':
        bot.send_message(call.message.chat.id, "Введите месяц с 1 по 12")
        bot.register_next_step_handler(call.message, process_month_step)

    # ----------------------------------------------------------------------------------------------------------------------------------- Вернутся назад "Фаил"


    elif call.data == 'prog_fiks':
        handle_prog_fiks(call)

    # ----------------------------------------------------------------------------------------------------------------------------------- Удаление суммы

    elif call.data == 'delete_summ':
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton('Удалить по ID', callback_data='delete_by_id'),
            types.InlineKeyboardButton('Удалить по дате', callback_data='delete_by_date')
        )
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_to_menu11'))
        bot.send_message(call.message.chat.id, "Выберите способ удаления:", reply_markup=markup)

    elif call.data == 'delete_by_id':
        msg = bot.send_message(call.message.chat.id, "Введите ID записи для удаления или нажмите /back для отмены:")
        bot.register_next_step_handler(msg, process_id_for_deletion)

    elif call.data == 'delete_by_date':
        msg = bot.send_message(call.message.chat.id, "Введите дату для удаления (в формате ДД.ММ.ГГГГ, например 10.10.2024) или /back для отмены:")
        bot.register_next_step_handler(msg, process_date_for_deletion)

    elif call.data == 'back_to_menu11':  # Вернуться с "Внесения суммы" в главное меню
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Посмотреть бюджет', callback_data='view_budget'))
        markup.row(types.InlineKeyboardButton('Внести сумму', callback_data='add_amount'),
                types.InlineKeyboardButton('История внесения', callback_data='history_add'))
        markup.add(types.InlineKeyboardButton('Удалить сумму', callback_data='delete_summ'))
        markup.add(types.InlineKeyboardButton('План/Факт', callback_data='plan_fact'))
        markup.add(types.InlineKeyboardButton('Дополнительный расчет', callback_data='additional_calculation'))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Выбор', reply_markup=markup)





    # ----------------------------------------------------------------------------------------------------------------------------------- Таблица плана
    elif call.data == 'table_plan_rs':
        try:
            with sqlite3.connect(db_path2) as db_rs:
                query = """
                SELECT ID, Groupe, Plan
                FROM sav_bot_rs
                ORDER BY ID
                """
                result_df = pd.read_sql_query(query, db_rs)

                # ✅ Создаём клавиатуру ДО отправки сообщения — будем использовать везде
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton('Таблица плана', callback_data='table_plan'))
                markup.add(types.InlineKeyboardButton('Корректировка плана', callback_data='correct_plan'))
                markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget_pl_fl'))

                if not result_df.empty:
                    # Форматируем план с пробелами как разделитель тысяч, округляем до 2 знаков
                    display_df = result_df.copy()
                    display_df['Plan'] = display_df['Plan'].apply(lambda x: f"{x:,.2f}".replace(',', ' '))

                    # Преобразуем в текстовую таблицу (вручную, без внешних зависимостей)
                    # Выравнивание по ширине колонок
                    col1_width = max(len(str(x)) for x in display_df['ID']) + 2
                    col2_width = max([len('Группа')] + [len(str(x)) for x in display_df['Groupe']]) + 2
                    col3_width = max([len('План')] + [len(str(x)) for x in display_df['Plan']]) + 2

                    lines = []
                    # Заголовок
                    header = f"{'ID'.ljust(col1_width)}{'Группа'.ljust(col2_width)}{'План'.ljust(col3_width)}"
                    lines.append(header)
                    lines.append("-" * (col1_width + col2_width + col3_width))

                    # Строки
                    for _, row in display_df.iterrows():
                        line = (
                            str(row['ID']).ljust(col1_width) +
                            str(row['Groupe']).ljust(col2_width) +
                            str(row['Plan']).ljust(col3_width)
                        )
                        lines.append(line)

                    table_text = "```\n" + "\n".join(lines) + "\n```"

                    # ✅ Отправляем текстовое сообщение (Markdown для моноширинного шрифта)
                    bot.send_message(
                        chat_id=call.message.chat.id,
                        text=table_text,
                        reply_markup=markup,
                        parse_mode='Markdown'
                    )

                else:
                    # ✅ Даже если таблица пуста — отправляем сообщение с кнопками
                    bot.send_message(
                        call.message.chat.id,
                        "Таблица плана пуста.",
                        reply_markup=markup
                    )

        except Exception as e:
            logger.error(f"Ошибка при работе с БД: {e}")

            # ✅ В случае ошибки — тоже отправляем сообщение с кнопками
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton('Таблица плана', callback_data='table_plan'))
            markup.add(types.InlineKeyboardButton('Корректировка плана', callback_data='correct_plan'))
            markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget_pl_fl'))

            bot.send_message(
                call.message.chat.id,
                "⚠️ Произошла ошибка при получении данных",
                reply_markup=markup
            )
        

    # # ----------------------------------------------------------------------------------------------------------------------------------- Анализ (План/Факт)
    elif call.data == 'analitics1':
        try:
            conn = sqlite3.connect(db_path1)
            cursor = conn.cursor()
            cursor.execute(f"ATTACH DATABASE '{db_path2.replace('\\', '/')}' AS db2")

            query = """
            SELECT 
                p.Groupe,
                p.Plan,
                COALESCE(f.Fact, 0) AS Fact,
                CASE 
                    WHEN p.Plan != 0 THEN ROUND(COALESCE(p.Plan, 0) - f.Fact, 2)
                    ELSE NULL
                END AS Deviation
            FROM db2.sav_bot_rs p
            LEFT JOIN (
                SELECT 
                    Groupe,
                    SUM(Price) AS Fact
                FROM sav_bot1
                WHERE strftime('%Y-%m', Date, 'localtime') = strftime('%Y-%m', 'now', 'localtime')
                GROUP BY Groupe
            ) f ON p.Groupe = f.Groupe
            """

            result_df = pd.read_sql_query(query, conn)

            # ✅ Готовим клавиатуру заранее — будем использовать в любом случае
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton('Анализ', callback_data='analitics1'))
            markup.add(types.InlineKeyboardButton('План', callback_data='plan_rs'))
            markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_add_dop_rs1'))

            if not result_df.empty:
                # Подготовка данных: форматирование чисел до 2 знаков, с пробелом как разделителем тысяч
                display_df = result_df.copy()
                display_df['Plan'] = display_df['Plan'].apply(lambda x: f"{x:,.2f}".replace(',', ' '))
                display_df['Fact'] = display_df['Fact'].apply(lambda x: f"{x:,.2f}".replace(',', ' '))
                display_df['Deviation'] = display_df['Deviation'].apply(
                    lambda x: f"{x:,.2f}".replace(',', ' ') if pd.notnull(x) else "—"
                )

                # Добавляем итоговую строку
                total_plan = result_df['Plan'].sum()
                total_fact = result_df['Fact'].sum()
                total_deviation = total_plan - total_fact

                total_row = {
                    'Groupe': 'ИТОГО',
                    'Plan': f"{total_plan:,.2f}".replace(',', ' '),
                    'Fact': f"{total_fact:,.2f}".replace(',', ' '),
                    'Deviation': f"{total_deviation:,.2f}".replace(',', ' ')
                }
                display_df = pd.concat([display_df, pd.DataFrame([total_row])], ignore_index=True)

                # → Преобразуем в текстовую таблицу (моноширинный шрифт)
                # Определяем ширину колонок
                col_groupe = max([len('Группа')] + [len(str(x)) for x in display_df['Groupe']]) + 2
                col_plan   = max([len('План')]   + [len(str(x)) for x in display_df['Plan']])   + 2
                col_fact   = max([len('Факт')]   + [len(str(x)) for x in display_df['Fact']])   + 2
                col_dev    = max([len('Откл')]    + [len(str(x)) for x in display_df['Deviation']]) + 2

                lines = []
                # Заголовок
                header = (
                    "Группа".ljust(col_groupe) +
                    "План".ljust(col_plan) +
                    "Факт".ljust(col_fact) +
                    "Откл".ljust(col_dev)
                )
                lines.append(header)
                lines.append("-" * (col_groupe + col_plan + col_fact + col_dev))

                # Строки данных
                for _, row in display_df.iterrows():
                    line = (
                        str(row['Groupe']).ljust(col_groupe) +
                        str(row['Plan']).ljust(col_plan) +
                        str(row['Fact']).ljust(col_fact) +
                        str(row['Deviation']).ljust(col_dev)
                    )
                    lines.append(line)

                table_text = "📊 *Анализ по группам*\n\n```\n" + "\n".join(lines) + "\n```"

                # ✅ Отправляем как текст (Markdown для моноширинного шрифта и жирного заголовка)
                bot.send_message(
                    chat_id=call.message.chat.id,
                    text=table_text,
                    reply_markup=markup,
                    parse_mode='Markdown'
                )

            else:
                bot.send_message(
                    call.message.chat.id,
                    "Нет данных для анализа.",
                    reply_markup=markup
                )

            cursor.execute("DETACH DATABASE db2")

        except Exception as e:
            logger.error(f"Ошибка при анализе данных: {e}")
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton('Анализ', callback_data='analitics1'))
            markup.add(types.InlineKeyboardButton('План', callback_data='plan_rs'))
            markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_add_dop_rs1'))
            bot.send_message(
                call.message.chat.id,
                "⚠️ Ошибка при формировании отчета",
                reply_markup=markup
            )

        finally:
            if 'conn' in locals():
                conn.close()



    # ----------------------------------------------------------------------------------------------------------------------------------- Обработка "Накопленная сумма"
    elif call.data == 'savings_summ':
        try:
            # Выполняем SQL-запрос
            query = """ SELECT SUM(Price) filter(WHERE Groupe IN ('Зп', 'Зп доп', 'Прибыль')) - SUM(Price) filter(WHERE Groupe  not IN ('Зп', 'Зп доп', 'Прибыль')) AS Сумма FROM sav_bot1 """
            result_df = pd.read_sql_query(query, db)  # Чтение данных в DataFrame
            
            # Преобразуем результат в число
            if not result_df.empty and not result_df.isna().any().any():
                total_sum = result_df.iloc[0, 0]  # Получаем значение из первой строки и первого столбца
                formatted_sum = "{:,.0f}".format(total_sum).replace(",", " ")  # Форматируем число с пробелами
                bot.send_message(call.message.chat.id, f"Сумма: {formatted_sum}")
            else:
                bot.send_message(call.message.chat.id, "Нет данных для отображения.")
        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            bot.send_message(call.message.chat.id, "Произошла ошибка при выполнении запроса.")
            
        # Отправляем меню кнопок после вывода суммы
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton('Накопленная сумма', callback_data='savings_summ'),
            types.InlineKeyboardButton('Накопления по месяцам', callback_data='savings_month')
        )
        markup.row(
            types.InlineKeyboardButton('Прибыль по месяцам', callback_data='profit_month'),
            types.InlineKeyboardButton('Прибыль по лицам', callback_data='profit_personal')
        )
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget2'))
        bot.send_message(call.message.chat.id, "Выберите опцию:", reply_markup=markup)
        
    elif call.data == 'add_to_db':
        add_to_db(call)  # Вызываем функцию добавления в БД

# ----------------------------------------------------------------------------------------------------------------------------------- Обработка "Накопления по месячно"
    elif call.data == 'savings_month':
        try:
            # Выполняем SQL-запрос
            query = """
                        select
                            month,
                            Сумма
                        from (
                            select
                                strftime('%m',"Date") as month,
                                strftime('%Y',"Date") as year,
                                COALESCE(SUM(Price) filter (where Groupe IN ('Зп', 'Зп доп', 'Прибыль')), 0) 
                                - 
                                COALESCE(SUM(Price) filter (where Groupe NOT IN ('Зп', 'Зп доп', 'Прибыль')), 0) AS Сумма
                            FROM sav_bot1
                            where "Date" >= DATE('now', '-7 months') 
                            Group by 1,2
                            ORDER BY year DESC, month DESC
                        ) q1
                    """
            result_df = pd.read_sql_query(query, db)  # Чтение данных в DataFrame
            
            # Проверяем, есть ли данные
            if not result_df.empty:
                # Создаем столбчатый график
                plt.figure(figsize=(10, 6))
                bars = plt.bar(result_df['month'], result_df['Сумма'], color='skyblue')
                
                # Добавляем цифры над столбцами
                for bar in bars:
                    height = bar.get_height()
                    plt.text(bar.get_x() + bar.get_width() / 2., height,
                            f'{int(height):,}'.replace(",", " "),  # Форматируем число с пробелами
                            ha='center', va='bottom')
                
                plt.xlabel('Месяц')
                plt.ylabel('Сумма')
                plt.title('Прибыль по месяцам')
                plt.xticks(rotation=45)
                plt.tight_layout()
                
                # Сохраняем график в буфер
                buf = io.BytesIO()
                plt.savefig(buf, format='png')
                buf.seek(0)
                
                # Отправляем график в чат
                bot.send_photo(call.message.chat.id, buf)
                plt.close()
            else:
                bot.send_message(call.message.chat.id, "Нет данных для отображения.")
        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            bot.send_message(call.message.chat.id, f"Произошла ошибка при выполнении запроса: {str(e)}")  # Выводим текст ошибки

        # Отправляем меню кнопок после вывода суммы
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton('Накопленная сумма', callback_data='savings_summ'),
            types.InlineKeyboardButton('Накопления по месяцам', callback_data='savings_month')
        )
        markup.row(
            types.InlineKeyboardButton('Прибыль по месяцам', callback_data='profit_month'),
            types.InlineKeyboardButton('Прибыль по лицам', callback_data='profit_personal')
        )
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget2'))
        bot.send_message(call.message.chat.id, "Выберите опцию:", reply_markup=markup)

    elif call.data == 'add_to_db':
        add_to_db(call)  # Вызываем функцию добавления в БД

# ----------------------------------------------------------------------------------------------------------------------------------- Обработка "Прибыль по месячно"
    elif call.data == 'profit_month':
        try:
            # Выполняем SQL-запрос
            query = """
                    select
                        month
                        ,Сумма
                    from
                    (SELECT
                            strftime('%m',"Date") as month
                            ,strftime('%Y',"Date") as year
                            ,SUM(Price) AS Сумма 
                    FROM sav_bot1
                    WHERE
                            "Date" >= DATE('now', '-7 months') 
                            and Groupe IN ('Зп', 'Зп доп', 'Прибыль')
                    Group by 1,2
                    ORDER BY
                        year DESC,
                        month DESC) q1
                    """
            result_df = pd.read_sql_query(query, db)  # Чтение данных в DataFrame
            
            # Проверяем, есть ли данные
            if not result_df.empty:
                # Создаем столбчатый график
                plt.figure(figsize=(10, 6))
                bars = plt.bar(result_df['month'], result_df['Сумма'], color='skyblue')
                
                # Добавляем цифры над столбцами
                for bar in bars:
                    height = bar.get_height()
                    plt.text(bar.get_x() + bar.get_width() / 2., height,
                            f'{int(height):,}'.replace(",", " "),  # Форматируем число с пробелами
                            ha='center', va='bottom')
                
                plt.xlabel('Месяц')
                plt.ylabel('Сумма')
                plt.title('Прибыль по месяцам')
                plt.xticks(rotation=45)
                plt.tight_layout()
                
                # Сохраняем график в буфер
                buf = io.BytesIO()
                plt.savefig(buf, format='png')
                buf.seek(0)
                
                # Отправляем график в чат
                bot.send_photo(call.message.chat.id, buf)
                plt.close()
            else:
                bot.send_message(call.message.chat.id, "Нет данных для отображения.")
        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            bot.send_message(call.message.chat.id, f"Произошла ошибка при выполнении запроса: {str(e)}")  # Выводим текст ошибки

        # Отправляем меню кнопок после вывода суммы
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton('Накопленная сумма', callback_data='savings_summ'),
            types.InlineKeyboardButton('Накопления по месяцам', callback_data='savings_month')
        )
        markup.row(
            types.InlineKeyboardButton('Прибыль по месяцам', callback_data='profit_month'),
            types.InlineKeyboardButton('Прибыль по лицам', callback_data='profit_personal')
        )
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget2'))
        bot.send_message(call.message.chat.id, "Выберите опцию:", reply_markup=markup)

    elif call.data == 'add_to_db':
        add_to_db(call)  # Вызываем функцию добавления в БД


# ----------------------------------------------------------------------------------------------------------------------------------- Обработка "Прибыль по лицам"
    elif call.data == 'profit_personal':
        try:
            # Выполняем SQL-запрос
            query = """
                    SELECT
                        strftime('%Y-%m', "Date") AS month_year,
                        Groupe,
                        SUM(Price) AS Сумма
                    FROM sav_bot1
                    WHERE
                        "Date" >= DATE('now', '-7 months')
                        AND Groupe IN ('Зп', 'Зп доп', 'Прибыль')
                    GROUP BY month_year, Groupe
                    ORDER BY month_year DESC
                    """
            result_df = pd.read_sql_query(query, db)  # Чтение данных в DataFrame
            
            # Проверяем, есть ли данные
            if not result_df.empty:
                # Преобразуем данные для построения графика
                pivot_df = result_df.pivot(index='month_year', columns='Groupe', values='Сумма')
                
                # Создаем группированный столбчатый график
                ax = pivot_df.plot(kind='bar', figsize=(12, 6), color=['skyblue', 'lightgreen', 'salmon'])
                
                # Добавляем цифры над столбцами
                for p in ax.patches:
                    if not pd.isna(p.get_height()):  # Проверяем, что значение не NaN
                        ax.annotate(f'{int(p.get_height()):,}'.replace(",", " "), (p.get_x() + p.get_width() / 2., p.get_height()),
                                    ha='center', va='bottom', fontsize=8, color='black', xytext=(0, 5),
                                    textcoords='offset points')
                
                plt.xlabel('Месяц')
                plt.ylabel('Сумма')
                plt.title('Прибыль по месяцам и группам')
                plt.xticks(rotation=45)
                plt.tight_layout()
                
                # Сохраняем график в буфер
                buf = io.BytesIO()
                plt.savefig(buf, format='png')
                buf.seek(0)
                
                # Отправляем график в чат
                bot.send_photo(call.message.chat.id, buf)
                plt.close()
            else:
                bot.send_message(call.message.chat.id, "Нет данных для отображения.")
        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            bot.send_message(call.message.chat.id, f"Произошла ошибка при выполнении запроса: {str(e)}")  # Выводим текст ошибки

        # Отправляем меню кнопок после вывода суммы
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton('Накопленная сумма', callback_data='savings_summ'),
            types.InlineKeyboardButton('Накопления по месяцам', callback_data='savings_month')
        )
        markup.row(
            types.InlineKeyboardButton('Прибыль по месяцам', callback_data='profit_month'),
            types.InlineKeyboardButton('Прибыль по лицам', callback_data='profit_personal')
        )
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget2'))
        bot.send_message(call.message.chat.id, "Выберите опцию:", reply_markup=markup)

    elif call.data == 'add_to_db':
        add_to_db(call)  # Вызываем функцию добавления в БД

    # ----------------------------------------------------------------------------------------------------------------------------------- Обработка "Общие траты"
    elif call.data == 'general':
        try:
            # Выполняем SQL-запрос
            query = """
            select 
                SUM(Price) as Сумма
            from sav_bot1
            where Groupe not in ('Зп','Зп доп','Прибыль')
                and strftime('%Y-%m', Date,'localtime') = strftime('%Y-%m', 'now','localtime')
            """
            
            result_df = pd.read_sql_query(query, db)  # Чтение данных в DataFrame
            
            # Преобразуем результат в число
            if not result_df.empty and not result_df.isna().any().any():
                total_sum1 = result_df.iloc[0, 0]  # Получаем значение из первой строки и первого столбца
                formatted_sum = "{:,.0f}".format(total_sum1).replace(",", " ")  # Форматируем число с пробелами
                bot.send_message(call.message.chat.id, f"Сумма: {formatted_sum}")
            else:
                bot.send_message(call.message.chat.id, "Нет данных для отображения.")
        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            bot.send_message(call.message.chat.id, "Произошла ошибка при выполнении запроса.")
            

        # Отправляем меню кнопок после вывода суммы
        markup = types.InlineKeyboardMarkup()
        markup.row(types.InlineKeyboardButton('За текущий месяц', callback_data='general')
                   ,types.InlineKeyboardButton('Граппа за месяц', callback_data='in_terms_group')
                   ,types.InlineKeyboardButton('1 группа за месяц',callback_data='one_group_month')
                   )
        markup.row(types.InlineKeyboardButton('Текущий, прошлый месяц', callback_data='in_terms_groupe')
                    ,types.InlineKeyboardButton('За 6 месяцев', callback_data='in_six_groupes'))
        markup.row(types.InlineKeyboardButton('За любой месяц.год', callback_data='contr_month_table1')
                   ,types.InlineKeyboardButton('1 группа за любой месяц.год',callback_data='one_contr_group_month'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget3'))

    elif call.data == 'add_to_db':
        add_to_db(call)  # Вызываем функцию добавления в БД

    # ----------------------------------------------------------------------------------------------------------------------------------- Обработка "В разрезе Групп"
    elif call.data == 'in_terms_group':
        try:
            # Выполняем SQL-запрос
            query = """
            SELECT
                Groupe AS Группы,
                SUM(Price) AS Сумма
            FROM sav_bot1
            WHERE Groupe NOT IN ('Зп','Зп доп','Прибыль')
                AND strftime('%Y-%m', Date, 'localtime') = strftime('%Y-%m', 'now', 'localtime')
            GROUP BY 1
            """
            
            result_df = pd.read_sql_query(query, db)

            # Формируем клавиатуру (вынесем вверх для единообразия)
            markup = types.InlineKeyboardMarkup()
            markup.row(
                types.InlineKeyboardButton('За текущий месяц', callback_data='general'),
                types.InlineKeyboardButton('Группа за месяц', callback_data='in_terms_group'),
                types.InlineKeyboardButton('1 группа за месяц', callback_data='one_group_month')
            )
            markup.row(
                types.InlineKeyboardButton('Текущий, прошлый месяц', callback_data='in_terms_groupe'),
                types.InlineKeyboardButton('За 6 месяцев', callback_data='in_six_groupes')
            )
            markup.row(
                types.InlineKeyboardButton('За любой месяц.год', callback_data='contr_month_table1'),
                types.InlineKeyboardButton('1 группа за любой месяц.год', callback_data='one_contr_group_month')
            )
            markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget3'))

            if not result_df.empty:
                # Форматируем сумму с пробелами как разделитель тысяч и 2 знака после запятой
                display_df = result_df.copy()
                display_df['Сумма'] = display_df['Сумма'].apply(lambda x: f"{x:,.2f}".replace(',', ' '))

                # Создаём изображение таблицы
                fig, ax = plt.subplots(figsize=(8, max(3, 0.45 * len(display_df))))
                ax.axis('tight')
                ax.axis('off')

                table = ax.table(
                    cellText=display_df.values,
                    colLabels=display_df.columns,
                    cellLoc='center',
                    loc='center'
                )

                # Настройка стиля
                table.auto_set_font_size(False)
                table.set_fontsize(10)
                table.scale(1.2, 1.5)

                # Стилизация заголовков
                for i in range(len(display_df.columns)):
                    table[(0, i)].set_facecolor('#2196F3')
                    table[(0, i)].set_text_props(weight='bold', color='white')

                # Сохраняем в буфер
                buf = BytesIO()
                plt.savefig(buf, format='png', bbox_inches='tight', dpi=150, facecolor='white')
                buf.seek(0)
                plt.close(fig)

                # Отправляем изображение с клавиатурой
                bot.send_photo(call.message.chat.id, photo=buf, reply_markup=markup)

            else:
                bot.send_message(call.message.chat.id, "Нет данных для отображения.", reply_markup=markup)

        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            # Формируем клавиатуру и при ошибке
            markup = types.InlineKeyboardMarkup()
            markup.row(
                types.InlineKeyboardButton('За текущий месяц', callback_data='general'),
                types.InlineKeyboardButton('Группа за месяц', callback_data='in_terms_group'),
                types.InlineKeyboardButton('1 группа за месяц', callback_data='one_group_month')
            )
            markup.row(
                types.InlineKeyboardButton('Текущий, прошлый месяц', callback_data='in_terms_groupe'),
                types.InlineKeyboardButton('За 6 месяцев', callback_data='in_six_groupes')
            )
            markup.row(
                types.InlineKeyboardButton('За любой месяц.год', callback_data='contr_month_table1'),
                types.InlineKeyboardButton('1 группа за любой месяц.год', callback_data='one_contr_group_month')
            )
            markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget3'))
            bot.send_message(call.message.chat.id, "Произошла ошибка при выполнении запроса.", reply_markup=markup)


    # ----------------------------------------------------------------------------------------------------------------------------------- Обработка "Текущий и прошлый месяц"
    elif call.data == 'in_terms_groupe':
        try:
            # Выполняем SQL-запрос
            query = """
            select
                strftime('%Y-%m', Date) as Dates
                ,Groupe as Группы
                ,SUM(Price) as Сумма
            from sav_bot1
            where Groupe not in ('Зп','Зп доп','Прибыль')
                and (strftime('%Y-%m', Date,'localtime') = strftime('%Y-%m', 'now','localtime') or strftime('%Y-%m', "Date",'localtime') = strftime('%Y-%m', DATE('now', '-1 month')))
            group by 1,2
            """
            
            result_df = pd.read_sql_query(query, db)  # Чтение данных в DataFrame

            # Проверяем, есть ли данные
            if not result_df.empty:
                # Преобразуем данные для построения графика
                pivot_df = result_df.pivot(index='Группы', columns='Dates', values='Сумма')

                # Построение графика
                ax = pivot_df.plot(kind='barh', figsize=(12, 8))
                plt.title('Сумма по группам за 2 месяца')
                plt.xlabel('Группы')
                plt.ylabel('Сумма')
                plt.xticks(rotation=45)
                plt.legend(title='Даты')
                plt.tight_layout()

                # Добавляем метки на столбцы
                for container in ax.containers:
                    ax.bar_label(container, fmt='%.1f', padding=3)  # fmt='%.1f' для отображения чисел с одним знаком после запятой

                # Сохраняем график в файл
                plt.savefig('grouped_bar_chart.png')
                plt.close()

                # Отправляем график в чат
                with open('grouped_bar_chart.png', 'rb') as photo:
                    bot.send_photo(call.message.chat.id, photo)
            else:
                bot.send_message(call.message.chat.id, "Нет данных для отображения.")
        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            bot.send_message(call.message.chat.id, "Произошла ошибка при выполнении запроса.")
            
        # Отправляем меню кнопок после вывода суммы
        markup = types.InlineKeyboardMarkup()
        markup.row(types.InlineKeyboardButton('За текущий месяц', callback_data='general')
                   ,types.InlineKeyboardButton('Граппа за месяц', callback_data='in_terms_group')
                   ,types.InlineKeyboardButton('1 группа за месяц',callback_data='one_group_month')
                   )
        markup.row(types.InlineKeyboardButton('Текущий, прошлый месяц', callback_data='in_terms_groupe')
                    ,types.InlineKeyboardButton('За 6 месяцев', callback_data='in_six_groupes'))
        markup.row(types.InlineKeyboardButton('За любой месяц.год', callback_data='contr_month_table1')
                   ,types.InlineKeyboardButton('1 группа за любой месяц.год',callback_data='one_contr_group_month'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget3'))
        
            
    elif call.data == 'add_to_db':
        add_to_db(call)  # Вызываем функцию добавления в БД

# ===================================================================================================================================================================================================== Траты за 6 месяцев

    elif call.data == 'in_six_groupes':
        try:
            # Выполняем SQL-запрос (без остановки polling)
            query = """
                    SELECT
                        strftime('%m.%Y', Date, 'localtime') as Dates,
                        Groupe as Группы,
                        SUM(Price) AS Сумма
                    FROM sav_bot1
                    WHERE Groupe NOT IN ('Зп', 'Зп доп', 'Прибыль')
                        AND strftime('%Y-%m', Date, 'localtime') BETWEEN strftime('%Y-%m', DATE('now', '-5 month'), 'localtime') 
                                                                AND strftime('%Y-%m', 'now', 'localtime')
                    GROUP BY Dates, Groupe
                ORDER BY 1 DESC
            """
            
            result_df = pd.read_sql_query(query, db)

            if not result_df.empty:
                pivot_df = result_df.pivot(index='Группы', columns='Dates', values='Сумма')

                # Увеличиваем размер фигуры (ширина, высота)
                plt.figure(figsize=(16, 12))
                
                # Увеличиваем расстояние между группами (параметр width)
                ax = pivot_df.plot(kind='barh', width=0.85, figsize=(16, 12))
                
                # Настройки внешнего вида
                plt.title('Сумма по группам за последние 6 месяцев', fontsize=12, pad=15)
                plt.xlabel('Сумма', fontsize=10)
                plt.ylabel('Группы', fontsize=10)
                
                # Уменьшаем шрифт подписей осей
                plt.xticks(fontsize=8)
                plt.yticks(fontsize=8)
                
                # Настраиваем легенду (уменьшаем шрифт и делаем компактнее)
                plt.legend(
                    title='Месяц.Год', 
                    fontsize=8, 
                    title_fontsize=9,
                    bbox_to_anchor=(1.02, 1),
                    loc='upper left',
                    borderaxespad=0.
                )
                
                # Увеличиваем расстояние между столбцами (через параметр subplots)
                plt.subplots_adjust(left=0.3, right=0.75, top=0.9, bottom=0.1)
                
                # Добавляем метки только для достаточно широких столбцов
                for container in ax.containers:
                    ax.bar_label(
                        container, 
                        fmt='%.0f', 
                        padding=2,
                        fontsize=7,  # Уменьшаем шрифт меток
                        label_type='edge',
                        labels=[f'{x:,.0f}' if x > ax.get_xlim()[1]*0.03 else '' for x in container.datavalues]
                    )
                
                # Сохраняем с высоким качеством
                plt.savefig('grouped_bar_chart.png', dpi=300, bbox_inches='tight')
                plt.close()

                with open('grouped_bar_chart.png', 'rb') as photo:
                    bot.send_photo(call.message.chat.id, photo)
            else:
                bot.send_message(call.message.chat.id, "Нет данных для отображения.")
        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            bot.send_message(call.message.chat.id, "Произошла ошибка при выполнении запроса.")

        # Меню кнопок остается без изменений
        try:
            markup = types.InlineKeyboardMarkup()
            markup.row(
                types.InlineKeyboardButton('За текущий месяц', callback_data='general'),
                types.InlineKeyboardButton('Граппа за месяц', callback_data='in_terms_group'),
                types.InlineKeyboardButton('1 группа за месяц', callback_data='one_group_month')
            )
            markup.row(
                types.InlineKeyboardButton('Текущий, прошлый месяц', callback_data='in_terms_groupe'),
                types.InlineKeyboardButton('За 6 месяцев', callback_data='in_six_groupes')
            )
            markup.row(
                types.InlineKeyboardButton('За любой месяц.год', callback_data='contr_month_table1'),
                types.InlineKeyboardButton('1 группа за любой месяц.год', callback_data='one_contr_group_month')
            )
            markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget3'))

            bot.edit_message_reply_markup(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=markup
            )
        except Exception as e:
            logger.error(f"Ошибка при обновлении меню: {e}")

# ===================================================================================================================================================================================================== История внесений 
# -----------------------------------------------------------------------------------------------------------------------today_history


    elif call.data == 'today_history':
        try:
            # Выполняем SQL-запрос
            query = """
            SELECT
                ID,
                Date,
                Groupe,
                Price,
                Description
            FROM sav_bot1
            WHERE strftime('%Y-%m-%d', Date,'localtime') = strftime('%Y-%m-%d', 'now','localtime')
            ORDER BY Date, CASE WHEN Groupe IN ('Прибыль', 'Зп', 'Зп доп') THEN 1 ELSE 2 END
            """
            
            # Чтение данных в DataFrame
            result_df = pd.read_sql_query(query, db)

            # Проверяем, есть ли данные
            if not result_df.empty:
                # Создаем графическое представление таблицы
                fig, ax = plt.subplots(figsize=(10, 4))  # Размер изображения
                ax.axis('tight')
                ax.axis('off')
                table = ax.table(
                    cellText=result_df.values,
                    colLabels=result_df.columns,
                    cellLoc='center',
                    loc='center'
                )

                # Настройка стиля таблицы
                table.auto_set_font_size(False)
                table.set_fontsize(10)
                table.scale(1.2, 1.2)

                # Сохраняем изображение в буфер
                buf = BytesIO()
                plt.savefig(buf, format='png', bbox_inches='tight')
                buf.seek(0)
                plt.close()

                # Отправляем изображение в чат
                bot.send_photo(call.message.chat.id, buf)
            else:
                bot.send_message(call.message.chat.id, "Нет данных для отображения.")
        except Exception as e:
            # Логируем ошибку для отладки
            print(f"Ошибка при выполнении запроса: {e}")  # Используйте print, если логгер не настроен
            bot.send_message(call.message.chat.id, f"Произошла ошибка: {e}")  # Отправляем пользователю текст ошибки
            
        # Отправляем меню кнопок после вывода суммы
        markup = types.InlineKeyboardMarkup()
        markup.row(types.InlineKeyboardButton('За сегодня', callback_data='today_history')
                   ,types.InlineKeyboardButton('За вчера', callback_data='yesterday_history')
                   ,types.InlineKeyboardButton('За неделю', callback_data='week_history')
                   ,types.InlineKeyboardButton('За период', callback_data='period_history'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_history1'))
        bot.send_message(call.message.chat.id, "Выбор", reply_markup=markup)
        
    elif call.data == 'add_to_db':
        add_to_db(call)  # Вызываем функцию добавления в БД



# -----------------------------------------------------------------------------------------------------------------------yesterday_history
    elif call.data == 'yesterday_history':
        try:
            query = """
                select
                    ID,
                    Date,
                    Groupe,
                    Price,
                    Description
                from sav_bot1
                where strftime('%Y-%m-%d', Date,'localtime') = strftime('%Y-%m-%d', DATE('now', '-1 day','localtime'))
                order by Date, case when Groupe in ('Прибыль', 'Зп', 'Зп доп') then 1 else 2 end
                """
            # Чтение данных в DataFrame
            result_df = pd.read_sql_query(query, db)

            # Проверяем, есть ли данные
            if not result_df.empty:
                # Создаем графическое представление таблицы
                fig, ax = plt.subplots(figsize=(10, 4))  # Размер изображения
                ax.axis('tight')
                ax.axis('off')
                table = ax.table(
                    cellText=result_df.values,
                    colLabels=result_df.columns,
                    cellLoc='center',
                    loc='center'
                )

                # Настройка стиля таблицы
                table.auto_set_font_size(False)
                table.set_fontsize(10)
                table.scale(1.2, 1.2)

                # Сохраняем изображение в буфер
                buf = BytesIO()
                plt.savefig(buf, format='png', bbox_inches='tight')
                buf.seek(0)
                plt.close()

                # Отправляем изображение в чат
                bot.send_photo(call.message.chat.id, buf)
            else:
                bot.send_message(call.message.chat.id, "Нет данных для отображения.")
        except Exception as e:
            # Логируем ошибку для отладки
            print(f"Ошибка при выполнении запроса: {e}")  # Используйте print, если логгер не настроен
            bot.send_message(call.message.chat.id, f"Произошла ошибка: {e}")  # Отправляем пользователю текст ошибки
            
        # Отправляем меню кнопок после вывода суммы
        markup = types.InlineKeyboardMarkup()
        markup.row(types.InlineKeyboardButton('За сегодня', callback_data='today_history')
                   ,types.InlineKeyboardButton('За вчера', callback_data='yesterday_history')
                   ,types.InlineKeyboardButton('За неделю', callback_data='week_history')
                   ,types.InlineKeyboardButton('За период', callback_data='period_history'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_history1'))
        bot.send_message(call.message.chat.id, "Выбор", reply_markup=markup)
        
    elif call.data == 'add_to_db':
        add_to_db(call)  # Вызываем функцию добавления в БД


# -----------------------------------------------------------------------------------------------------------------------week_history
    elif call.data == 'week_history':
        try:
            query = """
            with ranks as (select * ,dense_rank() over(order by Date) as rank from sav_bot1)
                ,ranks_max as ( select max(rank) as max_rank from ranks)
            select
                ID,
                Date
                ,Groupe
                ,Price
                ,Description
            from ranks
            where rank >= (select max_rank - 6 from ranks_max)
            """
            # Чтение данных в DataFrame
            result_df = pd.read_sql_query(query, db)

            # Проверяем, есть ли данные
            if not result_df.empty:
                # Создаем графическое представление таблицы
                fig, ax = plt.subplots(figsize=(10, 4))  # Размер изображения
                ax.axis('tight')
                ax.axis('off')
                table = ax.table(
                    cellText=result_df.values,
                    colLabels=result_df.columns,
                    cellLoc='center',
                    loc='center'
                )

                # Настройка стиля таблицы
                table.auto_set_font_size(False)
                table.set_fontsize(10)
                table.scale(1.2, 1.2)

                # Сохраняем изображение в буфер
                buf = BytesIO()
                plt.savefig(buf, format='png', bbox_inches='tight')
                buf.seek(0)
                plt.close()

                # Отправляем изображение в чат
                bot.send_photo(call.message.chat.id, buf)
            else:
                bot.send_message(call.message.chat.id, "Нет данных для отображения.")
        except Exception as e:
            # Логируем ошибку для отладки
            print(f"Ошибка при выполнении запроса: {e}")  # Используйте print, если логгер не настроен
            bot.send_message(call.message.chat.id, f"Произошла ошибка: {e}")  # Отправляем пользователю текст ошибки
            
        # Отправляем меню кнопок после вывода суммы
        markup = types.InlineKeyboardMarkup()
        markup.row(types.InlineKeyboardButton('За сегодня', callback_data='today_history')
                   ,types.InlineKeyboardButton('За вчера', callback_data='yesterday_history')
                   ,types.InlineKeyboardButton('За неделю', callback_data='week_history')
                   ,types.InlineKeyboardButton('За период', callback_data='period_history'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_history1'))
        bot.send_message(call.message.chat.id, "Выбор", reply_markup=markup)
        
    elif call.data == 'add_to_db':
        add_to_db(call)  # Вызываем функцию добавления в БД


# ----------------------------------------------------------------------------------------------------------------------- period_history (1)

    elif call.data == 'period_history':
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
            msg = bot.send_message(
                call.message.chat.id,
                "📅 Введите начальную дату в формате ДД.ММ.ГГГГ\n"
                "Например: 01.05.2025"
            )
            bot.register_next_step_handler(msg, process_start_date_step)
        except Exception as e:
            print(f"Ошибка в period_history: {e}")
            show_history_menu(call.message.chat.id)


# -----------------------------------------------------------------------------------------------------------------------------------------                Траты по 1 группе за любой месяц.год
    elif call.data == 'one_contr_group_month':
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton('Продукты', callback_data='foot_s2'),
            types.InlineKeyboardButton('Квартира', callback_data='apartment_s2'),
            types.InlineKeyboardButton('Развлечение', callback_data='entertainment_s2')
        )
        markup.row(
            types.InlineKeyboardButton('Дополнительное', callback_data='extra_s2'),
            types.InlineKeyboardButton('Личные траты', callback_data='personal_s2'),
            types.InlineKeyboardButton('Машина', callback_data='car_s2')
        )
        markup.row(
            types.InlineKeyboardButton('Одежда', callback_data='clothes_s2'),
            types.InlineKeyboardButton('Перевод', callback_data='transfer_s2'),
            types.InlineKeyboardButton('Потери', callback_data='losses_s2')
        )
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='expenses'))
        bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text='Выберите опцию:',
            reply_markup=markup
        )

    elif call.data in names_tr_s2.keys():
        try:
            group_name = names_tr_s2[call.data]
            user_id = call.message.chat.id
            
            # Сохраняем выбранную группу
            user_states[user_id] = {
                'group_name': group_name,
                'active': True
            }
            
            msg = bot.send_message(user_id, f"Выбрана категория: {group_name}\nВведите месяц (1-12):")
            bot.register_next_step_handler(msg, process_month_step_for_group)
            
        except Exception as e:
            print(f"Ошибка: {e}")
            bot.send_message(call.message.chat.id, "Ошибка при выборе категории")
    
# -----------------------------------------------------------------------------------------------------------------------------------------  Траты по 1  группе за месяц
    elif call.data == 'one_group_month':
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton('Продукты', callback_data='foot_1'),
            types.InlineKeyboardButton('Квартира', callback_data='apartment_1'),
            types.InlineKeyboardButton('Развлечение', callback_data='entertainment_1')
        )
        markup.row(
            types.InlineKeyboardButton('Дополнительное', callback_data='extra_1'),
            types.InlineKeyboardButton('Личные траты', callback_data='personal_1'),
            types.InlineKeyboardButton('Машина', callback_data='car_1')
        )
        markup.row(
            types.InlineKeyboardButton('Одежда', callback_data='clothes_1'),
            types.InlineKeyboardButton('Перевод', callback_data='transfer_1'),
            types.InlineKeyboardButton('Потери', callback_data='losses_1')
        )
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='expenses'))
        bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text='Выберите опцию:',
            reply_markup=markup
        )
        
# ===================================================================================================================================================================================================== траты по одной группе за текущий месяц 

    elif call.data in names_tr_1.keys():  # Обработка нажатий на кнопки групп
        try:
            # Получаем название группы из словаря
            group_name = names_tr_1[call.data]
            
            # SQL запрос с параметром
            query = """
            SELECT Groupe, Description, Price  
            FROM sav_bot1  
            WHERE strftime('%Y', "Date") = strftime('%Y', 'now')
            AND strftime('%m', "Date") = strftime('%m', 'now')
            AND Groupe = ?
            """
            
            # Чтение данных в DataFrame с параметром
            result_df = pd.read_sql_query(query, db, params=(group_name,))

            # Проверяем, есть ли данные
            if not result_df.empty:
                # Добавляем строку с итоговой суммой
                total_row = pd.DataFrame({
                    'Groupe': ['Итого:'],
                    'Description': [''],
                    'Price': [result_df['Price'].sum()]
                })
                result_df = pd.concat([result_df, total_row], ignore_index=True)
                
                # Создаем графическое представление таблицы
                fig, ax = plt.subplots(figsize=(10, 4))  # Размер изображения
                ax.axis('tight')
                ax.axis('off')
                table = ax.table(
                    cellText=result_df.values,
                    colLabels=result_df.columns,
                    cellLoc='center',
                    loc='center'
                )

                # Настройка стиля таблицы
                table.auto_set_font_size(False)
                table.set_fontsize(10)
                table.scale(1.2, 1.2)

                # Сохраняем изображение в буфер
                buf = BytesIO()
                plt.savefig(buf, format='png', bbox_inches='tight')
                buf.seek(0)
                plt.close()

                # Отправляем изображение в чат
                bot.send_photo(call.message.chat.id, buf, caption=f"Расходы по категории: {group_name}")
            else:
                bot.send_message(call.message.chat.id, f"Нет данных по категории: {group_name}")
                
        except Exception as e:
            # Логируем ошибку для отладки
            print(f"Ошибка при выполнении запроса: {e}")
            bot.send_message(call.message.chat.id, f"Произошла ошибка: {e}")
        # Отправляем меню кнопок после вывода суммы
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton('Продукты', callback_data='foot_1'),
            types.InlineKeyboardButton('Квартира', callback_data='apartment_1'),
            types.InlineKeyboardButton('Развлечение', callback_data='entertainment_1')
        )
        markup.row(
            types.InlineKeyboardButton('Дополнительное', callback_data='extra_1'),
            types.InlineKeyboardButton('Личные траты', callback_data='personal_1'),
            types.InlineKeyboardButton('Машина', callback_data='car_1')
        )
        markup.row(
            types.InlineKeyboardButton('Одежда', callback_data='clothes_1'),
            types.InlineKeyboardButton('Перевод', callback_data='transfer_1'),
            types.InlineKeyboardButton('Потери', callback_data='losses_1')
        )
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='expenses'))
        bot.send_message(call.message.chat.id, "Выбор", reply_markup=markup)
        
    elif call.data == 'add_to_db':
        add_to_db(call)  # Вызываем функцию добавления в БД

# ===================================================================================================================================================================================================== Корректировка план факт

def process_plan_amount(message, group_name):
    try:
        amount = float(message.text)
        # Сохраняем сумму в временных данных пользователя
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Да', callback_data=f'confirm_{group_name}_{amount}'))
        markup.add(types.InlineKeyboardButton('Назад', callback_data='table_plan'))
        
        bot.send_message(message.chat.id, 
                       f"Вы выбрали: {group_name}\nСумма: {amount}\nПодтверждаете?",
                       reply_markup=markup)
    except ValueError:
        msg = bot.send_message(message.chat.id, 'Пожалуйста, введите корректную сумму (число):')
        bot.register_next_step_handler(msg, process_plan_amount, group_name=group_name)

def process_plan_id(message):
    try:
        record_id = int(message.text)
        # Проверяем существование ID в базе
        db_rs = sqlite3.connect(db_path2)
        c1 = db_rs.cursor()
        c1.execute("SELECT Groupe FROM sav_bot_rs WHERE ID = ?", (record_id,))
        result = c1.fetchone()
        db_rs.close()
        
        if result:
            group_name = result[0]
            msg = bot.send_message(message.chat.id, f'Найдена запись: {group_name}\nВведите новую сумму плана:')
            bot.register_next_step_handler(msg, process_plan_amount_id, record_id=record_id)
        else:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton('← Назад', callback_data='correct_plan'))
            bot.send_message(message.chat.id, f'Запись с ID {record_id} не найдена.', reply_markup=markup)
    except ValueError:
        msg = bot.send_message(message.chat.id, 'Пожалуйста, введите корректный ID (целое число):')
        bot.register_next_step_handler(msg, process_plan_id)

def process_plan_amount_id(message, record_id):
    try:
        amount = float(message.text)
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Да', callback_data=f'confirm_id_{record_id}_{amount}'))
        markup.add(types.InlineKeyboardButton('Назад', callback_data='correct_plan_id'))
        
        bot.send_message(message.chat.id, 
                       f"ID записи: {record_id}\nНовая сумма: {amount}\nПодтверждаете?",
                       reply_markup=markup)
    except ValueError:
        msg = bot.send_message(message.chat.id, 'Пожалуйста, введите корректную сумму (число):')
        bot.register_next_step_handler(msg, process_plan_amount_id, record_id=record_id)

# ===================================================================================================================================================================================================== period_history (2)

def parse_custom_date(date_str):
    """Парсит дату из формата ДД.ММ.ГГГГ и возвращает в формате ГГГГ-ММ-ДД"""
    day, month, year = map(int, date_str.split('.'))
    return datetime.date(year, month, day).strftime("%Y-%m-%d")

def process_start_date_step(message):
    try:
        chat_id = message.chat.id
        date_str = message.text.strip()
        
        # Парсим дату из формата ДД.ММ.ГГГГ
        start_date = parse_custom_date(date_str)
        
        msg = bot.send_message(
            chat_id,
            "📅 Введите конечную дату в формате ДД.ММ.ГГГГ\n"
            "Например: 10.05.2025"
        )
        bot.register_next_step_handler(msg, lambda m: process_end_date_step(m, start_date))
        
    except ValueError:
        msg = bot.send_message(
            chat_id,
            "❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ\n"
            "Пример: 01.05.2025"
        )
        bot.register_next_step_handler(msg, process_start_date_step)
    except Exception as e:
        print(f"Ошибка в process_start_date_step: {e}")
        show_history_menu(chat_id)

def process_end_date_step(message, start_date):
    try:
        chat_id = message.chat.id
        date_str = message.text.strip()
        
        # Парсим дату из формата ДД.ММ.ГГГГ
        end_date = parse_custom_date(date_str)
        
        # Проверяем, что конечная дата не раньше начальной
        if end_date < start_date:
            raise ValueError("Конечная дата должна быть позже начальной")
        
        # Формируем SQL-запрос
        query = """
        SELECT 
            ID,
            Date,
            Groupe,
            Price,
            Description
        FROM sav_bot1
        WHERE date(Date) BETWEEN date(?) AND date(?)
        ORDER BY Date
        """
        
        # Выполняем запрос
        result_df = pd.read_sql_query(query, db, params=(start_date, end_date))

        if not result_df.empty:
            # Создаем табличное представление
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.axis('tight')
            ax.axis('off')
            table = ax.table(
                cellText=result_df.values,
                colLabels=result_df.columns,
                cellLoc='center',
                loc='center'
            )
            
            table.auto_set_font_size(False)
            table.set_fontsize(10)
            table.scale(1.2, 1.2)
            
            # Сохраняем изображение
            buf = BytesIO()
            plt.savefig(buf, format='png', bbox_inches='tight')
            buf.seek(0)
            plt.close()
            
            # Отправляем результат
            start_display = datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime("%d.%m.%Y")
            end_display = datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime("%d.%m.%Y")
            
            bot.send_photo(
                chat_id, 
                buf, 
                caption=f"📊 Данные за период: {start_display} - {end_display}"
            )
        else:
            start_display = datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime("%d.%m.%Y")
            end_display = datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime("%d.%m.%Y")
            bot.send_message(
                chat_id, 
                f"🔍 Данных за период {start_display} - {end_display} не найдено"
            )
            
    except ValueError as e:
        msg = bot.send_message(
            chat_id,
            f"❌ Ошибка: {str(e)}\n"
            "Введите дату в формате ДД.ММ.ГГГГ\n"
            "Пример: 10.05.2025"
        )
        bot.register_next_step_handler(msg, lambda m: process_end_date_step(m, start_date))
    except Exception as e:
        print(f"Ошибка в process_end_date_step: {e}")
        bot.send_message(chat_id, "⚠️ Произошла ошибка при обработке запроса")
    finally:
        show_history_menu(chat_id)

def show_history_menu(chat_id):
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton('За сегодня', callback_data='today_history'),
        types.InlineKeyboardButton('За вчера', callback_data='yesterday_history'),
        types.InlineKeyboardButton('За неделю', callback_data='week_history'),
        types.InlineKeyboardButton('За период', callback_data='period_history')
    )
    markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_history1'))
    bot.send_message(chat_id, "📊 Выберите период для просмотра истории:", reply_markup=markup)

# ===================================================================================================================================================================================================== траты по одной группе за за месяц.год


def process_month_step_for_group(message):
    user_id = message.chat.id
    
    if user_id not in user_states or not user_states[user_id].get('active'):
        bot.send_message(user_id, "⚠️ Сначала выберите категорию!")
        return
        
    try:
        if message.text.strip().lower() == 'stop':
            handle_stop_button(message)
            return
            
        month = int(message.text)
        
        if month < 1 or month > 12:
            msg = bot.send_message(user_id, "Введите номер месяца от 1 до 12:")
            bot.register_next_step_handler(msg, process_month_step_for_group)
            return
        
        user_states[user_id]['month'] = f"{month:02d}"
        
        msg = bot.send_message(user_id, f"Месяц: {month}\nТеперь введите год (например 2025):")
        bot.register_next_step_handler(msg, process_year_step_for_group)
        
    except ValueError:
        msg = bot.send_message(user_id, "Нужно ввести число от 1 до 12:")
        bot.register_next_step_handler(msg, process_month_step_for_group)

# Обработчик года для группового отчета
def process_year_step_for_group(message):
    user_id = message.chat.id
    
    if user_id not in user_states or not user_states[user_id].get('active'):
        bot.send_message(user_id, "⚠️ Сначала выберите месяц!")
        return process_month_step_for_group(message)
    
    try:
        if message.text.strip().lower() == 'stop':
            handle_stop_button(message)
            return
            
        year = int(message.text)
        
        if year < 2000 or year > 2100:
            msg = bot.send_message(user_id, "Введите год от 2000 до 2100:")
            bot.register_next_step_handler(msg, process_year_step_for_group)
            return
        
        month_str = user_states[user_id]['month']
        group_name = user_states[user_id]['group_name']
        
        # Ваш оригинальный запрос с фильтрацией по группе
        query = """
        SELECT Groupe, Description, Price  
        FROM sav_bot1  
        WHERE strftime('%Y', "Date") = ?
        AND strftime('%m', "Date") = ?
        AND Groupe = ?
        """
        
        result_df = pd.read_sql_query(query, db, params=(str(year), month_str, group_name))

        if not result_df.empty:
            # Добавляем итоговую строку
            total = result_df['Price'].sum()
            result_df = pd.concat([
                result_df,
                pd.DataFrame([['Итого:', '', total]], columns=result_df.columns)
            ])
            
            # Форматируем таблицу
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.axis('off')
            table = ax.table(
                cellText=result_df.values,
                colLabels=result_df.columns,
                cellLoc='center',
                loc='center'
            )
            table.auto_set_font_size(False)
            table.set_fontsize(10)
            
            # Сохраняем и отправляем изображение
            buf = BytesIO()
            plt.savefig(buf, format='png', bbox_inches='tight', dpi=150)
            buf.seek(0)
            plt.close()
            
            bot.send_photo(user_id, buf, 
                         caption=f"Расходы по категории '{group_name}' за {month_str}.{year}")
        else:
            # Дополнительная проверка: возможно данные есть, но не в этой группе
            check_query = """
            SELECT COUNT(*) 
            FROM sav_bot1
            WHERE strftime('%Y', "Date") = ?
            AND strftime('%m', "Date") = ?
            """
            count = pd.read_sql_query(check_query, db, params=(str(year), month_str)).iloc[0,0]
            
            if count > 0:
                bot.send_message(user_id, 
                               f"Данные за {month_str}.{year} есть, но по категории '{group_name}' расходов не найдено")
            else:
                bot.send_message(user_id, f"Нет данных за {month_str}.{year}")
        
        # Очищаем состояние
        if user_id in user_states:
            del user_states[user_id]
            
        # Показываем главное меню
        show_main_menu(user_id)
        
    except ValueError:
        msg = bot.send_message(user_id, "Пожалуйста, введите корректный год:")
        bot.register_next_step_handler(msg, process_year_step_for_group)
    except Exception as e:
        print(f"Ошибка: {e}")
        bot.send_message(user_id, "Произошла ошибка при формировании отчета")
        if user_id in user_states:
            del user_states[user_id]
        show_main_menu(user_id)

# Функция показа главного меню (аналогичная вашей)
def show_main_menu(chat_id):
    markup = types.InlineKeyboardMarkup()
    markup.row(
            types.InlineKeyboardButton('Продукты', callback_data='foot_s2'),
            types.InlineKeyboardButton('Квартира', callback_data='apartment_s2'),
            types.InlineKeyboardButton('Развлечение', callback_data='entertainment_s2')
        )
    markup.row(
            types.InlineKeyboardButton('Дополнительное', callback_data='extra_s2'),
            types.InlineKeyboardButton('Личные траты', callback_data='personal_s2'),
            types.InlineKeyboardButton('Машина', callback_data='car_s2')
        )
    markup.row(
            types.InlineKeyboardButton('Одежда', callback_data='clothes_s2'),
            types.InlineKeyboardButton('Перевод', callback_data='transfer_s2'),
            types.InlineKeyboardButton('Потери', callback_data='losses_s2')
    )
    markup.add(types.InlineKeyboardButton('← Назад', callback_data='expenses'))
    bot.send_message(chat_id, "Выбор категории:", reply_markup=markup)
    
# ===================================================================================================================================================================================================== Обработка "вносиммого числа"
def process_amount_step(message):
    user_id = message.from_user.id
    
    # 1. Проверка состояния должна быть ПЕРВОЙ операцией
    if user_states.get(user_id) == 'idle':
        bot.send_message(user_id, "❌ Текущая операция была отменена. Начните заново.")
        return
    
    # 2. Явная проверка на команду Stop (регистронезависимая)
    if message.text and message.text.strip().lower() == 'stop':
        handle_stop_button(message)
        return
    
    # 3. Основная логика обработки суммы
    try:
        amount = int(message.text)
        user_data[user_id]['amount'] = amount  # Сохраняем сумму
        
        # Сразу регистрируем следующий шаг ДО отправки сообщения
        msg = bot.send_message(user_id, "💬 Введите комментарий:")
        bot.register_next_step_handler(msg, process_comment_step)
        
    except ValueError:
        # При ошибке повторяем запрос с четкой инструкцией
        msg = bot.send_message(user_id, "🔢 Пожалуйста, введите ЦИФРЫ (например: 100):")
        bot.register_next_step_handler(msg, process_amount_step)

def process_comment_step(message):
    user_id = message.from_user.id
    user_data[user_id]['comment'] = message.text
    
    # Получаем дату (используем текущую, так как это обработчик для hands)
    date_info = datetime.datetime.now().strftime("%d.%m.%Y")
    
    # Формируем сообщение с подтверждением в новом формате
    confirmation_message = (
        f"📋 Подтвердите добавление:\n"
        f"📅 Дата: {date_info}\n"
        f"🏷 Группа: {user_data[user_id]['group']}\n"
        f"💰 Сумма: {user_data[user_id]['amount']}\n"
        f"📝 Комментарий: {user_data[user_id]['comment']}"
    )
    
    # Создаем кнопки для подтверждения
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('✅ Добавить в БД', callback_data='add_to_db'))
    markup.add(types.InlineKeyboardButton('↩️ Вернуться к выбору групп', callback_data='hands'))
    
    # Отправляем сообщение с подтверждением
    bot.send_message(message.chat.id, confirmation_message, reply_markup=markup)

def add_to_db(call):
    user_id = call.from_user.id
    
    # Проверяем, есть ли данные для этого пользователя
    if user_id not in user_data:
        bot.send_message(call.message.chat.id, "❌ Ошибка: данные не найдены.")
        return
    
    data = user_data[user_id]
    
    try:
        # Получаем текущую дату (или пользовательскую, если она была указана)
        date_to_use = data.get('date', datetime.datetime.now().strftime("%Y-%m-%d"))

        # Формируем SQL-запрос
        query = "INSERT INTO sav_bot1 (Date, `Groupe`, Price, Description) VALUES (?, ?, ?, ?)"
        values = (date_to_use, data['group'], data['amount'], data['comment'])

        # Выполняем запрос
        c.execute(query, values)
        db.commit()  # Сохраняем изменения в БД

        # Отправляем сообщение об успешном добавлении
        bot.send_message(call.message.chat.id, "✅ Данные успешно добавлены в БД.")

        # Возвращаем пользователя к выбору типа даты
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Текущая дата', callback_data='hands'))
        markup.add(types.InlineKeyboardButton('Любая дата', callback_data='custom_date'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_add_summ1'))

        bot.send_message(call.message.chat.id, "Выберите опцию:", reply_markup=markup)

    except Exception as e:
        # Логируем ошибку
        logger.error(f"Ошибка при добавлении данных: {e}")

        # Отправляем сообщение об ошибке
        bot.send_message(call.message.chat.id, "❌ Ошибка при добавлении данных в БД.")
        get_ph(call.message)
    
    finally:
        # Очищаем данные пользователя
        if user_id in user_data:
            del user_data[user_id]

# ===================================================================================================================================================================================================== Обработка "вносиммого числа на конкретную дату"


def process_custom_date_step(message):
    user_id = message.from_user.id
    
    if user_states.get(user_id) == 'idle':
        bot.send_message(user_id, "❌ Текущая операция была отменена. Начните заново.")
        return
    
    if message.text and message.text.strip().lower() == 'stop':
        handle_stop_button(message)
        return
    
    try:
        day, month, year = map(int, message.text.split('.'))
        # Используем date напрямую — предполагается, что в импортах есть: from datetime import date
        input_date = date(year, month, day)
        user_data[user_id]['date'] = input_date.strftime("%Y-%m-%d")
        
        msg = bot.send_message(user_id, "Введите сумму:")
        bot.register_next_step_handler(msg, process_amount_step)
        
    except (ValueError, AttributeError):
        msg = bot.send_message(user_id, "❌ Неверный формат даты. Введите дату в формате ДД.ММ.ГГГГ (например, 10.06.2025):")
        bot.register_next_step_handler(msg, process_custom_date_step)


def add_to_db(call):
    user_id = call.from_user.id
    
    if user_id not in user_data:
        bot.send_message(call.message.chat.id, "Ошибка: данные не найдены.")
        return
    
    data = user_data[user_id]
    
    try:
        # Используем datetime.now() — предполагается, что в импортах есть: from datetime import datetime
        date_to_use = data.get('date', datetime.now().strftime("%Y-%m-%d"))
        query = "INSERT INTO sav_bot1 (Date, `Groupe`, Price, Description) VALUES (?, ?, ?, ?)"
        values = (date_to_use, data['group'], data['amount'], data['comment'])

        c.execute(query, values)
        db.commit()

        bot.send_message(call.message.chat.id, "✅ Данные успешно добавлены в БД.")

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Текущая дата', callback_data='hands'))
        markup.add(types.InlineKeyboardButton('Любая дата', callback_data='custom_date'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_add_summ1'))

        bot.send_message(call.message.chat.id, "Выберите опцию:", reply_markup=markup)

    except Exception as e:
        logger.error(f"Ошибка при добавлении данных: {e}")
        bot.send_message(call.message.chat.id, "❌ Ошибка при добавлении данных в БД.")
        get_ph(call.message)
    
    finally:
        if user_id in user_data:
            del user_data[user_id]


def process_comment_step(message):
    user_id = message.from_user.id
    user_data[user_id]['comment'] = message.text
    
    date_info = user_data[user_id].get('date', 'Текущая дата')
    if date_info != 'Текущая дата':
        try:
            # Форматируем дату для красивого отображения
            date_obj = datetime.strptime(date_info, "%Y-%m-%d")
            date_info = date_obj.strftime("%d.%m.%Y")
        except:
            pass
    
    confirmation_message = (
        f"📋 Подтвердите добавление:\n"
        f"📅 Дата: {date_info}\n"
        f"🏷 Группа: {user_data[user_id]['group']}\n"
        f"💰 Сумма: {user_data[user_id]['amount']}\n"
        f"📝 Комментарий: {user_data[user_id]['comment']}"
    )
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('✅ Добавить в БД', callback_data='add_to_db'))
    markup.add(types.InlineKeyboardButton('↩️ Вернуться к выбору групп', callback_data='hands'))
    
    bot.send_message(message.chat.id, confirmation_message, reply_markup=markup)

# ===================================================================================================================================================================================================== Удаление данных из бд

def process_id_for_deletion(message):
    if message.text == '/back':
        # Возвращаем в меню выбора способа удаления
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton('Удалить по ID', callback_data='delete_by_id'),
            types.InlineKeyboardButton('Удалить по дате', callback_data='delete_by_date')
        )
        markup.add(types.InlineKeyboardButton('← Назад в меню', callback_data='back_to_menu11'))
        bot.send_message(message.chat.id, "Выберите способ удаления:", reply_markup=markup)
        return
    
    try:
        id_to_delete = int(message.text)
        
        # Проверяем существование записи с таким ID
        c.execute("SELECT COUNT(*) FROM sav_bot1 WHERE ID = ?", (id_to_delete,))
        exists = c.fetchone()[0]
        
        if exists > 0:
            # Удаляем запись
            c.execute("DELETE FROM sav_bot1 WHERE ID = ?", (id_to_delete,))
            db.commit()
            
            markup = types.InlineKeyboardMarkup()
            markup.row(
                types.InlineKeyboardButton('Удалить по ID', callback_data='delete_by_id'),
                types.InlineKeyboardButton('Удалить по дате', callback_data='delete_by_date')
            )
            markup.add(types.InlineKeyboardButton('← Назад в меню', callback_data='back_to_menu11'))
            bot.send_message(message.chat.id, f"Запись с ID {id_to_delete} успешно удалена!", reply_markup=markup)
        else:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton('Попробовать снова', callback_data='delete_by_id'))
            markup.row(
                types.InlineKeyboardButton('Удалить по ID', callback_data='delete_by_id'),
                types.InlineKeyboardButton('Удалить по дате', callback_data='delete_by_date')
            )
            markup.add(types.InlineKeyboardButton('← Назад в меню', callback_data='back_to_menu11'))
            bot.send_message(message.chat.id, f"Записи с ID {id_to_delete} не существует!", reply_markup=markup)
            
    except ValueError:
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Попробовать снова', callback_data='delete_by_id'))
        markup.row(
            types.InlineKeyboardButton('Удалить по ID', callback_data='delete_by_id'),
            types.InlineKeyboardButton('Удалить по дате', callback_data='delete_by_date')
        )
        markup.add(types.InlineKeyboardButton('← Назад в меню', callback_data='back_to_menu11'))
        bot.send_message(message.chat.id, "Пожалуйста, введите корректный ID (целое число)!", reply_markup=markup)

def process_date_for_deletion(message):
    if message.text == '/back':
        # Возвращаем в меню выбора способа удаления
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton('Удалить по ID', callback_data='delete_by_id'),
            types.InlineKeyboardButton('Удалить по дате', callback_data='delete_by_date')
        )
        markup.add(types.InlineKeyboardButton('← Назад в меню', callback_data='back_to_menu11'))
        bot.send_message(message.chat.id, "Выберите способ удаления:", reply_markup=markup)
        return
    
    try:
        # Преобразуем дату из формата ДД.ММ.ГГГГ в ГГГГ-ММ-ДД
        day, month, year = message.text.split('.')
        date_to_delete = f"{year}-{month}-{day}"
        
        # Проверяем существование записей с такой датой
        c.execute("SELECT COUNT(*) FROM sav_bot1 WHERE Date = ?", (date_to_delete,))
        count = c.fetchone()[0]
        
        if count > 0:
            # Удаляем записи
            c.execute("DELETE FROM sav_bot1 WHERE Date = ?", (date_to_delete,))
            db.commit()
            
            markup = types.InlineKeyboardMarkup()
            markup.row(
                types.InlineKeyboardButton('Удалить по ID', callback_data='delete_by_id'),
                types.InlineKeyboardButton('Удалить по дате', callback_data='delete_by_date')
            )
            markup.add(types.InlineKeyboardButton('← Назад в меню', callback_data='back_to_menu11'))
            bot.send_message(message.chat.id, f"Удалено {count} записей за дату {message.text}!", reply_markup=markup)
        else:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton('Попробовать снова', callback_data='delete_by_date'))
            markup.row(
                types.InlineKeyboardButton('Удалить по ID', callback_data='delete_by_id'),
                types.InlineKeyboardButton('Удалить по дате', callback_data='delete_by_date')
            )
            markup.add(types.InlineKeyboardButton('← Назад в меню', callback_data='back_to_menu11'))
            bot.send_message(message.chat.id, f"Записей за дату {message.text} не найдено!", reply_markup=markup)
            
    except Exception as e:
        logger.error(f"Ошибка при удалении по дате: {e}")
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Попробовать снова', callback_data='delete_by_date'))
        markup.row(
            types.InlineKeyboardButton('Удалить по ID', callback_data='delete_by_id'),
            types.InlineKeyboardButton('Удалить по дате', callback_data='delete_by_date')
        )
        markup.add(types.InlineKeyboardButton('← Назад в меню', callback_data='back_to_menu11'))
        bot.send_message(message.chat.id, "Неверный формат даты! Используйте ДД.ММ.ГГГГ (например 10.10.2024)", reply_markup=markup)
        

# =====================================================================================================================================================================================================   Рачет прогноза на 12 месяц фикс
def handle_prog_fiks(call):
    # Фиксируем сумму накоплений
    query = """SELECT SUM(Price) filter(WHERE Groupe IN ('Зп', 'Зп доп', 'Прибыль')) - SUM(Price) filter(WHERE Groupe  not IN ('Зп', 'Зп доп', 'Прибыль')) AS Сумма FROM sav_bot1 """
    result_df = pd.read_sql_query(query, db)
    savings = result_df['Сумма'].iloc[0]

    # Запрашиваем у пользователя данные
    bot.send_message(call.message.chat.id, "Внесите сумму ежемесячной прибыли (Целое число):")
    bot.send_message(call.message.chat.id, "Для возврата назад напишите 'назад'")
    bot.register_next_step_handler(call.message, process_profit_input, savings, call)

def process_profit_input(message, savings, call):
    if message.text.lower() == 'назад':
        handle_prog_fiks(call)
        return
        
    try:
        profit = int(message.text)
        bot.send_message(message.chat.id, "Внесите сумму ежемесячных трат (Целое число):")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_expenses_input, savings, profit, call)
    except ValueError:
        bot.send_message(message.chat.id, "Пожалуйста, введите целое число.")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_profit_input, savings, call)

def process_expenses_input(message, savings, profit, call):
    if message.text.lower() == 'назад':
        bot.send_message(message.chat.id, "Внесите сумму ежемесячной прибыли (Целое число):")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_profit_input, savings, call)
        return
        
    try:
        expenses = int(message.text)
        bot.send_message(message.chat.id, "Учитывать аномалии 0 - нет, 1 - да:")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_anomalies_input, savings, profit, expenses, call)
    except ValueError:
        bot.send_message(message.chat.id, "Пожалуйста, введите целое число.")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_expenses_input, savings, profit, call)

def process_anomalies_input(message, savings, profit, expenses, call):
    if message.text.lower() == 'назад':
        bot.send_message(message.chat.id, "Внесите сумму ежемесячных трат (Целое число):")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_expenses_input, savings, profit, call)
        return
        
    try:
        anomalies_count = int(message.text)
        if anomalies_count == 0:
            calculate_and_plot(savings, profit, expenses, chat_id=message.chat.id)
        else:
            bot.send_message(message.chat.id, "Введите количество месяцев с аномалиями (от 1 до 12):")
            bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
            bot.register_next_step_handler(message, process_anomalies_months_input, savings, profit, expenses, anomalies_count, message.chat.id)
    except ValueError:
        bot.send_message(message.chat.id, "Пожалуйста, введите целое число.")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_anomalies_input, savings, profit, expenses, call)

def process_anomalies_months_input(message, savings, profit, expenses, anomalies_count, chat_id):
    if message.text.lower() == 'назад':
        bot.send_message(message.chat.id, "Учитывать аномалии 0 - нет, 1 - да:")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, lambda m: process_anomalies_input(m, savings, profit, expenses, message))
        return
        
    try:
        anomalies_months = int(message.text)
        if 1 <= anomalies_months <= 12:
            bot.send_message(message.chat.id, "Введите общее количество аномалий:")
            bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
            bot.register_next_step_handler(message, process_total_anomalies_input, savings, profit, expenses, anomalies_count, anomalies_months, chat_id)
        else:
            bot.send_message(message.chat.id, "Пожалуйста, введите число от 1 до 12.")
            bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
            bot.register_next_step_handler(message, process_anomalies_months_input, savings, profit, expenses, anomalies_count, chat_id)
    except ValueError:
        bot.send_message(message.chat.id, "Пожалуйста, введите целое число.")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_anomalies_months_input, savings, profit, expenses, anomalies_count, chat_id)

def process_total_anomalies_input(message, savings, profit, expenses, anomalies_count, anomalies_months, chat_id):
    if message.text.lower() == 'назад':
        bot.send_message(message.chat.id, "Введите количество месяцев с аномалиями (от 1 до 12):")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_anomalies_months_input, savings, profit, expenses, anomalies_count, chat_id)
        return
        
    try:
        total_anomalies = int(message.text)
        bot.send_message(message.chat.id, "Введите минимальную сумму аномалии:")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_min_anomaly_input, savings, profit, expenses, anomalies_count, anomalies_months, total_anomalies, chat_id)
    except ValueError:
        bot.send_message(message.chat.id, "Пожалуйста, введите целое число.")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_total_anomalies_input, savings, profit, expenses, anomalies_count, anomalies_months, chat_id)

def process_min_anomaly_input(message, savings, profit, expenses, anomalies_count, anomalies_months, total_anomalies, chat_id):
    if message.text.lower() == 'назад':
        bot.send_message(message.chat.id, "Введите общее количество аномалий:")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_total_anomalies_input, savings, profit, expenses, anomalies_count, anomalies_months, chat_id)
        return
        
    try:
        min_anomaly = int(message.text)
        bot.send_message(message.chat.id, "Введите максимальную сумму аномалии:")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_max_anomaly_input, savings, profit, expenses, anomalies_count, anomalies_months, total_anomalies, min_anomaly, chat_id)
    except ValueError:
        bot.send_message(message.chat.id, "Пожалуйста, введите целое число.")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_min_anomaly_input, savings, profit, expenses, anomalies_count, anomalies_months, total_anomalies, chat_id)

def process_max_anomaly_input(message, savings, profit, expenses, anomalies_count, anomalies_months, total_anomalies, min_anomaly, chat_id):
    if message.text.lower() == 'назад':
        bot.send_message(message.chat.id, "Введите минимальную сумму аномалии:")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_min_anomaly_input, savings, profit, expenses, anomalies_count, anomalies_months, total_anomalies, chat_id)
        return
        
    try:
        max_anomaly = int(message.text)
        selected_months = random.sample(range(1, 13), anomalies_months)
        anomalies_distribution = distribute_anomalies(total_anomalies, anomalies_months)
        calculate_and_plot(savings, profit, expenses, anomalies_count, selected_months, anomalies_distribution, min_anomaly, max_anomaly, chat_id)
    except ValueError:
        bot.send_message(message.chat.id, "Пожалуйста, введите целое число.")
        bot.send_message(message.chat.id, "Для возврата назад напишите 'назад'")
        bot.register_next_step_handler(message, process_max_anomaly_input, savings, profit, expenses, anomalies_count, anomalies_months, total_anomalies, min_anomaly, chat_id)


# Функция для распределения аномалий по месяцам
def distribute_anomalies(total_anomalies, anomalies_months):
    anomalies_distribution = []
    for _ in range(anomalies_months - 1):
        anomaly = random.randint(1, total_anomalies - (anomalies_months - 1 - _))
        anomalies_distribution.append(anomaly)
        total_anomalies -= anomaly
    anomalies_distribution.append(total_anomalies)
    return anomalies_distribution

# Функция для расчета и построения графика
def format_number(value):
    """
    Форматирует число: если >= 1 млн, отображает в миллионах (M), иначе в тысячах (K).
    """
    if value >= 1e6:
        return f'{value / 1e6:.1f}M'  # В миллионах
    else:
        return f'{value / 1e3:.1f}K'  # В тысячах

def calculate_and_plot(savings, profit, expenses, anomalies_count=0, anomalies_months=None, anomalies_distribution=None, min_anomaly=0, max_anomaly=0, chat_id=None, call=None):
    months = []
    savings_list = []
    profit_list = []
    expenses_list = []
    current_date = datetime.datetime.now()  # Используем datetime.now() правильно

    for month in range(12):
        months.append(current_date.strftime("%b %Y"))  # Формат: "Mar 2025"
        savings_list.append(savings)
        profit_list.append(profit)
        expenses_list.append(expenses)

        # Расчет аномалий
        if anomalies_count > 0 and anomalies_months and month + 1 in anomalies_months:
            anomalies = [random.randint(min_anomaly, max_anomaly) for _ in range(anomalies_distribution[anomalies_months.index(month + 1)])]
            total_anomalies_sum = sum(anomalies)
            savings -= total_anomalies_sum
            expenses_list[-1] += total_anomalies_sum

        # Расчет накоплений
        savings += profit - expenses
        current_date += timedelta(days=30)  # Упрощенное добавление месяца

    # Построение графика
    plt.figure(figsize=(12, 10))  # Увеличиваем размер графика для таблицы

    # Линии с маркерами и подписями
    plt.plot(months, savings_list, marker='o', label='Накопительная сумма', color='blue')
    plt.plot(months, profit_list, marker='s', label='Прибыль', color='green')
    plt.plot(months, expenses_list, marker='^', label='Траты', color='red')

    # Добавляем подписи к точкам
    for i, (sav, prof, exp) in enumerate(zip(savings_list, profit_list, expenses_list)):
        plt.text(i, sav, format_number(sav), ha='center', va='bottom', fontsize=8, color='blue')
        plt.text(i, prof, format_number(prof), ha='center', va='bottom', fontsize=8, color='green')
        plt.text(i, exp, format_number(exp), ha='center', va='top', fontsize=8, color='red')

    # Форматируем ось Y
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: format_number(x)))

    # Настройки графика
    plt.xlabel('Месяц')
    plt.ylabel('Сумма')
    plt.title('Прогноз на 12 месяцев')
    plt.legend(loc='upper left')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(rotation=45)  # Поворачиваем подписи по оси X для удобства чтения

    # Создаем таблицу
    table_data = [
        ['Накопления'] + [format_number(sav) for sav in savings_list],
        ['Прибыль'] + [format_number(prof) for prof in profit_list],
        ['Траты'] + [format_number(exp) for exp in expenses_list]
    ]

    # Добавляем таблицу под графиком
    plt.table(
        cellText=table_data,
        colLabels=['Категория'] + months,  # Заголовки колонок
        cellLoc='center',
        loc='bottom',
        bbox=[0, -0.5, 1, 0.3]  # Расположение таблицы (под графиком)
    )

    # Настраиваем layout, чтобы таблица не перекрывала график
    plt.tight_layout()

    # Сохраняем график
    plt.savefig('forecast.png', bbox_inches='tight')  # Сохраняем с учетом таблицы
    plt.close()

    # Отправка графика пользователю
    if chat_id:
        with open('forecast.png', 'rb') as photo:
            bot.send_photo(chat_id, photo)

        # Отправляем кнопки после отправки графика
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('Прогноз на 12 месяцев (Фик)', callback_data='prog_fiks'))
        markup.add(types.InlineKeyboardButton('Прогноз на 12 месяцев (Дин)', callback_data='prog_dinam'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_add_dop_rs1'))

        # Отправляем сообщение с кнопками
        bot.send_message(chat_id, text='Выберите действие:', reply_markup=markup)
        
# =====================================================================================================================================================================================================  "За конкретный месяц.год"
# Функция для обработки введенного месяца
def process_month_step(message):
    user_id = message.chat.id
    
    # Удаляем проверку на 'cancelled', так как состояние теперь полностью сбрасывается
    
    try:
        # Проверяем, не является ли ввод командой Stop
        if message.text.strip().lower() == 'stop':
            handle_stop_button(message)
            return
            
        month = int(message.text)
        
        if month < 1 or month > 12:
            msg = bot.send_message(user_id, "Введите номер месяца от 1 до 12:")
            bot.register_next_step_handler(msg, process_month_step)
            return
        
        month_str = f"{month:02d}"
        # Создаем новое состояние для пользователя
        user_states[user_id] = {
            'stage': 'waiting_year',
            'month': month_str,
            'active': True  # Флаг активного процесса
        }
        
        msg = bot.send_message(user_id, f"Выбран месяц: {month_str}\nТеперь введите год (1900-2100):")
        bot.register_next_step_handler(msg, process_year_step)
        
    except ValueError:
        msg = bot.send_message(user_id, "Нужно ввести число от 1 до 12:")
        bot.register_next_step_handler(msg, process_month_step)

# Функция обработки года (обновленная)
def process_year_step(message):
    user_id = message.chat.id
    
    # Проверяем наличие активного процесса
    if user_id not in user_states or not user_states[user_id].get('active'):
        bot.send_message(user_id, "⚠️ Сначала выберите месяц!")
        return process_month_step(message)
    
    try:
        if message.text.strip().lower() == 'stop':
            handle_stop_button(message)
            return
            
        year = int(message.text)
        
        if year < 1900 or year > 2100:
            msg = bot.send_message(user_id, "Введите год от 1900 до 2100:")
            bot.register_next_step_handler(msg, process_year_step)
            return
        
        # Получаем сохраненный месяц
        month_str = user_states[user_id]['month']
        execute_sql_query(user_id, month_str, year)
        
        # Полностью очищаем состояние
        if user_id in user_states:
            del user_states[user_id]
            
        bot.send_message(user_id, "✅ Данные успешно сохранены!")
        
    except ValueError:
        msg = bot.send_message(user_id, "Пожалуйста, введите корректный год:")
        bot.register_next_step_handler(msg, process_year_step)


# Функция для выполнения SQL-запроса и отправки результата
def execute_sql_query(chat_id, month_str, year):
    # Первый запрос: проверка наличия данных
    count_query = """
    SELECT COUNT(*)
    FROM sav_bot1
    WHERE Groupe NOT IN ('Зп', 'Зп доп', 'Прибыль')
        AND strftime('%Y', "Date") = ?
        AND strftime('%m', "Date") = ?
    """
    try:
        # Проверяем, есть ли данные
        count_df = pd.read_sql_query(count_query, db, params=(str(year), month_str))
        if count_df.iloc[0, 0] == 0:
            bot.send_message(chat_id, f"Данных за {month_str}.{year} нет, выберите другой период.")
            send_options(chat_id)  # Отправляем кнопки с опциями
            return
        
        # Если данные есть, выполняем второй запрос для формирования сводной таблицы
        summary_query = """
        SELECT
            Groupe,
            SUM(Price) AS Сумма
        FROM sav_bot1
        WHERE Groupe NOT IN ('Зп', 'Зп доп', 'Прибыль')
            AND strftime('%Y', "Date") = ?
            AND strftime('%m', "Date") = ?
        GROUP BY Groupe
        ORDER BY Сумма DESC
        """
        summary_df = pd.read_sql_query(summary_query, db, params=(str(year), month_str))
        
        # Форматируем числа с пробелами между разрядами
        summary_df['Сумма'] = summary_df['Сумма'].apply(lambda x: f"{x:,.2f}".replace(",", " ").replace(".", ","))
        
        # Создаем изображение таблицы
        image_path = create_table_image(summary_df, month_str, year)
        
        # Отправляем изображение пользователю
        with open(image_path, 'rb') as photo:
            bot.send_photo(chat_id, photo, caption=f"📊 Сводная таблица за {month_str}.{year}")
        
        # Отправляем кнопки с опциями
        send_options(chat_id)
    except Exception as e:
        bot.send_message(chat_id, f"Произошла ошибка при выполнении запроса: {e}")

# Функция для создания изображения таблицы
def create_table_image(df, month, year):
    # Создаем фигуру и оси
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.axis('off')  # Скрываем оси
    
    # Создаем таблицу
    table = plt.table(
        cellText=df.values,
        colLabels=df.columns,
        loc='center',
        cellLoc='center',
        colColours=['#f2f2f2'] * len(df.columns)  # Цвет фона заголовков
    )
    
    # Настройка стиля таблицы
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1.2, 1.2)  # Масштабирование таблицы
    
    # Заголовок таблицы
    plt.title(f"Сводная таблица за {month}.{year}", fontsize=14, pad=20)
    
    # Сохраняем изображение
    image_path = f"table_{month}_{year}.png"
    plt.savefig(image_path, bbox_inches='tight', dpi=300)
    plt.close()
    
    return image_path

# Функция для отправки кнопок с опциями
def send_options(chat_id):
        markup = types.InlineKeyboardMarkup()
        markup.row(types.InlineKeyboardButton('За текущий месяц', callback_data='general')
                   ,types.InlineKeyboardButton('Граппа за месяц', callback_data='in_terms_group')
                   ,types.InlineKeyboardButton('1 группа за месяц',callback_data='one_group_month')
                   )
        markup.row(types.InlineKeyboardButton('Текущий, прошлый месяц', callback_data='in_terms_groupe')
                    ,types.InlineKeyboardButton('За 6 месяцев', callback_data='in_six_groupes'))
        markup.row(types.InlineKeyboardButton('За любой месяц.год', callback_data='contr_month_table1')
                   ,types.InlineKeyboardButton('1 группа за любой месяц.год',callback_data='one_contr_group_month'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_buget3'))

# ===================================================================================================================================================================================================== Прогноз динамика "prog_dinam"

def forecast_dinam(call):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('📊 Метод 1: Стандартный', callback_data='forecast_method_1'))
    markup.add(types.InlineKeyboardButton('📉 Метод 2: Консервативный', callback_data='forecast_method_2'))
    markup.add(types.InlineKeyboardButton('📈 Метод 3: Оптимистичный', callback_data='forecast_method_3'))
    markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_add_dop_rs1'))
    bot.edit_message_text(
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        text="Выберите метод прогнозирования:",
        reply_markup=markup
    )

def build_forecast(call, method="method_1"):
    try:
        conn = sqlite3.connect(db_path1)
        cursor = conn.cursor()
        cursor.execute(f"ATTACH DATABASE '{db_path2.replace('\\\\', '/')}' AS db2")

        cursor.execute("SELECT SUM(Plan) as total_plan FROM db2.sav_bot_rs")
        result = cursor.fetchone()
        total_plan_expense = float(result[0]) if result and result[0] is not None else 0.0

        query_fact = "SELECT Date, Groupe, Price FROM sav_bot1"
        df = pd.read_sql_query(query_fact, conn)

        if df.empty:
            bot.send_message(call.message.chat.id, "⚠️ Нет фактических данных для прогноза.")
            cursor.execute("DETACH DATABASE db2")
            conn.close()
            return

        # === Текущий бюджет через SQL ===
        budget_query = """
        SELECT 
            SUM(Price) FILTER(WHERE Groupe IN ('Зп', 'Зп доп', 'Прибыль')) 
            - 
            SUM(Price) FILTER(WHERE Groupe NOT IN ('Зп', 'Зп доп', 'Прибыль')) 
        AS Сумма 
        FROM sav_bot1
        """
        cursor.execute(budget_query)
        budget_result = cursor.fetchone()
        current_budget = float(budget_result[0]) if budget_result and budget_result[0] is not None else 0.0

        # === Прибыль: только зарплаты ===
        profit_groups = ['Зп', 'Зп доп']
        profit_df = df[df['Groupe'].isin(profit_groups)].copy()
        profit_df['Date'] = pd.to_datetime(profit_df['Date'], errors='coerce')
        profit_df = profit_df.dropna(subset=['Date'])
        profit_df['YearMonth'] = profit_df['Date'].dt.to_period('M').astype(str)

        now = datetime.now()
        current_month = now.replace(day=1)
        months = [current_month + timedelta(days=30 * i) for i in range(12)]
        month_labels = [m.strftime('%Y-%m') for m in months]

        # === Расчёт прибыли: Никита — как раньше (по первой цифре), Егор — min из 3 последних месяцев ===
        total_typical_profit = 0.0

        # --- Обработка 'Зп' (без изменений) ---
        nikita_df = profit_df[profit_df['Groupe'] == 'Зп']
        if not nikita_df.empty:
            monthly_nikita = nikita_df.groupby('YearMonth')['Price'].sum().reset_index()
            monthly_sums = monthly_nikita['Price'].tolist()
            def first_digit(n):
                n = abs(int(n))
                while n >= 10:
                    n //= 10
                return n
            first_digits = [first_digit(x) for x in monthly_sums]
            from collections import Counter
            digit_counter = Counter(first_digits)
            max_freq = max(digit_counter.values())
            most_common_digits = [d for d, count in digit_counter.items() if count == max_freq]
            target_digit = min(most_common_digits)
            filtered_sums = [x for x, d in zip(monthly_sums, first_digits) if d == target_digit]
            nikita_forecast = max(filtered_sums) if filtered_sums else max(monthly_sums)
            total_typical_profit += nikita_forecast
        else:
            nikita_forecast = 0.0

        # --- Обработка 'Зп доп' (новая логика: min из 3 последних месяцев по дате) ---
        egor_df = profit_df[profit_df['Groupe'] == 'Зп доп'].copy()
        if not egor_df.empty:
            # Сортируем по дате по убыванию и берём уникальные месяцы (в порядке убывания)
            egor_df = egor_df.sort_values('Date', ascending=False)
            # Группируем по месяцу и суммируем зарплату в месяце (на случай нескольких записей в месяце)
            monthly_egor = egor_df.groupby(egor_df['Date'].dt.to_period('M'))['Price'].sum().reset_index()
            monthly_egor = monthly_egor.sort_values('Date', ascending=False)  # свежие — вверху
            # Берём до 3 последних месяца
            last_3_months_vals = monthly_egor['Price'].head(3).tolist()
            egor_forecast = min(last_3_months_vals)  # гарантированно не пусто, т.к. egor_df не пуст
            total_typical_profit += egor_forecast
        else:
            egor_forecast = 0.0

        # === Расчёт итоговой прибыли с учётом метода ===
        if method == "method_2":
            # Консервативный: -15% от общей суммы (как раньше)
            avg_profit = total_typical_profit * 0.85
        elif method == "method_3":
            # Оптимистичный: только Никита увеличивается на случайные 40–70%, Егор — без изменений
            boost_percent = np.random.uniform(0.40, 0.70)  # от 40% до 70% (не включая 70? — можно randint(40,71)/100)
            # Альтернатива, если хотите строго целые проценты:
            # boost_percent = random.randint(40, 70) / 100.0
            nikita_optimistic = nikita_forecast * (1 + boost_percent)
            avg_profit = nikita_optimistic + egor_forecast
        else:
            # Стандартный: как раньше
            avg_profit = total_typical_profit

        # === МЯГКИЙ РАСЧЁТ АНОМАЛИЙ ===
        expense_df = df[~df['Groupe'].isin(['Зп', 'Зп доп', 'Прибыль'])].copy()
        expense_df['Date'] = pd.to_datetime(expense_df['Date'], errors='coerce')
        expense_df = expense_df.dropna(subset=['Date'])
        expense_df['YearMonth'] = expense_df['Date'].dt.to_period('M').astype(str)

        monthly_expense = expense_df.groupby('YearMonth')['Price'].sum().reset_index()

        if monthly_expense.empty:
            avg_expense_stable = total_plan_expense
            anomaly_values = []
            anomaly_freq = 0
        else:
            monthly_expense['Excess'] = monthly_expense['Price'] - total_plan_expense
            monthly_expense['Is_Anomaly'] = monthly_expense['Excess'] > 0
            anomaly_rows = monthly_expense[monthly_expense['Is_Anomaly']]

            if not anomaly_rows.empty:
                base_anomaly = anomaly_rows['Excess'].median()  # ← МЕДИАНА вместо случайного
                anomaly_freq = (len(anomaly_rows) / len(monthly_expense)) * 0.6  # ← 60% частоты
                anomaly_freq = min(anomaly_freq, 0.5)  # ← максимум 50%
                anomaly_values = [base_anomaly]
            else:
                anomaly_values = []
                anomaly_freq = 0

            avg_expense_stable = total_plan_expense

        # === Коррекция прибыли на текущий месяц: вычитаем уже полученные Зп за этот месяц ===
        current_year_month = now.strftime('%Y-%m')
        # Фильтруем только 'Зп' и 'Зп доп' за текущий месяц
        current_month_profit_fact = df[
            (df['Groupe'].isin(['Зп', 'Зп доп'])) &
            (pd.to_datetime(df['Date']).dt.to_period('M').astype(str) == current_year_month)
        ]['Price'].sum()
        # current_month_profit_fact — уже получено в этом месяце (может быть 0)


        # === Прогноз на 12 месяцев ===
        forecast_data = []
        balance_stable = current_budget
        balance_with_anomaly = current_budget

        for i in range(12):
            has_anomaly = np.random.rand() < anomaly_freq
            anomaly_amount = anomaly_values[0] if has_anomaly and anomaly_values else 0

                        # Для первого месяца (текущего) — вычитаем уже полученную прибыль
            if i == 0:
                profit_val = max(0.0, avg_profit - current_month_profit_fact)
            else:
                profit_val = avg_profit
            expense_stable_val = avg_expense_stable
            expense_total_val = expense_stable_val + anomaly_amount

            balance_stable += profit_val - expense_stable_val
            balance_with_anomaly += profit_val - expense_total_val

            forecast_data.append({
                'Месяц': month_labels[i],
                'Прибыль': round(profit_val, 2),
                'Траты (стаб.)': round(expense_stable_val, 2),
                'Аномалии': round(anomaly_amount, 2),
                'Банк (без аном.)': round(balance_stable, 2),
                'Банк (с аном.)': round(balance_with_anomaly, 2)
            })

        forecast_df = pd.DataFrame(forecast_data)

        cursor.execute("DETACH DATABASE db2")
        conn.close()

        # === Визуализация ===
        plt.rcParams['font.size'] = 9
        fig, ax = plt.subplots(figsize=(12, 7))
        ax.axis('tight')
        ax.axis('off')
        table = ax.table(
            cellText=forecast_df.values,
            colLabels=forecast_df.columns,
            cellLoc='center',
            loc='center',
            colColours=["#e0e0e0"] * len(forecast_df.columns)
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1.1, 2.0)

        method_names = {
            "method_1": "Стандартный",
            "method_2": "Консервативный",
            "method_3": "Оптимистичный"
        }
        subtitle = f"План трат: {total_plan_expense:.0f} ₽ | Аномалий: {len(anomaly_values)} | Вероятность: {anomaly_freq:.0%}"
        plt.title(f"📈 Прогноз: {method_names.get(method)}\n{subtitle}\n(с {month_labels[0]})", 
                  fontweight='bold', fontsize=11, pad=30, linespacing=1.5)

        img_path = f"forecast_{call.message.chat.id}_{method}.png"
        plt.savefig(img_path, bbox_inches='tight', dpi=120, facecolor='white')
        plt.close()

        with open(img_path, 'rb') as photo:
            caption = f"📊 Прогноз по методу: {method_names.get(method)}\nПлан трат: {total_plan_expense:.0f} ₽"
            bot.send_photo(call.message.chat.id, photo, caption=caption)

        try:
            os.remove(img_path)
        except:
            pass

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton(f'🔄 Пересчитать ({method_names.get(method)})', callback_data=f'forecast_method_{method.split("_")[-1]}'))
        markup.add(types.InlineKeyboardButton('📊 Выбрать метод', callback_data='prog_dinam'))
        markup.add(types.InlineKeyboardButton('← Назад', callback_data='back_add_dop_rs1'))

        bot.send_message(call.message.chat.id, "Что дальше?", reply_markup=markup)

    except Exception as e:
        bot.send_message(call.message.chat.id, f"⚠️ Ошибка при построении прогноза: {str(e)}")
        try:
            cursor.execute("DETACH DATABASE db2")
            conn.close()
        except:
            pass

























# =====================================================================================================================================================================================================  Запуск бота

# Настройка логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# Глобальные переменные для управления перезапусками
GLOBAL_RETRY_COUNT = 0
LAST_SUCCESSFUL_START = None
MAX_RETRIES = 15
RESET_HOURS = 2  # Через сколько часов обнулять счетчик попыток

def send_alert_to_admin(error, retry_count):
    """Упрощенная отправка уведомлений (без проверки чата)"""
    try:
        admin_id = 123456789  # Оставьте 0 если уведомления не нужны
        if admin_id == 0:
            return
            
        message = (f"🚨 Бот упал с ошибкой (попытка {retry_count}/{MAX_RETRIES}):\n"
                  f"{str(error)[:1000]}\n"
                  f"Последний успешный запуск: {LAST_SUCCESSFUL_START}")
        bot.send_message(admin_id, message)
    except Exception:
        pass  # Полностью игнорируем ошибки отправки

def should_reset_retries():
    """Проверяет, нужно ли обнулить счетчик попыток"""
    if LAST_SUCCESSFUL_START is None:
        return False
    
    time_diff = dt.datetime.now() - LAST_SUCCESSFUL_START
    return time_diff.total_seconds() >= RESET_HOURS * 3600

def run_bot():
    """Основная функция запуска бота"""
    global GLOBAL_RETRY_COUNT, LAST_SUCCESSFUL_START
    
    try:
        logger.info(f"🟢 Запуск бота (попытка {GLOBAL_RETRY_COUNT + 1}/{MAX_RETRIES}) {dt.datetime.now()}")
        
        # Обновляем время последнего успешного запуска
        LAST_SUCCESSFUL_START = dt.datetime.now()
        
        # Запускаем бота с увеличенными таймаутами
        bot.polling(
            none_stop=True,
            timeout=90,
            long_polling_timeout=90
        )
        
        # Если polling завершился без ошибок (редкий случай)
        logger.info("Polling завершился, перезапуск...")
        time.sleep(15)
        
    except RequestException as e:
        logger.error(f"🔴 Ошибка соединения: {str(e)[:500]}")
        raise

def manage_bot_lifecycle():
    """Управляет жизненным циклом бота"""
    global GLOBAL_RETRY_COUNT
    
    while True:
        try:
            if should_reset_retries():
                logger.info(f"🔄 Обнуление счетчика попыток после {RESET_HOURS} часов работы")
                GLOBAL_RETRY_COUNT = 0
            
            if GLOBAL_RETRY_COUNT >= MAX_RETRIES:
                logger.critical(f"💤 Бот достиг максимума попыток ({MAX_RETRIES}). Спящий режим на 1 час.")
                time.sleep(3600)
                GLOBAL_RETRY_COUNT = 0
                continue
                
            run_bot()
            GLOBAL_RETRY_COUNT = 0
            
        except RequestException as e:
            GLOBAL_RETRY_COUNT += 1
            logger.error(f"🔴 Попытка {GLOBAL_RETRY_COUNT}/{MAX_RETRIES} не удалась")
            
            delay = min(60 * (2 ** (GLOBAL_RETRY_COUNT - 1)), 300)
            logger.info(f"⏳ Повторная попытка через {delay} секунд...")
            time.sleep(delay)
            
        except Exception as e:
            logger.critical(f"💀 Критическая ошибка: {str(e)[:500]}")
            send_alert_to_admin(e, GLOBAL_RETRY_COUNT)
            
            try:
                db.close()
            except:
                pass
                
            time.sleep(300)
            GLOBAL_RETRY_COUNT += 1

if __name__ == "__main__":
    manage_bot_lifecycle()


# daily	23:50	python3.8 /home/Telebotfinanc12trev/botfin/Progect_bot.py	Telegram bot with auto-restart	Expired on 2025-05-08	
# daily	20:40	while true; do python3.8 /home/Telebotfinanc12trev/botfin/Progect_bot.py; sleep 60; done	Telegram bot with auto-restart	2025-08-31










































































