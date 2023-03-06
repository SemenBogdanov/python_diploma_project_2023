#!/usr/bin/python
# coding: cp1251
# Импортируем необходимый пул библиотек

import re
import sys

import tabula
import requests
from bs4 import BeautifulSoup
from datetime import datetime as dt
import pandas
from key import host, port, username, password
import psycopg2
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib as mpl




#  -----------------Глобальные переменные-----------------

# URL - первоначальная ссылка для парсинга
URL = 'https://www.economy.gov.ru/material/directions/makroec/ekonomicheskie_obzory/'

# HEADERS - словарь требуется для обхода системы блокировки при обращении к серверу
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 '
                         '(KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

# LINK_DATES - объявление массива, где будут храниться все ссылки на страницы с файлами
LINK_DATES = {}


# -----------------Функции обработки-----------------

# Функция которая возвращает ссылку на файл pdf.
# Входной параметр - ссылка на страницу с файлом
# Выходной параметр - ссылка на файл pdf
def get_pdf_link_file(link):
    result = requests.get(link, headers=HEADERS)
    html = result.content.decode()
    soup = BeautifulSoup(html, 'html.parser')
    a = [link for link in soup.find_all('a', class_="e-file")][0]
    link = 'https://www.economy.gov.ru' + a['href']
    return link


# Функция которая переводит текстовую дату в datetime.
# Входной параметр - строка текста ссылки страницы документа
# Выходной параметр - объект класса datetime
#
# Действие:
# 1. Первая строка обрезается до 39 символа, оставляя подстроку " 21 декабря 2022"
# 2. Формируется помесячный словарь.
# 3. Путем конкатенации и использования срезом реализуется перевод текста даты в datetime
def string_to_date(text):
    text = text[39:].replace(' года', '')  # " 21 декабря 2022"
    # print(text.strip(), '.')

    months = {"января": "01", "февраля": "02", "марта": "03", "апреля": "04", "мая": "05", "июня": "06",
              "июля": "07",
              "августа": "08", "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12"}
    return dt.strptime(text[0:text.find(" ", 0)] + '.' \
                       + [v for k, v in months.items() if
                          k == text[text.find(" ", 0) + 1:text.find(" ", text.find(" ", 0) + 1)]][0] \
                       + '.' + text.strip()[-4:], '%d.%m.%Y')


# -----------------Общая функция MAIN() -----------------

def main(choice: int):
    ans = 0
    link = ''
    # -----------------Этап 1: Парсинг первоначальной страницы-----------------
    # Отправляем запрос передавая заголовки и получаем ответ. Декодируем ответ, т.к. он нечитабелен
    result = requests.get(URL, headers=HEADERS)
    html = result.content.decode()

    # Создаём "суп", чтоб удобно получить ссылки
    soup = BeautifulSoup(html, 'html.parser')

    # Для каждой ссылки в "супе", через метод find_all класса soup, ищем все ссылки(href), где встречается текст
    # 'o_tekushchey_cenovoy'.
    #
    # Переменная i содержит:
    # <a href="/material/directions/makroec/ekonomicheskie_obzory/o_tekushchey_cenovoy_situacii_10_fevralya_
    # 2023_goda.html" title="О текущей ценовой ситуации. 10 февраля 2023 года"> О текущей ценовой ситуации.
    # 10 февраля 2023 года</a>
    # В цикле for идет перебор всех ссылок и формируется словать LINK_DATES, где ключ - дата из i.text пропущенная через
    # функцию string_to_date(), а значение - часть ссылки на страницу с нужным файлом
    #
    # Значение ключа получается с помощью среза (:). Чтоб срезать href=" прибавляем 6 символов, и чтоб оставить .html
    # прибавляем 5 символов, т.к. конец среза не включается в получаемое значение)
    # print("Пока что всё хорошо1")
    # print(soup)
    for i in soup.find_all(href=re.compile('o_tekushchey_cenovoy')):
        # print(i)
        LINK_DATES[string_to_date(i.text)] = str(i)[str(i).find('''href="''') + 6:str(i).find('.html') + 5]
    # print("97 - ок!")
    # Конкатенация для получения полной ссылки
    # sorted() - сортирует данные словаря по ключу (.. а там дата), параметр reverse делает список по убыванию
    # [0] - получаем первый (верхний) ключ и подставляем его в Link_dates
    # В результате манипуляций получаем самую свежую (максимальную) ссылку

    if choice == 1:
        link = 'https://www.economy.gov.ru' + LINK_DATES[sorted(LINK_DATES, reverse=True)[0]]
        print(f"Последняя полученная ссылка: \n{link}")
    elif choice == 2:
        for k in list(enumerate(LINK_DATES)):
            print(str(k[0]) + ': ' + dt.strftime(k[1], '%d.%m.%Y'))

        ans = int(input('''Выберите файл для парсинга по номеру! \n->> '''))
        # print(LINK_DATES)

        link = 'https://www.economy.gov.ru' + LINK_DATES[sorted(LINK_DATES, reverse=True)[ans]]
        print(f"Полученная ссылка: \n{link}")
        # input('..')

    # Получаем ссылку на pdf-файл
    link_pdf = get_pdf_link_file(link)
    print(f"Ссылка на свежий PDF-файл: \n{link_pdf}")

    # Переходим по полученной ссылке и сохраняем pdf-файл
    response = requests.get(link_pdf, headers=HEADERS, stream=True)
    with open("tempfile.pdf", "wb") as f:
        f.write(response.content)

    try:
        # -----------------Этап 2: Работа с PDF-файлом-----------------
        # Формируем tabula-датафрейм (библиотека, которая переводит изображение в текст)
        dfs = tabula.read_pdf('tempfile.pdf', pages='all', lattice=True, pandas_options={'header': None})

        # т.к. в файле может быть несколько таблиц, все они будут отдельными tabula-датафреймами.
        # Нужная таблица имеет характерный признак: в ней больше всего строк, чем в любой другой таблице документоа.
        # Посчитаем строки, для поиска нужной таблицы, для этого сформируем лист, к которому далее применим ф-цию max()
        mx = []
        for i in dfs:
            mx.append(len(i))

        # создаем объект pandas из данных объекта tabula с максимальным количеством строк
        df = pandas.DataFrame(dfs[mx.index(max(mx))])

        # Сохранение файла для анализа и формирование нового датафрейма pandas
        df.to_csv('to_exc.csv')  # C

        # Заново прочтем сохраненный файл, для того, чтоб сразу установить header, если мы его не установим,
        # то заголовки будут Unnamed:1
        df = pandas.read_csv('to_exc.csv', header=[0], index_col=[0])

        # Создаем новый датафрейм с нужными товарами
        ddf = df[(df['1'] == 'Баранина') | (df['1'] == 'Куры') | (df['1'] == 'Свинина')]
        ddf = ddf[ddf.columns[[1, 3]]].reset_index(drop=True)

        if not ddf.empty:
            ddf.rename(columns={'1': 'product', '3': 'value'}, inplace=True)
        elif ddf.empty:
            ddf = df[(df['0'] == 'Баранина') | (df['0'] == 'Куры') | (df['0'] == 'Свинина')]
            ddf = ddf[ddf.columns[[0, 1]]].reset_index(drop=True)
            ddf.rename(columns={'0': 'product', '1': 'value'}, inplace=True)

        conn_str = f'postgresql://{username}:{password}@{host}:{port}/firstbase1'
        engine = create_engine(conn_str)
        ddf.rename(columns={'1': 'product', '3': 'value'}, inplace=True)
        if choice == 1:
            ddf['datetime'] = sorted(LINK_DATES, reverse=True)[0]
        elif choice == 2:
            ddf['datetime'] = sorted(LINK_DATES, reverse=True)[ans]
        if ddf.empty:
            print('Внимание! Датафрейм пуст, скорее всего в файле нет данных по индексам!')
            print('Необходимо ручное выполнение операции с парсингом предыдущего файла!')
            print('')
            menu()

        else:
            print('Попытка добавления данных датафрейма в базу данных...')
            try:
                ddf.to_sql('pg_learning_prod_index', engine, if_exists='append', index=False, schema='dal_data')
                print('Успешно. Ошибок нет. Проверье таблицу базы данных')
                print('')
                print('Желаете еще что-то сделать?')
                menu()
            except Exception as e:
                print('Ошибка добавления данных в базу данных. Подробности:', e)
                menu()

    except Exception as e:
        print('Ошибка!', e)
        menu()


# Функция для меню

def get_data_from_db():
    try:
        with psycopg2.connect(host=host, port=port, database='firstbase1', user=username,
                              password=password) as conn:
            cursor = conn.cursor()
            sql = '''select 
                        product,
                        (replace(value, ',', '.')::double precision) + 100 as val,
                        datetime::date
                    from dal_data.pg_learning_prod_index
                    order by datetime::date asc'''
            cursor.execute(sql)
            return cursor.fetchall()
    except Exception as e:
        print(f'Невозможно получить данные из базы данных: {e}')
        menu()


def chart():

    print("3 - Построить график по данным из базы данных")

    data = get_data_from_db()
    # print(data)
    meat_labels = ['Баранина', 'Куры', 'Свинина']
    dict_meat = {}
    itog = []
    costs = []
    dates = []

    for label in meat_labels:
        for i in data:
            if i[0] == label:
                costs.append(i[1])
                dates.append(i[2])

        dict_meat[label] = [costs, dates]
        costs = []
        dates = []

    # print(dict_meat)
    # print(dict_meat['Баранина'][1])
    print('Попытка построить график')
    mpl.use('TkAgg')  # !IMPORTANT
    # print(plt.subplots())
    fig, ax1 = plt.subplots()
    color = 'tab:red'
    ax1.set_xlabel('Дата')
    # Rotating X-axis labels
    for tick in ax1.get_xticklabels():
        tick.set_rotation(75)
    ax1.set_ylabel('Значение индекса')
    print('Попытка построить график')
    ax1.plot(dict_meat['Баранина'][1], dict_meat['Баранина'][0], color='r', label='Баранина')
    # ax1.tick_params(axis='y', labelcolor=color)
    # Fixing random state for reproducibility
    # plt.plot(dates, costs, color='r', label='Баранина')
    # plt.plot(dates, costs, color='g', label='Свинина')
    # plt.plot(dict_meat['Баранина'][1], dict_meat['Баранина'][0], color='r', label='Баранина')
    ax2 = ax1.twinx()
    color = 'tab:green'
    ax2.set_ylabel('Значение индекса\n по дополнительной оси')
    ax2.plot(dict_meat['Свинина'][1], dict_meat['Свинина'][0], color='g', label='Свинина')
    ax2.plot(dict_meat['Куры'][1], dict_meat['Куры'][0], color='b', label='Куры')
    # Naming the x-axis, y-axis and the whole graph3
    # plt.xlabel("Дата")
    # plt.ylabel("Индекс цены")
    plt.title("Динамика изменения индексов цен на мясо")
    # plt.xticks(rotation=90)
    plt.subplots_adjust(bottom=0.3, right=0.85)

    # Adding legend, which helps us recognize the curve according to it's color
    ax1.legend(loc=(0.72, 0.70))
    ax2.legend(loc=1)
    plt.grid()
    # plt.axis .tick_params(axis='x', which='major', pad=15)
    # To load the display window
    plt.show()
    # axs[0].plot(t, s1, t, s2)
    # axs[0].set_xlim(0, 2)
    # axs[0].set_xlabel('Time')
    # axs[0].set_ylabel('s1 and s2')
    # axs[0].grid(True)
    #
    # cxy, f = axs[1].cohere(s1, s2, 256, 1. / dt)
    # axs[1].set_ylabel('Coherence')
    #
    # fig.tight_layout()
    # plt.show()
    menu()


def menu():
    try:
        choice = int(input('''
        Добро пожаловать!

        Выберите действие, указав номер опции:

        1 - Парсинг последнего свежего файла
        2 - Парсинг файла с сайта по выбору пользователя
        3 - Построить график по данным из базы данных
        4 - Выход

        >> '''))
        if choice == 1:
            print("Выбрано: 1 - Парсинг последнего свежего файла")
            main(1)

        elif choice == 2:
            print("2 - Парсинг файла с сайта по выбору пользователя")
            main(2)
        elif choice == 3:
            chart()
        elif choice == 4:
            sys.exit()
        else:
            print('Не удалось распознать выбор!')
            menu()
    except Exception as e:
        print('Не удалось распознать выбор! Подробности: ', e)
        menu()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    menu()
