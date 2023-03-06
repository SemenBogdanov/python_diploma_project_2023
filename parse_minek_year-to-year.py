#!/usr/bin/python
# coding: cp1251
# ����������� ����������� ��� ���������

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




#  -----------------���������� ����������-----------------

# URL - �������������� ������ ��� ��������
URL = 'https://www.economy.gov.ru/material/directions/makroec/ekonomicheskie_obzory/'

# HEADERS - ������� ��������� ��� ������ ������� ���������� ��� ��������� � �������
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 '
                         '(KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

# LINK_DATES - ���������� �������, ��� ����� ��������� ��� ������ �� �������� � �������
LINK_DATES = {}


# -----------------������� ���������-----------------

# ������� ������� ���������� ������ �� ���� pdf.
# ������� �������� - ������ �� �������� � ������
# �������� �������� - ������ �� ���� pdf
def get_pdf_link_file(link):
    result = requests.get(link, headers=HEADERS)
    html = result.content.decode()
    soup = BeautifulSoup(html, 'html.parser')
    a = [link for link in soup.find_all('a', class_="e-file")][0]
    link = 'https://www.economy.gov.ru' + a['href']
    return link


# ������� ������� ��������� ��������� ���� � datetime.
# ������� �������� - ������ ������ ������ �������� ���������
# �������� �������� - ������ ������ datetime
#
# ��������:
# 1. ������ ������ ���������� �� 39 �������, �������� ��������� " 21 ������� 2022"
# 2. ����������� ���������� �������.
# 3. ����� ������������ � ������������� ������ ����������� ������� ������ ���� � datetime
def string_to_date(text):
    text = text[39:].replace(' ����', '')  # " 21 ������� 2022"
    # print(text.strip(), '.')

    months = {"������": "01", "�������": "02", "�����": "03", "������": "04", "���": "05", "����": "06",
              "����": "07",
              "�������": "08", "��������": "09", "�������": "10", "������": "11", "�������": "12"}
    return dt.strptime(text[0:text.find(" ", 0)] + '.' \
                       + [v for k, v in months.items() if
                          k == text[text.find(" ", 0) + 1:text.find(" ", text.find(" ", 0) + 1)]][0] \
                       + '.' + text.strip()[-4:], '%d.%m.%Y')


# -----------------����� ������� MAIN() -----------------

def main(choice: int):
    ans = 0
    link = ''
    # -----------------���� 1: ������� �������������� ��������-----------------
    # ���������� ������ ��������� ��������� � �������� �����. ���������� �����, �.�. �� �����������
    result = requests.get(URL, headers=HEADERS)
    html = result.content.decode()

    # ������ "���", ���� ������ �������� ������
    soup = BeautifulSoup(html, 'html.parser')

    # ��� ������ ������ � "����", ����� ����� find_all ������ soup, ���� ��� ������(href), ��� ����������� �����
    # 'o_tekushchey_cenovoy'.
    #
    # ���������� i ��������:
    # <a href="/material/directions/makroec/ekonomicheskie_obzory/o_tekushchey_cenovoy_situacii_10_fevralya_
    # 2023_goda.html" title="� ������� ������� ��������. 10 ������� 2023 ����"> � ������� ������� ��������.
    # 10 ������� 2023 ����</a>
    # � ����� for ���� ������� ���� ������ � ����������� ������� LINK_DATES, ��� ���� - ���� �� i.text ����������� �����
    # ������� string_to_date(), � �������� - ����� ������ �� �������� � ������ ������
    #
    # �������� ����� ���������� � ������� ����� (:). ���� ������� href=" ���������� 6 ��������, � ���� �������� .html
    # ���������� 5 ��������, �.�. ����� ����� �� ���������� � ���������� ��������)
    # print("���� ��� �� ������1")
    # print(soup)
    for i in soup.find_all(href=re.compile('o_tekushchey_cenovoy')):
        # print(i)
        LINK_DATES[string_to_date(i.text)] = str(i)[str(i).find('''href="''') + 6:str(i).find('.html') + 5]
    # print("97 - ��!")
    # ������������ ��� ��������� ������ ������
    # sorted() - ��������� ������ ������� �� ����� (.. � ��� ����), �������� reverse ������ ������ �� ��������
    # [0] - �������� ������ (�������) ���� � ����������� ��� � Link_dates
    # � ���������� ����������� �������� ����� ������ (������������) ������

    if choice == 1:
        link = 'https://www.economy.gov.ru' + LINK_DATES[sorted(LINK_DATES, reverse=True)[0]]
        print(f"��������� ���������� ������: \n{link}")
    elif choice == 2:
        for k in list(enumerate(LINK_DATES)):
            print(str(k[0]) + ': ' + dt.strftime(k[1], '%d.%m.%Y'))

        ans = int(input('''�������� ���� ��� �������� �� ������! \n->> '''))
        # print(LINK_DATES)

        link = 'https://www.economy.gov.ru' + LINK_DATES[sorted(LINK_DATES, reverse=True)[ans]]
        print(f"���������� ������: \n{link}")
        # input('..')

    # �������� ������ �� pdf-����
    link_pdf = get_pdf_link_file(link)
    print(f"������ �� ������ PDF-����: \n{link_pdf}")

    # ��������� �� ���������� ������ � ��������� pdf-����
    response = requests.get(link_pdf, headers=HEADERS, stream=True)
    with open("tempfile.pdf", "wb") as f:
        f.write(response.content)

    try:
        # -----------------���� 2: ������ � PDF-������-----------------
        # ��������� tabula-��������� (����������, ������� ��������� ����������� � �����)
        dfs = tabula.read_pdf('tempfile.pdf', pages='all', lattice=True, pandas_options={'header': None})

        # �.�. � ����� ����� ���� ��������� ������, ��� ��� ����� ���������� tabula-������������.
        # ������ ������� ����� ����������� �������: � ��� ������ ����� �����, ��� � ����� ������ ������� ����������.
        # ��������� ������, ��� ������ ������ �������, ��� ����� ���������� ����, � �������� ����� �������� �-��� max()
        mx = []
        for i in dfs:
            mx.append(len(i))

        # ������� ������ pandas �� ������ ������� tabula � ������������ ����������� �����
        df = pandas.DataFrame(dfs[mx.index(max(mx))])

        # ���������� ����� ��� ������� � ������������ ������ ���������� pandas
        df.to_csv('to_exc.csv')  # C

        # ������ ������� ����������� ����, ��� ����, ���� ����� ���������� header, ���� �� ��� �� ���������,
        # �� ��������� ����� Unnamed:1
        df = pandas.read_csv('to_exc.csv', header=[0], index_col=[0])

        # ������� ����� ��������� � ������� ��������
        ddf = df[(df['1'] == '��������') | (df['1'] == '����') | (df['1'] == '�������')]
        ddf = ddf[ddf.columns[[1, 3]]].reset_index(drop=True)

        if not ddf.empty:
            ddf.rename(columns={'1': 'product', '3': 'value'}, inplace=True)
        elif ddf.empty:
            ddf = df[(df['0'] == '��������') | (df['0'] == '����') | (df['0'] == '�������')]
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
            print('��������! ��������� ����, ������ ����� � ����� ��� ������ �� ��������!')
            print('���������� ������ ���������� �������� � ��������� ����������� �����!')
            print('')
            menu()

        else:
            print('������� ���������� ������ ���������� � ���� ������...')
            try:
                ddf.to_sql('pg_learning_prod_index', engine, if_exists='append', index=False, schema='dal_data')
                print('�������. ������ ���. �������� ������� ���� ������')
                print('')
                print('������� ��� ���-�� �������?')
                menu()
            except Exception as e:
                print('������ ���������� ������ � ���� ������. �����������:', e)
                menu()

    except Exception as e:
        print('������!', e)
        menu()


# ������� ��� ����

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
        print(f'���������� �������� ������ �� ���� ������: {e}')
        menu()


def chart():

    print("3 - ��������� ������ �� ������ �� ���� ������")

    data = get_data_from_db()
    # print(data)
    meat_labels = ['��������', '����', '�������']
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
    # print(dict_meat['��������'][1])
    print('������� ��������� ������')
    mpl.use('TkAgg')  # !IMPORTANT
    # print(plt.subplots())
    fig, ax1 = plt.subplots()
    color = 'tab:red'
    ax1.set_xlabel('����')
    # Rotating X-axis labels
    for tick in ax1.get_xticklabels():
        tick.set_rotation(75)
    ax1.set_ylabel('�������� �������')
    print('������� ��������� ������')
    ax1.plot(dict_meat['��������'][1], dict_meat['��������'][0], color='r', label='��������')
    # ax1.tick_params(axis='y', labelcolor=color)
    # Fixing random state for reproducibility
    # plt.plot(dates, costs, color='r', label='��������')
    # plt.plot(dates, costs, color='g', label='�������')
    # plt.plot(dict_meat['��������'][1], dict_meat['��������'][0], color='r', label='��������')
    ax2 = ax1.twinx()
    color = 'tab:green'
    ax2.set_ylabel('�������� �������\n �� �������������� ���')
    ax2.plot(dict_meat['�������'][1], dict_meat['�������'][0], color='g', label='�������')
    ax2.plot(dict_meat['����'][1], dict_meat['����'][0], color='b', label='����')
    # Naming the x-axis, y-axis and the whole graph3
    # plt.xlabel("����")
    # plt.ylabel("������ ����")
    plt.title("�������� ��������� �������� ��� �� ����")
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
        ����� ����������!

        �������� ��������, ������ ����� �����:

        1 - ������� ���������� ������� �����
        2 - ������� ����� � ����� �� ������ ������������
        3 - ��������� ������ �� ������ �� ���� ������
        4 - �����

        >> '''))
        if choice == 1:
            print("�������: 1 - ������� ���������� ������� �����")
            main(1)

        elif choice == 2:
            print("2 - ������� ����� � ����� �� ������ ������������")
            main(2)
        elif choice == 3:
            chart()
        elif choice == 4:
            sys.exit()
        else:
            print('�� ������� ���������� �����!')
            menu()
    except Exception as e:
        print('�� ������� ���������� �����! �����������: ', e)
        menu()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    menu()
