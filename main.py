"""
Параметры запуска скрипта:
-h, --help: подсказка с описанием аргументов запуска
-с, --clear:  флаг предварительной очистки таблицы БД;
-w, --winner: флаг для записи только победителей GGA;
-y, --year: год проведения церемонии GGA в формате YYYY

Пример: python main.py -с --year 1995 -w

Для чтения и подготовки данных применена pandas, хотя возможны варианты
без сторонних библиотек и модулей (csv, например).
Для случае работы с большим исходным файлом предусмотрено чтение частями:
разбиение проводится по указанному количеству строк.

Исключение вставки дубликатов реализовано через уникальное ограничение таблицы
с условием ON CONFLICT IGNORE: исходная запись не затрагивается, дубликаты
игнорируются.
"""

import sys
import warnings
import sqlite3
import pandas as pd
import sqlite3 as db
import logging
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--clear', action='store_const', const=True,
                    help='Флаг предварительной очистки таблицы БД')
parser.add_argument('-w', '--winner', action='store_const', const=True,
                    help='Флаг для записи только победителей GGA')
parser.add_argument('-y', '--year', default=None,
                    help='Год проведения церемонии GGA')
args = parser.parse_args()
year = int(args.year) if args.year else None
sys.tracebacklimit = -1
warnings.filterwarnings("ignore")


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(message)s',
    datefmt='%d/%m/%Y %I:%M:%S'
)


class DBConnection:
    """
    Дескриптор атрибута conn - строки подключения к БД
    """
    def __get__(self, obj, objtype=None):
        return self.conn

    def __set__(self, obj, conn):
        try:
            conn = db.connect(conn, uri=True)
        except db.DatabaseError:
            logging.error("Ошибка подключения к базе данных")
            raise
        self.conn = conn


def db_validator(f):
    def wrapper(*args, **kwargs):
        try:
            f(*args, **kwargs)
        except sqlite3.IntegrityError:
            logging.error(f"====== Обнаружены дубликаты. "
                          f"Запись фрагмента не выполнена")
        except sqlite3.OperationalError as e:
            logging.error(f"====== Ошибка базы данных: {e}")
        else:
            logging.info("====== Работа с фрагментом завершена")
    return wrapper


class DB:
    """
    Класс для работы с базой данных
    """
    conn = DBConnection()

    def __init__(self, conn):
        self.conn = conn

    @db_validator
    def write_data(self, df_chunk, year_award=None, winners_only=False):
        with self.conn:
            if winners_only:
                df_chunk = df_chunk[df_chunk["win"]]
            if year_award:
                df_chunk = df_chunk[df_chunk["year_award"] == year_award]
            if not df_chunk.empty:

                # Значения null заменяем на NaN для корректной работы constraint
                df_chunk.fillna(0, inplace=True)
                print(df_chunk)
                df_chunk.to_sql(
                    name="gg_awards",
                    con=self.conn,
                    if_exists="append",
                    index=False
                )
                logging.info("====== Запись фрагмента проведена успешно")
            else:
                logging.info("====== Фрагмент не содержит данных")

    def clear_all(self):
        with self.conn:
            cursor = self.conn.cursor()
            del_query = "DELETE from gg_awards"
            cursor.execute(del_query)
        logging.info("====== Все записи таблицы успешно удалены")


class Reader:
    """
    Класс для кусочного (chunky) извлечения данных из файла
    """
    def __init__(self, filepath):
        self.filepath = filepath

    def get_data_chunk(self, size):
        """
        Генератор датафрейм-фрагментов.
        Размер фрагмента задается количеством строк
        """
        with pd.read_csv(self.filepath, chunksize=size) as r:
            for chunk in r:
                yield chunk


if __name__ == "__main__":

    path_to_file = "golden_globe_awards.csv"
    db_conn = "file:golden_globe_awards.db?moe=rw"

    gga_db = DB(db_conn)
    reader = Reader(path_to_file)

    if args.clear:
        gga_db.clear_all()
    for ch in reader.get_data_chunk(size=1000):
        gga_db.write_data(ch, year_award=year, winners_only=args.winner)
