import psycopg2 as pg
from qazm import posintput


def create_db(connection, table_name, *columns):  # создает таблицы
    if isinstance(table_name, str):
        string_columns = ''
        for column in columns:
            if isinstance(column, str):
                #print(f'создаем столбец {column} в таблице {table_name}')
                string_columns += column+', '
        string_columns = string_columns.strip(', ')
        with connection.cursor() as cursor:
            if string_columns > '':
                try:
                    cursor.execute(f"CREATE TABLE {table_name} ({string_columns})")
                    return True
                except Exception as e:
                    print(f'Не удалось создать таблицу по причине: {e}')
    return False


def get_students(connection, course_id):  # возвращает студентов определенного курса
    with connection.cursor() as cursor:
        try:
            cursor.execute(f"select * from student where id in (select student_id from course_student " \
                           f"where course_id = {course_id})")
            return cursor.fetchall()
        except Exception as e:
            print(f'Не удалось определить студентов курса {course_id} по причине: {e}')


def add_students(connection ,course_id, students): # создает студентов и записывает их на курс
    with connection.cursor() as cursor:
        try:
            for student in students:
                student_id = add_student(connection, student)
                if student_id:
                    print(f'Студент  добавлен {student_id}')
                    cursor.execute("insert into course_student (course_id, student_id) values(%s, %s) returning id",
                                   (course_id, student_id))
                else:
                    print(f'Студент с данными {student} не был добавлен на курс')
            return True
        except Exception as e:
            print(f'Не удалось записать студента на курс по причине: {e}')


# student = {name:string(100) ,gpa:float(10), birth:datetime}
def add_student(connection, student): # просто создает студента
    with connection.cursor() as cursor:
            student_name = student.get('name')
            student_gpa = student.get('gpa')
            student_birth = student.get('birth')
            if student_name and student_gpa and student_birth:
                try:
                    cursor.execute("insert into student (name, gpa, birth) values(%s, %s, %s) returning id",
                                   (student_name, student_gpa, student_birth))
                    student_id = cursor.fetchall()[0][0]
                    return student_id
                except Exception as e:
                    print(f'Не удалось добавить студента по причине {e}')
            else:
                print(f'Не достаточно данных для добавления: ФИО: {student_name}, Оценка: {student_gpa},'
                      f' Дата рождения: {student_birth}')


def get_student(connection, student_id):
    with connection.cursor() as cursor:
        try:
            cursor.execute("select * from student where id =  %s",
                           (student_id,))
            student_info = cursor.fetchall()
            return student_info if student_info else None

        except Exception as e:
            print(f'Не удалось найти студента {student_id} по причине {e}')


def add_cource(connection, name_course):
    with connection.cursor() as cursor:
        try:
            if isinstance(name_course, str) and name_course:
                cursor.execute("insert into course (name ) values(%s) returning id",
                               (name_course,))
                course_id = cursor.fetchall()[0][0]
                return course_id
            else:
                print('Некорректное название курса')
        except Exception as e:
            print(f'Не удалось добавить курс по причине: {e}')


def get_all_course(connection):
    with connection.cursor() as cursor:
        try:
            cursor.execute("select id, name from course")
            all_course = cursor.fetchall()
            return all_course if all_course else None
        except Exception as e:
            print(f'Не удалось найти курсы по причине: {e}')


if __name__ == '__main__':
    connection = pg.connect(dbname='netology', user='netology', password='netology')

    table_list = [{'table_name': "student",
                   'parameters': ('id serial PRIMARY KEY', 'name varchar(100)', 'gpa numeric(3, 2)',
                                  'birth timestamp with time zone')},
                  {'table_name': 'course',
                   'parameters': ('id serial PRIMARY KEY', 'name varchar(100)')},
                  {'table_name': 'course_student',
                   'parameters': ('id serial PRIMARY KEY', 'course_id integer references course(id)',
                                  'student_id integer references student(id)')}]

    print('Cоздаем таблицы')
    for table in table_list:
        with connection:
            if create_db(connection, table.get('table_name'), *table.get('parameters')):
                print(f"Таблица {table.get('table_name')} успешно создана")

    while True:
        if input('Добавим студента в БД?(y): ').upper() == 'Y':
            student_data = input('Внесите данные студента через запятую: ФИО, оценка, '
                                 'дата рождения в формате 01.01.2001): ').split(',')
            if len(student_data) == 3:
                student = {'name': student_data[0], 'gpa': student_data[1], 'birth': student_data[2]}
            else:
                student = {'name': 'Vasya', 'gpa': 9.20, 'birвth': '20.12.2000'}
                print(f"Неверные данные студента, пожалуй я добавлю своего студента:\n"
                      f"{student.get('name')}, {student.get('gpa')}, {student.get('birth')}")
            with connection:
                if add_student(connection, student):
                    print('Студент успешно добавлен')

        if input('Добавим новый курс в БД?(y): ').upper() == 'Y':
            with connection:
                cource_name = input('Введите название курса: ')
                if cource_name:
                    print(f"Номер нового курса: {add_cource(connection,cource_name)}")
                else:
                    print('Такой курс я добавить не могу')

        if input('Поищем студента в БД по номеру?(y): ').upper() == 'Y':
            find_student_id = posintput('Введите номер студента: ')
            with connection:
                find_student = (get_student(connection, find_student_id))
                if find_student:
                    for student in find_student:
                        print(f'ИД студента: {student[0]} \nИмя студента: {student[1]} \n'
                              f'Баллы студента: {student[2]} \nДата рождения: {student[3].date()}')
                else:
                    print(f'Студент под номером {find_student_id} не найден')

        if input('Хотите записать студентов на курс?(y): ').upper() == 'Y':
            print('Список курсов:')
            for course in get_all_course(connection):
                print(f'Курс:{course[0]}-{course[1]}')
            number_course = posintput('Выберите номер курса: ')
            students =[]
            if input('Вы хотите сами добавить студентов?(y): ').upper() == 'Y':
                while True:
                    student_data = input(
                        'Внесите данные студента через запятую: ФИО, оценка, дада рождения(01.01.2001): ').split(',')
                    if len(student_data) == 3:
                        student = {'name': student_data[0], 'gpa': student_data[1], 'birth': student_data[2]}
                        students.append(student)
                        print('Студент добавлен')
                    else:
                        print('Неверно внесли данные')
                    if input('Хотите еще добавить студента?(y): ').upper() != 'Y':
                        break
            if not students:
                print('Раз у вас не нашлось студентов - я запишу своих')
                students = [{'name': 'Vasya', 'gpa': 3.20, 'birth': '20.12.1999'},
                            {'name': 'Valera', 'gpa': 2.30, 'birth': '20.12.1998'},
                            {'name': 'Sveta', 'gpa': 3.30, 'birth': '20.12.1997'}]
            with connection:
                if add_students(connection, number_course, students):
                    print('Студенты добавлены на курс')

        if input('Хотите узнать всех студентов курса?(y): ').upper() == 'Y':
            print('Список курсов:')
            for course in get_all_course(connection):
                print(f'Курс:{course[0]}-{course[1]}')
            number_course = posintput('Выберите номер курса: ')
            with connection:
                for student_of_course in get_students(connection, number_course):
                    print(f'Студент: {student_of_course[0]}-{student_of_course[1]}')

        if input('Хотите еще поработать с БД?(y): ').upper() != 'Y':
            print('Приятного отдыха')
            break


