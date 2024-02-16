import psycopg2
import random

#  Create the database connection to the database "bookkeeping"
conn = psycopg2.connect(host='localhost', password='YOUR_PASSWORD', port=5432, user='postgres', database='bookkeeping')
cur = conn.cursor()

# cur.execute('''
# DROP TABLE IF EXISTS author_books;
# DROP TABLE IF EXISTS authors;
# DROP TABLE IF EXISTS books;
# ''')
# conn.commit()

create_tables_stmt = '''
    CREATE TABLE IF NOT EXISTS Authors (id SERIAL PRIMARY KEY UNIQUE, first_name VARCHAR (100) NOT NULL, 
    last_name VARCHAR (100) NOT NULL, CONSTRAINT unique_first_last_names_constraints UNIQUE(first_name, last_name)); 
    
    CREATE TABLE IF NOT EXISTS Books (id SERIAL PRIMARY KEY UNIQUE, title VARCHAR (100) NOT NULL, 
    number_of_pages INT NOT NULL, CONSTRAINT title_pages_constraints UNIQUE(title, number_of_pages)); 
    
    CREATE TABLE IF NOT EXISTS Author_books (id SERIAL PRIMARY KEY UNIQUE, 
    author_id SERIAL NOT NULL, book_id SERIAL NOT NULL UNIQUE, 
    CONSTRAINT fk_author_id FOREIGN KEY (author_id) REFERENCES Authors(id) ON DELETE CASCADE, 
    CONSTRAINT fk_book_id FOREIGN KEY (book_id) REFERENCES Books(id) ON DELETE CASCADE,
    CONSTRAINT unique_author_books UNIQUE(author_id, book_id)); 
'''

data = [{'book_id': 1, 'title': 'The Mystery', 'number_of_pages': 300, 'author_id': 1, 'first_name': 'John',
         'last_name': 'Doe'},
        {'book_id': 2, 'title': 'Journey to the Stars', 'number_of_pages': 250, 'author_id': 2, 'first_name': 'Jane',
         'last_name': 'Smith'},
        {'book_id': 3, 'title': 'Beyond the Horizon', 'number_of_pages': 400, 'author_id': 3, 'first_name': 'Michael',
         'last_name': 'Johnson'},
        {'book_id': 4, 'title': 'Secrets Unveiled', 'number_of_pages': 320, 'author_id': 4, 'first_name': 'Sarah',
         'last_name': 'Williams'},
        {'book_id': 5, 'title': 'Echoes of Time', 'number_of_pages': 280, 'author_id': 5, 'first_name': 'Robert',
         'last_name': 'Davis'},
        {'book_id': 6, 'title': 'Whispers in the Wind', 'number_of_pages': 200, 'author_id': 6, 'first_name': 'Emily',
         'last_name': 'Taylor'},
        {'book_id': 7, 'title': 'The Silent Observer', 'number_of_pages': 350, 'author_id': 7,
         'first_name': 'Christopher', 'last_name': 'Brown'},
        {'book_id': 8, 'title': 'Enchanted Gardens', 'number_of_pages': 180, 'author_id': 8, 'first_name': 'Olivia',
         'last_name': 'Anderson'},
        {'book_id': 9, 'title': 'Shattered Dreams', 'number_of_pages': 300, 'author_id': 9, 'first_name': 'Daniel',
         'last_name': 'Wilson'},
        {'book_id': 10, 'title': 'Lost in Translation', 'number_of_pages': 240, 'author_id': 10,
         'first_name': 'Sophia', 'last_name': 'Miller'},
        {'book_id': 11, 'title': 'Midnight Serenade', 'number_of_pages': 280, 'author_id': 1, 'first_name': 'John',
         'last_name': 'Doe'},
        {'book_id': 12, 'title': 'Captivating Moments', 'number_of_pages': 320, 'author_id': 2, 'first_name': 'Jane',
         'last_name': 'Smith'},
        {'book_id': 13, 'title': 'Shadows of the Past', 'number_of_pages': 400, 'author_id': 3,
         'first_name': 'Michael', 'last_name': 'Johnson'},
        {'book_id': 14, 'title': 'The Forgotten Realm', 'number_of_pages': 260, 'author_id': 4, 'first_name': 'Sarah',
         'last_name': 'Williams'},
        {'book_id': 15, 'title': 'Symphony of Shadows', 'number_of_pages': 300, 'author_id': 5, 'first_name': 'Robert',
         'last_name': 'Davis'},
        {'book_id': 16, 'title': "A Garden's Tale", 'number_of_pages': 220, 'author_id': 6, 'first_name': 'Emily',
         'last_name': 'Taylor'},
        {'book_id': 17, 'title': 'The Art of Silence', 'number_of_pages': 380, 'author_id': 7,
         'first_name': 'Christopher', 'last_name': 'Brown'},
        {'book_id': 18, 'title': 'Colors of the Sky', 'number_of_pages': 200, 'author_id': 8, 'first_name': 'Olivia',
         'last_name': 'Anderson'},
        {'book_id': 19, 'title': 'Broken Reflections', 'number_of_pages': 320, 'author_id': 9, 'first_name': 'Daniel',
         'last_name': 'Wilson'},
        {'book_id': 20, 'title': 'Echoes of Eternity', 'number_of_pages': 240, 'author_id': 10, 'first_name': 'Sophia',
         'last_name': 'Miller'},
        {'book_id': 21, 'title': 'The Hidden Truth', 'number_of_pages': 300, 'author_id': 1, 'first_name': 'John',
         'last_name': 'Doe'},
        {'book_id': 22, 'title': 'Serendipity', 'number_of_pages': 250, 'author_id': 2, 'first_name': 'Jane',
         'last_name': 'Smith'},
        {'book_id': 23, 'title': "The Alchemist's Legacy", 'number_of_pages': 400, 'author_id': 3,
         'first_name': 'Michael', 'last_name': 'Johnson'},
        {'book_id': 24, 'title': 'Veil of Illusions', 'number_of_pages': 320, 'author_id': 4, 'first_name': 'Sarah',
         'last_name': 'Williams'},
        {'book_id': 25, 'title': 'Eternal Odyssey', 'number_of_pages': 280, 'author_id': 5, 'first_name': 'Robert',
         'last_name': 'Davis'},
        {'book_id': 26, 'title': 'Echoes of Eternity', 'number_of_pages': 240, 'author_id': 10, 'first_name': 'Sophia',
         'last_name': 'Miller'},
        {'book_id': 27, 'title': 'The Hidden Truth', 'number_of_pages': 300, 'author_id': 1, 'first_name': 'Michael',
         'last_name': 'Johnson'},
        {'book_id': 28, 'title': 'Serendipity', 'number_of_pages': 250, 'author_id': 2, 'first_name': 'Jane',
         'last_name': 'Smith'},
        {'book_id': 29, 'title': "The Alchemist's Legacy", 'number_of_pages': 400, 'author_id': 3,
         'first_name': 'Michael', 'last_name': 'Johnson'},
        {'book_id': 30, 'title': 'Veil of Illusions', 'number_of_pages': 320, 'author_id': 4, 'first_name': 'Jane',
         'last_name': 'Smith'},
        {'book_id': 31, 'title': 'Eternal Odyssey', 'number_of_pages': 280, 'author_id': 5, 'first_name': 'Robert',
         'last_name': 'Davis'}]


random.shuffle(data)  # Used just to obtain/test different datasets case scenarios

insert_stmt1 = '''INSERT INTO books (id, title, number_of_pages) VALUES(%(book_id)s, %(title)s, %(number_of_pages)s) 
ON CONFLICT DO NOTHING; INSERT INTO authors (id, first_name, last_name) VALUES(%(author_id)s, %(first_name)s, 
%(last_name)s) ON CONFLICT DO NOTHING; '''

insert_stmt2 = '''INSERT INTO author_books (author_id, book_id) VALUES((SELECT id FROM authors WHERE first_name = %(
first_name)s AND last_name = %(last_name)s), (SELECT id FROM books WHERE title = %(title)s AND number_of_pages = %(
number_of_pages)s)); '''


cur.execute(create_tables_stmt)
cur.executemany(insert_stmt1, data)
conn.commit()

#  Perform the INSERT (data) operations executing rollback if any constraint exception is encountered.
for row in data:
    try:
        cur.execute(insert_stmt2, row)
    except Exception as e:
        print(e)
        conn.rollback()
    else:
        conn.commit()

# Query the database and select all records in the author_books table, order by ID.
cur.execute('SELECT * FROM author_books ORDER BY id')
print(cur.fetchall(), "\n")

# Query the database and select all records in the books table, order by ID.
cur.execute('SELECT * FROM books ORDER BY id')
print(cur.fetchall(), "\n")

# Query the database and select all records in the authors table, order by ID.
cur.execute('SELECT * FROM authors ORDER BY id')
print(cur.fetchall(), "\n")
cur.close()

