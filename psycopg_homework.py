import psycopg2


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS customer (
            customer_id SERIAL PRIMARY KEY,
            first_name VARCHAR(40) NOT NULL,
            last_name VARCHAR(40) NOT NULL,
            email VARCHAR(50) NOT NULL
            );
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS phone_numbers (
            customer_id SERIAL REFERENCES customer(customer_id),
            phone_id SERIAL,
            phone_number VARCHAR(20)    
            );
        ''')
        conn.commit()
        
        
def add_customer(conn, first_name, last_name, email, phone=None): 
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO customer(first_name, last_name, email) VALUES (%s, %s, %s);
            ''', (first_name, last_name, email))
        
        if phone:
            cur.execute('''
                SELECT customer_id FROM customer ORDER BY customer_id DESC
                ''')
            cust_id = cur.fetchone()[0]
    
            cur.execute('''
                INSERT INTO phone_numbers(customer_id, phone_number) VALUES(%s, %s);
                ''', (cust_id, phone))
        
        conn.commit()


def add_phone(conn, customer_id, phone):
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO phone_numbers(customer_id, phone_number) VALUES(%s, %s);
            ''', (customer_id, phone))
        
        conn.commit()


def change_customer_info(
        conn, customer_id, first_name=None, last_name=None, 
        email=None, phone_number=None):
    with conn.cursor() as cur:
        if first_name:
            cur.execute('''
                UPDATE customer
                   SET first_name = %s
                 WHERE customer_id = %s   
                ''', (first_name, customer_id))
        
        if last_name:
            cur.execute('''
                UPDATE customer
                   SET last_name = %s
                 WHERE customer_id = %s   
                ''', (last_name, customer_id))
        
        if email:
            cur.execute('''
                UPDATE customer
                   SET email = %s
                 WHERE customer_id = %s   
                ''', (email, customer_id))
        
        if phone_number:
            cur.execute('''
                UPDATE phone_numbers
                   SET phone_number = %s
                 WHERE customer_id = %s   
                ''', (phone_number, customer_id))
        
        conn.commit()


def delete_phone(conn, customer_id, phone_number):
    with conn.cursor() as cur:
        cur.execute('''
            DELETE FROM phone_numbers
             WHERE customer_id = %s 
               AND phone_number = %s
            ''', (customer_id, phone_number))
        
        conn.commit()


def delete_customer(conn, customer_id):
    with conn.cursor() as cur:
        
        cur.execute(''' 
            DELETE FROM phone_numbers
             WHERE customer_id = %s
            ''', (customer_id,))
        
        cur.execute(''' 
            DELETE FROM customer
             WHERE customer_id = %s
            ''', (customer_id,))
        
        conn.commit()            


def find_customer(
        conn, first_name=None, last_name=None, 
        email=None, phone_number=None):
    with conn.cursor() as cur:
        if phone_number:
            cur.execute('''
                SELECT customer_id, first_name, last_name, email, phone_number
                  FROM phone_numbers
                  JOIN customer USING(customer_id)
                 WHERE phone_number = %s  
                ''', (phone_number,))
            print(cur.fetchall())
        else:
            cur.execute('''
                SELECT customer_id, first_name, last_name, email, phone_number
                  FROM customer
                  JOIN phone_numbers USING(customer_id)
                 WHERE first_name = %s 
                    OR last_name = %s 
                    OR email = %s  
                ''', (first_name, last_name, email))
            print(cur.fetchall())    
       
        
with psycopg2.connect(database='', user='', password='') as connect:
    # create_db(connect)
    
    # add_customer(connect, 'Иван', 'Иванов', 'ivanov@mail.ru', '89601231231')
    
    # add_customer(connect, 'Петр', 'Петров', 'petrov@mail.ru', '89998887766')
    
    # add_phone(connect, 1, '89008007060')
    
    # change_customer_info(connect, 19, 'ПЕТР', 'ПЕТРОВ', 'PETROV@mail.ru', 8888888)
    
    # delete_phone(connect, 1, '89008007060')
    
    # delete_customer(connect, 19)
    
    # find_customer(connect, 'Петр', 'Петров')
    # find_customer(connect, email='petrov@mail.ru')
    # find_customer(connect, phone_number='89998887766')
    
    connect.close()
