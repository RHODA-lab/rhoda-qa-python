import requests
import time
import logging
import json
import pandas as pd
from pyservicebinding import binding
import pymssql 


def provision_accounts(conn):

    """
    'master' (the main sqlserver DB) cannot be used, so we need to create a new db.
    Also, we need to activate the autocommit when creating or deleting DBs: 
        https://stackoverflow.com/questions/9918129/how-can-i-create-a-database-using-pymssql
    """    
    create_database_query = """
        IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'testdb')
        BEGIN
            CREATE DATABASE testdb;
        END
    """
    
    conn.autocommit(True)
    with conn.cursor() as cur:
        cur.execute("USE master")
        cur.execute(create_database_query)
        cur.execute("USE testdb")
        cur.execute("DROP TABLE IF EXISTS accounts")
        cur.execute('CREATE TABLE accounts (id INT NOT NULL, balance INT, PRIMARY KEY(id))')
        cur.execute('INSERT INTO accounts (id, balance) VALUES (1, 1000), (2, 250)')
    conn.autocommit(False)
    

def query(conn):

    with conn.cursor() as cur:
        cur.execute("SELECT id, balance FROM accounts")
        rows = cur.fetchall()
        conn.commit()
        print("\nBalances at {}".format(time.asctime()))
        for row in rows:
            print([str(cell) for cell in row])


def transfer_funds(conn, frm, to, amount):

    with conn.cursor() as cur:
        # Check the current balance.
        cur.execute("SELECT balance FROM accounts WHERE id = " + str(frm))
        from_balance = cur.fetchone()[0]
        if from_balance < amount:
            err_msg = f"Insufficient funds in account {frm}: have {from_balance}, need {amount}"
            raise RuntimeError(err_msg)

        # Perform the transfer.
        cur.execute(f"UPDATE accounts SET balance = balance - {amount} WHERE id = {frm}")
        cur.execute(f"UPDATE accounts SET balance = balance + {amount} WHERE id = {to}")
        conn.commit()


def final_verification(conn):
    
    df_ref = pd.read_csv('validate.csv')
    cur = conn.cursor()
    cur.execute("SELECT * FROM accounts")
    cursor_fetch = cur.fetchall()
    logging.debug(f"select_all(): fetch: {cursor_fetch}")
    df = pd.DataFrame(cursor_fetch, columns = ['id','balance'])
    return df.equals(df_ref)



def main():

    # RETRIEVE DDBB INFO
    response = requests.get('http://localhost:8080')
    jresponse = response.json()
    if jresponse['status'] != "DB binding ok":
        print(jresponse['status'])
        exit(1)
    sb = binding.ServiceBinding()
    print(f"\nBINDINGS:\n{sb.all_bindings()}\n")
    bindings_list = sb.bindings('sqlserver', 'Red Hat DBaaS / Amazon Relational Database Service (RDS)')
    
    # CONNECT TO SQLSERVER
    connection = pymssql.connect(bindings_list[0]["host"],
                                 bindings_list[0]["username"],
                                 bindings_list[0]["password"],
                                 bindings_list[0]["database"])
    print(connection)
    

    # PROVISION DB AND TABLE
    provision_accounts(connection)

    # GET ACCOUNTS BALANCE
    query(connection)

    # TRANSFER BETWEEN ACCOUNTS
    amount = 100
    fromId = 1
    toId = 2
    try:
        transfer_funds(connection, fromId, toId, amount)
    except ValueError as ve:
        logging.debug("run_transaction(connection, op) failed: {}".format(ve))
        pass

    # GET ACCOUNTS BALANCE
    query(connection)

    # VALIDATE TRANSFERS
    print("\n******* Validation is: ", final_verification(connection))
  
  
if __name__=="__main__":
    main()
