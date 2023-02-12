import requests
import time
import logging
import pandas as pd
from pyservicebinding import binding
import oracledb


def create_accounts(conn):
    
    create_accounts_table_query = """
    CREATE TABLE accounts
    ( id number NOT NULL,
      balance number NOT NULL,
      PRIMARY KEY(id)
    )
    """
    insert_into_accounts_query = """
    INSERT ALL
        INTO accounts(id, balance) VALUES (1, 1000)
        INTO accounts(id, balance) VALUES (2, 250)
    SELECT * FROM DUAL
    """
    with conn.cursor() as cur:
        cur.execute(create_accounts_table_query)
        cur.execute(insert_into_accounts_query)
    conn.commit()


def delete_table(conn):
    
    delete_table_query = """
    DECLARE cnt NUMBER;
    BEGIN
        SELECT COUNT(*) INTO cnt FROM user_tables WHERE table_name = 'ACCOUNTS';
        IF cnt <> 0 THEN
        EXECUTE IMMEDIATE 'DROP TABLE accounts';
        END IF;
    END;
    """
    with conn.cursor() as cur:
        cur.execute(delete_table_query)
    conn.commit()


def query(conn):
    
    with conn.cursor() as cur:
        cur.execute("SELECT id, balance FROM accounts")
        rows = cur.fetchall()
        conn.commit()
        print("Balances at {}".format(time.asctime()))
        for row in rows:
            print([str(cell) for cell in row])


def transfer_funds(conn, frm, to, amount):
    
    with conn.cursor() as cur:
        # Check the current balance.
        cur.execute("SELECT balance FROM accounts WHERE id = " + str(frm))
        from_balance = cur.fetchone()[0]
        if from_balance < amount:
            err_msg = "Insufficient funds in account {}: have {}, need {}".format(frm, from_balance, amount)
            raise RuntimeError(err_msg)

        # Perform the transfer.
        cur.execute(f"UPDATE accounts SET balance = balance - {amount} WHERE id = {frm}")
        cur.execute(f"UPDATE accounts SET balance = balance + {amount} WHERE id = {to}")
        conn.commit()


def final_verification(conn):
    
    df_ref = pd.read_csv('validate.csv')
    cur = conn.cursor()
    cur.execute("SELECT * FROM accounts")
    #There is no something like statusmessage in oracledb library
    # logging.debug("select_all(): status message: {}".format(cur.statusmessage))
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
    print(sb.all_bindings())
    bindings_list = sb.bindings('oracle', 'Red Hat DBaaS / Amazon Relational Database Service (RDS)')

    # CONNECT TO ORACLE
    dsn = f'{bindings_list[0]["username"]}/{bindings_list[0]["password"]}@{bindings_list[0]["host"]}:{bindings_list[0]["port"]}/{bindings_list[0]["database"]}'
    connection = oracledb.connect(dsn)
    print(connection)

    # PROVISION TABLE
    delete_table(connection)
    create_accounts(connection)

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
    print("******* Validation is: ", final_verification(connection))
    delete_table(connection)
  
  
if __name__=="__main__":
    main()
