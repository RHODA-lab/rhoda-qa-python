import requests
import json
import psycopg2
import psycopg2.errorcodes
import time
import logging
import random
import pandas as pd
from pyservicebinding import binding
#
response = requests.get('http://localhost:8080')
jresponse = response.json()
if jresponse['status'] != "DB binding ok":
  print(jresponse['status'])
  exit(1)
sb = binding.ServiceBinding()
#bindings_list = sb.bindings('postgresql', 'Red Hat DBaaS / Amazon Relational Database Service (RDS)')
bindings_list = sb.bindings('postgresql', 'OpenShift Database Access / Amazon Relational Database Service (RDS)')
print(bindings_list[0].get('database'), '\n', \
    bindings_list[0].get('username'), '\n', \
    bindings_list[0].get('password'), '\n', \
    bindings_list[0].get('host'), '\n', \
    bindings_list[0].get('port'))
db_connection = psycopg2.connect(database=bindings_list[0].get('database'), \
    user=bindings_list[0].get('username'), \
    password=bindings_list[0].get('password'), \
    host=bindings_list[0].get('host'), \
    port=bindings_list[0].get('port'))
print(db_connection)

#
def create_accounts(conn):
    with conn.cursor() as cur:
        cur.execute('CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)')
        cur.execute('INSERT INTO accounts (id, balance) VALUES (1, 1000), (2, 250)')
        logging.debug("create_accounts(): status message: {}".format(cur.statusmessage))
    conn.commit()
#
def delete_table(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS accounts")
        logging.debug("delete_accounts(): status message: {}".format(cur.statusmessage))
    conn.commit()

#
def query(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, balance FROM accounts")
        logging.debug("print_balances(): status message: {}".format(cur.statusmessage))
        rows = cur.fetchall()
        conn.commit()
        print("Balances at {}".format(time.asctime()))
        for row in rows:
            print([str(cell) for cell in row])
#
def run_transaction(conn, op):
    retries = 0
    max_retries = 3
    with conn:
        while True:
            retries +=1
            if retries == max_retries:
                err_msg = "Transaction did not succeed after {} retries".format(max_retries)
                raise ValueError(err_msg)

            try:
                op(conn)

                # If we reach this point, we were able to commit, so we break
                # from the retry loop.
                break
            except psycopg2.Error as e:
                logging.debug("e.pgcode: {}".format(e.pgcode))
                if e.pgcode == '40001':
                    # This is a retry error, so we roll back the current
                    # transaction and sleep for a bit before retrying. The
                    # sleep time increases for each failed transaction.
                    conn.rollback()
                    logging.debug("EXECUTE SERIALIZATION_FAILURE BRANCH")
                    sleep_ms = (2**retries) * 0.1 * (random.random() + 0.5)
                    logging.debug("Sleeping {} seconds".format(sleep_ms))
                    time.sleep(sleep_ms)
                    continue
                else:
                    logging.debug("EXECUTE NON-SERIALIZATION_FAILURE BRANCH")
                    raise e

#
def transfer_funds(conn, frm, to, amount):
    with conn.cursor() as cur:

        # Check the current balance.
        cur.execute("SELECT balance FROM accounts WHERE id = " + str(frm))
        from_balance = cur.fetchone()[0]
        if from_balance < amount:
            err_msg = "Insufficient funds in account {}: have {}, need {}".format(frm, from_balance, amount)
            raise RuntimeError(err_msg)

        # Perform the transfer.
        cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s",
                    (amount, frm))
        cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s",
                    (amount, to))
    conn.commit()
    logging.debug("transfer_funds(): status message: {}".format(cur.statusmessage))

delete_table(db_connection)
create_accounts(db_connection)
query(db_connection)

amount = 100
fromId = 1
toId = 2

try:
    run_transaction(db_connection, lambda db_connection: transfer_funds(db_connection, fromId, toId, amount))
except ValueError as ve:
    logging.debug("run_transaction(db_connection, op) failed: {}".format(ve))
    pass
query(db_connection)

#
def final_verification(conn):
    df_ref = pd.read_csv('validate.csv')
    cur = conn.cursor()
    cur.execute("SELECT * FROM accounts")
    logging.debug("select_all(): status message: {}".format(cur.statusmessage))
    df = pd.DataFrame(cur.fetchall(), columns = ['id','balance'])
    return df.equals(df_ref)

print("******* Validation is: ", final_verification(db_connection))
delete_table(db_connection)
