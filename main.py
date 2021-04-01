import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql


class TwoPhaseTransaction:
    def __init__(self, db_name, format_id, gtrid, bqual, user, password, port):
        self.conn = psycopg2.connect(f"dbname={db_name} user={user} port={port} password={password}")
        self.xid = self.conn.xid(format_id, gtrid, bqual)

    def begin_tpc(self, sql):
        self.conn.tpc_begin(self.xid)

        cur = self.conn.cursor()
        cur.execute(sql)

        self.conn.tpc_prepare()

    def rollback(self):
        self.conn.tpc_rollback()

    def commit(self):
        self.conn.tpc_commit()

    # for testing purpose only
    def recover(self):
        print(self.conn.tpc_recover())
        self.conn.tpc_rollback(self.xid)



class DistributedDatabases:
    format_id, gtrid  = 1, "transaction"

    def __init__(self, user, password, port):
        self.user = user
        self.password = password
        self.port = port

    def book_hotel_and_fly(self):
        try:
            t_fly_booking = TwoPhaseTransaction("d_db_fly_booking", self.format_id, self.gtrid, "fly_booking",
                                                self.user, self.password, self.port)
            t_hotel_booking = TwoPhaseTransaction("d_db_hotel_booking", self.format_id, self.gtrid, "hotel_booking",
                                                  self.user, self.password, self.port)
            t_account = TwoPhaseTransaction("d_db_account", self.format_id, self.gtrid, "account",
                                            self.user, self.password, self.port)
            t_fly_booking.begin_tpc("""
                        INSERT INTO fly_booking (clientName, flyNumber, "from", "to", date)
                        VALUES ('Ivan Popov', 1, 'Lviv', 'NY', '01.01.2021')
                    """)

            t_hotel_booking.begin_tpc("""
                INSERT INTO hotel_booking (clientname, hotelname, arrival, departure)
                VALUES ('Ivan Popov', 'Hilton', '02.01.2021', '08.01.2021')
                """)

            t_account.begin_tpc("""
                UPDATE account SET amount=amount-200 WHERE clientName='Ivan Popov'
            """)
            except psycopg2.errors.CheckViolation:
                print('booking failure, no money on account')
                t_account.rollback()
                t_fly_booking.rollback()
                t_hotel_booking.rollback()
            else:
                print('booking success')
                t_fly_booking.commit()
                t_hotel_booking.commit()

                # to imitate lock comment this string
                t_account.commit()
        except psycopg2.Error as e:
            print(e)

    def recover(self):
        TwoPhaseTransaction("d_db_fly_booking", self.format_id, self.gtrid, "fly_booking",
                            self.user, self.password, self.port).recover()
        TwoPhaseTransaction("d_db_hotel_booking", self.format_id, self.gtrid, "hotel_booking",
                            self.user, self.password, self.port).recover()
        TwoPhaseTransaction("d_db_account", self.format_id, self.gtrid, "account",
                            self.user, self.password, self.port).recover()

    def initDB(self):
        self.createDBs()
        self.create_tables()

    def createDBs(self):
        con = psycopg2.connect(f"dbname=postgres user={self.user} port={self.port} password={self.password}")
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()
        cursor.execute(sql.SQL('CREATE DATABASE {};').format(sql.Identifier("d_db_account")))
        cursor.execute(sql.SQL('CREATE DATABASE {};').format(sql.Identifier("d_db_fly_booking")))
        cursor.execute(sql.SQL('CREATE DATABASE {};').format(sql.Identifier("d_db_hotel_booking")))

    def create_tables(self):
        self.create_fly_booking_table()
        self.create_hotel_booking_table()
        self.create_account_table()

        self.init_account_table()

    def init_account_table(self):
        # todo as simple transaction
        t_account = TwoPhaseTransaction("d_db_account", self.format_id, self.gtrid, "account",
                                        self.user, self.password, self.port)
        t_account.begin_tpc("""
            INSERT INTO account (clientname, amount)
            VALUES ('Ivan Popov', 400)
            """)

        t_account.commit()

    def create_fly_booking_table(self):
        fly_booking_conn = psycopg2.connect(
            f"dbname='d_db_fly_booking' user={self.user} port={self.port} password={self.password}"
        )
        fly_booking_cur = fly_booking_conn.cursor()

        fly_booking_cur.execute("""
                CREATE TABLE IF NOT EXISTS fly_booking (
                bookingID SERIAL PRIMARY KEY,
                clientName varchar(255),
                flyNumber varchar(255),
                "from" varchar(255),
                "to" varchar(255),
                date varchar(255)
              )
            """)

        fly_booking_conn.commit()

    def create_hotel_booking_table(self):
        hotel_booking_conn = psycopg2.connect(
            f"dbname='d_db_hotel_booking' user={self.user} port={self.port} password={self.password}"
        )
        hotel_booking_cur = hotel_booking_conn.cursor()

        hotel_booking_cur.execute("""
                CREATE TABLE IF NOT EXISTS hotel_booking (
                bookingID SERIAL PRIMARY KEY,
                clientName varchar(255),
                hotelName varchar(255),
                arrival varchar(255),
                departure varchar(255)
              )
            """)

        hotel_booking_conn.commit()

    def create_account_table(self):
        account_conn = psycopg2.connect(
            f"dbname='d_db_account' user={self.user} port={self.port} password={self.password}"
        )
        account_conn_cur = account_conn.cursor()

        account_conn_cur.execute("""
                CREATE TABLE IF NOT EXISTS account (
                accountID SERIAL PRIMARY KEY,
                clientName varchar(255),
                amount NUMERIC CHECK (amount > 0)
              )
            """)

        account_conn.commit()


# DistributedDatabases('admin', 1111, 5433).recover()
DistributedDatabases('admin', 1111, 5433).book_hotel_and_fly()
# DistributedDatabases('admin', 1111, 5433).initDB()
