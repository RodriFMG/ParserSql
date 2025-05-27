from Scanner import Scanner
from Parser import ParserSQL
from Visitor import VisitorExecutor
from bin_data.BinaryManager import BinStorageManager
import psycopg2

#AGREGADO
def ExtractAllTables(conection):

    cursor = conection.cursor()

    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE';
    """)

    tables = [tabla[0] for tabla in cursor.fetchall()]

    database = {}

    for tabla_name in tables:
        cursor.execute(f"SELECT * FROM {tabla_name}")

        # Cursor description: Consigo el nombre de los atributos
        AtributeName = [atribute[0] for atribute in cursor.description]
        rows = cursor.fetchall()

        listrow = []
        for row in rows:
            dbrow = {}
            for idx, data_row in enumerate(row):
                dbrow.update({AtributeName[idx]: data_row})
            listrow.append(dbrow)

        # Si no hay filas, se agrega una fila vacía con solo los encabezados
        if not listrow:
            listrow = [{col: None for col in AtributeName}]

        database.update({tabla_name.upper(): listrow})

    cursor.close()
    return database


def ExecuteSQLParser(code:str):

    scanner = Scanner(code)
    parser = ParserSQL(scanner)
    program = parser.ParseProgram()

    # Hacemos la coneccion
    # Conexión a la base de datos
    conn = psycopg2.connect(
        dbname='proydb2',
        user='postgres',
        password='2019wess:v',
        host='localhost',
        port="5432"
    )

    # Extraer TODAS las tablas de la base de datos - Sincroniza con el postgres
    db = ExtractAllTables(conn)
    # AGREGADO
    bin_manager = BinStorageManager(pg_conn=conn)
    #AGREGADO, VERIFICA LAS MODIFICACIONES
    for table in db:
        if bin_manager.is_synced(table, db[table]):
            print(f"[BIN] Tabla '{table}' sincronizada. Cargando desde archivo binario.")
            db[table] = bin_manager.load_table(table)
        else:
            print(f"[BIN] Tabla '{table}' desactualizada. Guardando nueva versión.")
            bin_manager.save_table(table, db[table])
    



    # Ejecutar código
    executor = VisitorExecutor(db, conn)
    program.accept(executor)

    return executor;  