from Scanner import Scanner
from Parser import ParserSQL
from Visitor import VisitorExecutor
from bin_data.BinaryManager import BinStorageManager
import psycopg2
import os
from MainIndex import MainIndex


# AGREGADO
def ExtractAllTables(conection):
    cursor = conection.cursor()

    os.makedirs("./bin_data/Tablas", exist_ok=True)

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


def ver_tokens(code):
    scanner = Scanner(code)
    while True:
        token = scanner.next_token()
        print(token)  # Usará __repr__
        if token.type.name == "EOF":
            break


def CreateIndexOfPostgresToPython(table_name, index_map, pg_conn, columns, default_index = "AVL"):

    atts_table = [att[0] for att in columns]

    for att in atts_table:

        if att in index_map.keys():
            for index in index_map[att]:
                _ = MainIndex(table_name, att, index, pg_conn)
        else:
            _ = MainIndex(table_name, att, default_index, pg_conn)


if __name__ == "__main__":

    with open('Codigos/code.txt', 'r', encoding='utf-8') as f:
        code = f.read()

    # with open("CSV/news_es.csv", encoding="utf-8") as f:
    #    ver_tokens(code)

    scanner = Scanner(code)
    parser = ParserSQL(scanner)
    program = parser.ParseProgram()

    # Conexión a la base de datos
    conn = psycopg2.connect(
        dbname='proydb2',
        user='postgres',
        password='2019wess:v',
        host='localhost',
        port="5432"
    )

    # Extraer TODAS las tablas de la base de datos
    db = ExtractAllTables(conn)

    # AGREGADO
    bin_manager = BinStorageManager(pg_conn=conn)
    # AGREGADO, VERIFICA LAS MODIFICACIONES
    for table in db:

        if table.upper() == "SPATIAL_REF_SYS":
            continue

        if bin_manager.is_synced(table, db[table]):
            print(f"[BIN] Tabla '{table}' sincronizada. Cargando desde archivo binario.")
            db[table] = bin_manager.load_table(table)

        else:
            print(f"[BIN] Tabla '{table}' desactualizada. Guardando nueva versión.")
            bin_manager.save_table(table, db[table])
            print("tabla actual: ", table)
            columns, index_map = bin_manager._get_index_postgres(table)
            CreateIndexOfPostgresToPython(table, index_map, conn, columns,
                                          default_index="AVL")

    # Ejecutar código
    executor = VisitorExecutor(db, conn)
    program.accept(executor)

    print("\nRegistros por tabla desde archivos binarios:")

    '''
        for table in bin_manager.meta.keys():  # Recorre todas las tablas del meta.json
            print(f"\nTabla: {table}")
            registros = bin_manager.load_records_as_objects(table)
            for r in registros:
                print(r.to_dict())
        '''

    # Visualizar archivo .bin de una tabla específica después de ejecutar las instrucciones

    conn.close()
