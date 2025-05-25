import csv
from Objects import IdExp, BoolExp, NumberExp, BinaryExp, StringExp, BetweenExp
from Constantes import BinaryOp
from psycopg2 import sql
from Token import Type
from bin_data.BinaryManager import BinStorageManager
from MainIndex import MainIndex


class VisitorExecutor:
    def __init__(self, db, conn):
        self.db = db
        self.conection = conn

        #Agregado
        self.bin_manager = BinStorageManager()

        # Crear la extensión de gist en la DB en caso no se tenga creada
        cursor = self.conection.cursor()

        cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

        self.conection.commit()
        cursor.close()


    def visit_select(self, stmt):
        table_name = stmt.table

        if table_name not in self.db:
            raise ValueError(f"Tabla '{table_name}' no encontrada")

        rows = self.db[table_name]
        selected_rows = []

        TablaAtributos = rows[0].keys()
        for att in stmt.atributos:
            if att != '*' and att.lower() not in TablaAtributos:
                raise ValueError(f"Atributo: {att} no presente en la tabla {table_name}")


        ################## INDEXAR #######################


        print("\nResultado del SELECT:")
        for r in selected_rows:
            print(r)

    # Falta: Realizar Search del ID si es que inserta ( en caso sea SERIAL )
    # Insertar los indices en el archivo indexs.bin (table:atribute:index)
    def visit_insert(self, stmt):

        cursor = self.conection.cursor()
        table_name = stmt.table

        # Validación de la tabla en la bdd
        if table_name not in self.db:
            raise ValueError(f"Tabla '{table_name}' no encontrada")

        table_data = self.db[table_name]

        if table_data:
            # Si la tabla ya tiene filas, tomamos los atributos desde la primera fila
            TablaAtributos = table_data[0].keys()
        else:
            # Si no hay filas aún, recuperamos los atributos desde meta.json
            meta_info = self.bin_manager.meta.get(table_name.lower())
            if meta_info and "columns" in meta_info:
                TablaAtributos = meta_info["columns"]
            else:
                raise ValueError(f"No se pudo determinar los atributos de la tabla '{table_name}'")

        # Validación de que cada columna esté en la tabla
        for att in stmt.atributos:
            if att.lower() not in TablaAtributos:
                raise ValueError(f"Atributo: {att} no presente en la tabla {table_name}")

        # Validación de que el número de argumentos sean iguales al número de atributos colocados
        for RowToInsert in stmt.values:
            if len(stmt.atributos) != len(RowToInsert):
                raise ValueError("Se esperaba la misma cantidad de argumentos a insertar.")

        # Formato base para el insert
        query = sql.SQL("INSERT INTO {table} ({atributos}) VALUES ({values})").format(
            table=sql.Identifier(table_name.lower()),
            atributos=sql.SQL(', ').join(map(lambda att: sql.Identifier(att.lower()), stmt.atributos)),
            values=sql.SQL(', ').join(sql.Placeholder() * len(stmt.atributos))
        )

        # Funciona si el primary key es SIMILAR y numérico ( corregir luego ).
        #last_id = self.db[table_name][-1]['id']

        #ACTUAL:
        if self.db[table_name]:
            last_id = int(self.db[table_name][-1].get('id', 0))
        else:
            last_id = 0

        ################## INDEXAR #######################


        # Guardar los cambios realizados
        self.conection.commit()
        cursor.close()

        print(f"\nInserción realizada con éxito, se insertaron {len(stmt.values)} a {table_name}")

    def visit_delete(self, stmt):

        cursor = self.conection.cursor()
        if stmt.table not in self.db:
            raise ValueError(f"Tabla '{stmt.table}' no encontrada")

        print("\nREPORTE DEL DELETE:")

        ################## INDEXAR #######################

        print("No se encontraron filas para eliminar. Consulta no ejecutada.")

    def visit_create(self, stmt):

        cursor = self.conection.cursor()

        # Verificando ya ha sido creada antes.
        if stmt.name in self.db:
            raise ValueError(f"Tabla '{stmt.name}' ya existe")

        # Verificando que no exista más de una primary key
        numPrimaryKey = 0
        for atribute in stmt.columns:
            if atribute[2]:
                numPrimaryKey += 1

            if numPrimaryKey > 1:
                raise ValueError("No se puede crear una tabla con más de una llave primaria")


        # Estructuramos el contenidos en sintaxis de postgres SQL.
        atribute_and_type = [tupla[:3] for tupla in stmt.columns]

        is_array = lambda type_att: f"{type_att[1].name}[]" if isinstance(type_att, list) else type_att.name
        is_pk = lambda pk: " PRIMARY KEY" if pk else ""

        columns = [att + " " + is_array(type_att) + is_pk(pk)
                   for att, type_att, pk in atribute_and_type]

        # Construimos la consulta
        query = sql.SQL("CREATE TABLE {table} ({atributes})").format(
            table=sql.Identifier(stmt.name.lower()),
            atributes=sql.SQL(', ').join(sql.SQL(col) for col in columns)
        )

        # Creamos la Tabla
        cursor.execute(query)

        # Consiguiendo los atributos con el mismo índice
        att_index = {}

        for att_content in stmt.columns:
            if att_content[3] is not None:

                if att_content[3] not in att_index:
                    att_index[att_content[3]] = []

                att_index[att_content[3]].append(att_content[0])

        # Añadiendo los indices

        for index in att_index:

            # Indices no existentes en POSTGRES ( se usarán solamente en el python, no se insertaran en el postgres.
            if index in ["AVL", "HASH", "SEQ", "RTREE"]:
                continue

            index_to_aplicar = index
            for attr in att_index[index]:

                index_name = f"{stmt.name.lower()}_{index.lower()}_{attr.lower()}_idx"

                query = sql.SQL("CREATE INDEX {name} ON {table} USING {idx} ({attribute})").format(
                    name=sql.Identifier(index_name),
                    table=sql.Identifier(stmt.name.lower()),
                    idx=sql.SQL(index_to_aplicar.lower()),
                    attribute=sql.Identifier(attr.lower())
                )

                # Creando cada indice
                cursor.execute(query)

        # Que se guarde el nombre de las tablas en minúsculas.
        columnas = {col[0].lower(): None for col in stmt.columns}

        # inicializacion vacía con encabezados

        # ANTES: self.db[stmt.name] = [columnas.copy()]
        # ACTUAL:
        self.db[stmt.name] = []

        # AGREGADO
        header = []
        for name, type_token, is_pk, index in stmt.columns:
            if isinstance(type_token, list):  # ARRAY
                inner_type = type_token[1].name.upper()
                full_type = f"ARRAY[{inner_type}]"
            else:
                full_type = type_token.name.upper()

            header.append({
                "name": name.lower(),
                "type": full_type,
                "indexes": [index.lower()] if index else []
            })

        self.bin_manager.save_table(stmt.name, self.db[stmt.name], header=header)

        # Guardamos los cambios
        self.conection.commit()
        cursor.close()

        print(f"\nTabla '{stmt.name}' creada con columnas: {list(columnas.keys())}")

    def visit_create_from_file(self, stmt):

        if stmt.name in self.db:
            raise ValueError(f"Tabla '{stmt.name}' ya existe")

        cursor = self.conection.cursor()

        with open(stmt.file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            csv_content = [row for row in reader]

        if not csv_content:
            raise ValueError("El csv está vacio.")

        self.db[stmt.name] = csv_content

        keys = list(csv_content[0].keys())
        values = [list(row.values()) for row in csv_content]


        #### Creando la tabla ####

        # Crear la tabla con columnas tipo TEXT por defecto
        column_defs = ', '.join(f"{col} TEXT" for col in keys)
        create_table_query = f'CREATE TABLE "{stmt.name.lower()}" ({column_defs});'
        cursor.execute(create_table_query)

        #### Insertando las filas ####

        insert_query = sql.SQL("INSERT INTO {table} ({columns}) VALUES ({placeholders})").format(
            table=sql.Identifier(stmt.name.lower()),
            columns=sql.SQL(', ').join(map(sql.Identifier, keys)),
            placeholders=sql.SQL(', ').join(sql.Placeholder() * len(keys))
        )

        for RowToInser in values:
            cursor.execute(insert_query, RowToInser)

        #### Insertando el indicd ####

        # Indices no existentes en POSTGRES ( se usarán solamente en el python, no se insertaran en el postgres.
        if stmt.index_type not in ["AVL", "HASH", "SEQ", "RTREE"]:

            intex_to_aplicar = stmt.index_type

            if intex_to_aplicar == "RTREE":
                intex_to_aplicar = "GIST"

            index_name = f"{stmt.name.lower()}_{intex_to_aplicar.lower()}_{stmt.index_field.lower()}_idx"

            index_query = sql.SQL("CREATE INDEX {name} ON {table} USING {idx} ({attribute})").format(
                name=sql.Identifier(index_name),
                table=sql.Identifier(stmt.name.lower()),
                idx=sql.SQL(intex_to_aplicar.lower()),
                attribute=sql.Identifier(stmt.index_field.lower())
            )

            cursor.execute(index_query)

        self.conection.commit()
        cursor.close()

        print(f"\nTabla '{stmt.name}' creada desde archivo con {len(self.db[stmt.name])} filas")

    def visit_create_index(self, stmt):

        if stmt.table not in self.db:
            raise ValueError(f"Tabla '{stmt.table}' no existe")

        TotalAtributos = self.db[stmt.table][0].keys()

        for col in stmt.list_atributos:
            if col.lower() not in TotalAtributos:
                raise ValueError(f"No existe el atributo: {col} en la tabla {stmt.table}")

        # Asignarlo en la meta data
        if stmt.index_type in ["AVL", "HASH", "SEQ", "RTREE"]:
            return

        cursor = self.conection.cursor()

        index_query = sql.SQL("CREATE INDEX {name} ON {table} USING {idx} ({attributes})").format(
            name=sql.Identifier(stmt.index_name),
            table=sql.Identifier(stmt.table.lower()),
            idx=sql.SQL(stmt.index_type.lower()),
            attributes=sql.SQL(', ').join(sql.Identifier(col.lower()) for col in stmt.list_atributos)
        )

        cursor.execute(index_query)

        self.conection.commit()
        cursor.close()

    def visit_drop_index(self, stmt):

        cursor = self.conection.cursor()

        index_query = sql.SQL("DROP INDEX {index}").format(
            index = sql.Identifier(stmt.index_name.upper())
        )

        cursor.execute(index_query)

        self.conection.commit()
        cursor.close()

## Tener mucho cuidado con el indices, no todos estan disponibles en postgres.
