import csv
from Objects import IdExp, BoolExp, NumberExp, BinaryExp, StringExp, BetweenExp
from Constantes import BinaryOp
from psycopg2 import sql
from Token import Type
from bin_data.BinaryManager import BinStorageManager
from MainIndex import MainIndex
from datetime import datetime
from bin_data.Record import RecordGeneric


class VisitorExecutor:
    def __init__(self, db, conn, default_index = "AVL"):
        self.db = db
        self.conection = conn
        self.default_index = default_index.upper()

        #Agregado
        self.bin_manager = BinStorageManager(
            pg_conn=self.conection
        )

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

        TablaAtributos = rows[0].keys()
        for att in stmt.atributos:
            if att != '*' and att.lower() not in TablaAtributos:
                raise ValueError(f"Atributo: {att} no presente en la tabla {table_name}")


        ################## INDEXAR #######################

        if stmt.condition:
            selected_rows = self.eval_condition(stmt.condition, table_name)
        else:


            selected_rows = []

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

        ################## INDEXAR #######################

        atts_table = self.bin_manager.get_atts_table(stmt.table)

        if "id" in atts_table:
            select_att_to_insert = "id"
            get_index_id = self.bin_manager.get_indexs_att(stmt.table, select_att_to_insert)
            Index = MainIndex(stmt.table, select_att_to_insert, get_index_id[0], self.conection)
        else:

            select_att_to_insert = atts_table[0]
            get_index_att = self.bin_manager.get_indexs_att(stmt.table, select_att_to_insert)
            Index = MainIndex(stmt.table, select_att_to_insert, get_index_att[0], self.conection)

        records_to_insert = []
        for record in stmt.values:
            record_eval = []

            # En teoría no debería haber ningun IdExp.
            for op in record:
                record_eval.append(self.eval_condition(op, row=None))

            records_to_insert.append(record_eval)

        record_generic_list = []
        for record_to_generic in records_to_insert:
            dic_record = dict(zip(stmt.atributos, record_to_generic))
            record_generic = RecordGeneric(atts_table)

            for att_name in stmt.atributos:
                setattr(record_generic, att_name.lower(), dic_record.get(att_name, None))

            record_generic_list.append(record_generic)

        for insert_record in record_generic_list:

            att_idx_insert = insert_record.to_dict()[select_att_to_insert.lower()]
            if not att_idx_insert:

                type_att = self.bin_manager.get_type_att(stmt.table, select_att_to_insert)

                if type_att.lower() == "serial":
                    last_row_table = self.bin_manager.get_last_row_by_attribute(stmt.table, select_att_to_insert)

                    if last_row_table is None:
                        att_idx_insert = 1
                    else:
                        att_idx_insert = last_row_table[select_att_to_insert] + 1

                    setattr(insert_record, select_att_to_insert.lower(), att_idx_insert)


                else:
                    raise ValueError("Error, key o atributo: None, al insertar. Además no es tipo serial!")
            Index.insert(
                key=att_idx_insert,
                record=insert_record
            )
            # Insertar también en PostgreSQL
            insert_dict = insert_record.to_dict()
            column_names = list(insert_dict.keys())
            column_values = [insert_dict[col] for col in column_names]

            insert_query_pg = sql.SQL("INSERT INTO {table} ({cols}) VALUES ({vals})").format(
                table=sql.Identifier(table_name.lower()),
                cols=sql.SQL(', ').join(map(sql.Identifier, column_names)),
                vals=sql.SQL(', ').join(sql.Placeholder() * len(column_names))
            )
            cursor.execute(insert_query_pg, column_values)

        self.db[table_name].extend([r.to_dict() for r in record_generic_list])
        self.bin_manager.save_table(table_name, self.db[table_name])
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

                # Para que el indice por default sea BTREE
                "indexes": [index.lower(), self.default_index.lower()] if index else [self.default_index.lower()],
                "primary_key": is_pk
            })

        self.bin_manager.save_table(stmt.name, self.db[stmt.name], header=header)

        # Consiguiendo los atributos con el mismo índice
        att_index = {}

        for att_content in stmt.columns:

            # Declarar indice por default el AVL
            att_content[3] = att_content[3] or "AVL"

            if att_content[3] not in att_index:
                att_index[att_content[3]] = []

            att_index[att_content[3]].append(att_content[0])

        # Agregar default index

        if self.default_index not in att_index:
            att_index[self.default_index] = []

        for att_content in stmt.columns:
            if att_content[0] not in att_index[self.default_index]:
                att_index[self.default_index].append(att_content[0])


        # Añadiendo los indices

        for index in att_index:

            # Indices no existentes en POSTGRES ( se usarán solamente en el python, no se insertaran en el postgres.
            if index in ["SEQ", "ISAM"]:
                continue


            index_to_aplicar = index
            for attr in att_index[index]:

                if index == "AVL" or index == "BTREE" or index == "RTREE" or index == "HASH":
                    _ = MainIndex(stmt.name, attr, index, self.conection)
                    continue

                index_name = f"{stmt.name.lower()}_{index.lower()}_{attr.lower()}_idx"

                query = sql.SQL("CREATE INDEX {name} ON {table} USING {idx} ({attribute})").format(
                    name=sql.Identifier(index_name),
                    table=sql.Identifier(stmt.name.lower()),
                    idx=sql.SQL(index_to_aplicar.lower()),
                    attribute=sql.Identifier(attr.lower())
                )

                # Creando cada indice
                cursor.execute(query)


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

        #### Creando los indices ####


        for col in keys:

            to_indexar = [self.default_index]
            if stmt.index_field and col == stmt.index_field and self.default_index != stmt.index_type:
                to_indexar = [stmt.index_type, self.default_index]

                # Insertar el indice en postgres solo si es el declarado en el CSV o ese indice existe en postgres.
                if stmt.index_type in ["BTREE", "HASH"]:
                    index_name = f"csv_{stmt.name.lower()}_{stmt.index_type.lower()}_{stmt.index_field.lower()}_idx"

                    index_query = sql.SQL("CREATE INDEX {name} ON {table} USING {idx} ({attribute})").format(
                        name=sql.Identifier(index_name),
                        table=sql.Identifier(stmt.name.lower()),
                        idx=sql.SQL(stmt.index_type.lower()),
                        attribute=sql.Identifier(stmt.index_field.lower())
                    )

                    cursor.execute(index_query)

            for indices_yo_aply in to_indexar:
                self.bin_manager.add_index_to_attribute(stmt.name, col, indices_yo_aply)




        header = self.bin_manager._reconstruct_header_from_postgres(stmt.name)
        self.bin_manager.save_table(stmt.name, self.db[stmt.name], header=header)

        self.conection.commit()
        cursor.close()

        print(f"\nTabla '{stmt.name}' creada desde archivo con {len(self.db[stmt.name])} filas")

    def visit_create_index(self, stmt):
        table_name = stmt.table.lower()

        if table_name not in self.bin_manager.meta:
            raise ValueError(f"Tabla '{stmt.table}' no existe")

        total_atributos = [col["name"] for col in self.bin_manager.meta[table_name]["columns"]]

        for col in stmt.list_atributos:
            if col.lower() not in total_atributos:
                raise ValueError(f"No existe el atributo: {col} en la tabla {stmt.table}")


        # Aplicar índices nativos de Python ( Quitar el BTREE o HASH si se quiere agregar en postgres )
        if stmt.index_type in ["AVL", "HASH", "SEQ", "ISAM", "BTREE", "RTREE"]:
            for att in stmt.list_atributos:
                if stmt.index_type in ["AVL", "BTREE", "RTREE", "HASH"]:
                    _ = MainIndex(stmt.table, att, stmt.index_type, self.conection)

                #Actualizar el meta.json en la tabla correcta
                table_meta = self.bin_manager.meta.get(table_name)
                if table_meta:
                    for col_meta in table_meta["columns"]:
                        if col_meta["name"] == att.lower():
                            if stmt.index_type.lower() not in col_meta["indexes"]:
                                col_meta["indexes"].append(stmt.index_type.lower())
                    table_meta["last_modified"] = datetime.now().isoformat()
                    self.bin_manager._save_metadata()

            return

        # Para índices PostgreSQL nativos
        cursor = self.conection.cursor()
        index_query = sql.SQL("CREATE INDEX {name} ON {table} USING {idx} ({attributes})").format(
            name=sql.Identifier(stmt.index_name),
            table=sql.Identifier(table_name),
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

    def visit_alter_add_primary_key(self, stmt):
        table_name = stmt.table_name.lower()
        column_name = stmt.column_name.lower()

        # Verificar si la tabla está registrada en el meta.json
        if table_name not in self.bin_manager.meta:
            raise ValueError(f"La tabla '{table_name}' no existe.")

        # Intentar cargar la tabla si aún no está cargada en self.db
        if table_name not in self.db:
            try:
                self.db[table_name] = self.bin_manager.load_table(table_name)
            except Exception as e:
                raise ValueError(f"No se pudo cargar la tabla '{table_name}': {e}")

        # Obtener lista de columnas
        if not self.db[table_name]:
            meta_info = self.bin_manager.meta.get(table_name)
            if not meta_info:
                raise ValueError(f"No se pudo obtener información de la tabla '{table_name}'")
            columnas = [col["name"] for col in meta_info["columns"]]
        else:
            columnas = list(self.db[table_name][0].keys())

        if column_name not in columnas:
            raise ValueError(f"La columna '{column_name}' no existe en la tabla '{table_name}'")

        cursor = self.conection.cursor()

        # Verificar si ya existe una primary key en PostgreSQL
        cursor.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary;
        """, (table_name,))
        existing_pk = cursor.fetchall()

        if existing_pk:
            raise ValueError(f"La tabla '{table_name}' ya tiene una clave primaria definida: {existing_pk[0][0]}")

        # Verificar si ya existe en el meta.json una columna marcada como primary_key
        meta_info = self.bin_manager.meta.get(table_name)
        for col in meta_info.get("columns", []):
            if col.get("primary_key", False):
                raise ValueError(
                    f"La tabla '{table_name}' ya tiene una clave primaria definida en el meta.json: {col['name']}")

        # 🔧 Ejecutar ALTER TABLE en PostgreSQL
        try:
            query = sql.SQL("ALTER TABLE {table} ADD PRIMARY KEY ({column});").format(
                table=sql.Identifier(table_name),
                column=sql.Identifier(column_name)
            )
            cursor.execute(query)
            self.conection.commit()
            print(f"\nSe ha agregado PRIMARY KEY sobre la columna '{column_name}' en la tabla '{table_name}'")

            # Actualizar el meta.json
            for col in meta_info["columns"]:
                if col["name"] == column_name:
                    col["primary_key"] = True
            meta_info["last_modified"] = datetime.now().isoformat()
            self.bin_manager._save_metadata()

        except Exception as e:
            self.conection.rollback()
            raise ValueError(f"Error al ejecutar ALTER TABLE: {e}")
        finally:
            cursor.close()

    def eval_condition(self, exp, table_name=None):

        # Si por alguna razón entra, se considera.
        if exp is None:
            return True

        return self.visit(exp, table_name)

    def visit(self, node, table_name=None):

        result = 0

        match node:


            case IdExp():

                atts_of_table = self.bin_manager.get_atts_table(table_name)

                if node.name.lower() not in atts_of_table:
                    raise ValueError(f"Error, el atributo {node.name}, no pertenece a la tabla {table_name}")

                result = node.name.lower()


            case NumberExp():
                result = node.value
            case BoolExp():
                result = node.boolean
            case StringExp():
                result = node.value
            case BinaryExp():

                v1 = self.visit(node.left)
                v2 = self.visit(node.right)
                op = node.op

                match op:

                    # Operaciones matemáticas
                    case BinaryOp.PLUS_OP:
                        result = v1 + v2
                    case BinaryOp.MINUS_OP:
                        result = v1 - v2
                    case BinaryOp.MUL_OP:
                        result = v1 * v2
                    case BinaryOp.DIV_OP:
                        if v2 == 0:
                            raise ValueError("No se puede dividir entre 0")
                        result = v1 / v2

                    # Comparaciones
                    case BinaryOp.EQUAL_OP:
                        result = int(v1 == v2)
                    case BinaryOp.LESS_OP:
                        result = int(v1 < v2)
                    case BinaryOp.EQLESS_OP:
                        result = int(v1 <= v2)
                    case BinaryOp.MAYOR_OP:
                        result = int(v1 > v2)
                    case BinaryOp.EQMAYOR_OP:
                        result = int(v1 >= v2)
                    case BinaryOp.NOTEQUAL_OP:
                        result = int(v1 != v2)

                    # AND OR NOT
                    case BinaryOp.AND_OP:
                        result = int(v1 and v2)
                    case BinaryOp.OR_OP:
                        result = int(v1 or v2)
                    case BinaryOp.NOT_OP:
                        result = int(not v2)

            case BetweenExp():

                att_query = self.visit(node.atribute, table_name)

                indexs_to_att = self.bin_manager.get_indexs_att(table_name, att_query)

                v1 = self.visit(node.left, table_name)
                v2 = self.visit(node.right, table_name)

                for index in indexs_to_att:
                    index_query = MainIndex(table_name, att_query, index, self.conection)

                    result = index_query.range_search(v1, v2)
                    if result:
                        break


        return result

## Tener mucho cuidado con el indices, no todos estan disponibles en postgres.
