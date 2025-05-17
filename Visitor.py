import csv
from Objects import IdExp, BoolExp, NumberExp, BinaryExp, StringExp, BetweenExp
from Constantes import BinaryOp
from psycopg2 import sql
from Token import Type


class VisitorExecutor:
    def __init__(self, db, conn):
        self.db = db
        self.conection = conn

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

        for row in rows:
            if stmt.condition:
                if not self.eval_condition(stmt.condition, row):
                    continue
            if stmt.atributos == "*":
                selected_rows.append(row)
            else:

                # Extrae todas las filas de tu tabla.
                lower_row = {k.lower(): v for k, v in row.items()}

                # De cada atributo presente en tu fila, rescatas los valores de los atributos
                # deseados
                selected_rows.append({attr: lower_row.get(attr.lower(), None) for attr in stmt.atributos})

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
        TablaAtributos = table_data[0].keys()

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
        last_id = self.db[table_name][-1]['id']


        # Insertar todas las filas.
        for RowToInsert in stmt.values:

            # Ejecutar en SQL para guardarlo en la BDD
            RowInsert = [self.eval_condition(exp, RowToInsert) for exp in RowToInsert]
            cursor.execute(query, RowInsert)

            if 'id' not in stmt.atributos:
                last_id += 1

            # Guardado temporal en el diccionario usado
            new_row = {att: None for att in TablaAtributos}
            for attName, content in zip(stmt.atributos, RowInsert):
                if 'id' not in stmt.atributos:
                    new_row['id'] = last_id
                new_row[attName.lower()] = content

            print(new_row)
            self.db[table_name].append(new_row)

        # Guardar los cambios realizados
        self.conection.commit()
        cursor.close()

        print(f"\nInserción realizada con éxito, se insertaron {len(stmt.values)} a {table_name}")

    def visit_delete(self, stmt):
        if stmt.table not in self.db:
            raise ValueError(f"Tabla '{stmt.table}' no encontrada")

        original = len(self.db[stmt.table])

        # Construimos nuevamente la tabla pero ahora solo con las que no cumplan la condición.
        self.db[stmt.table] = [row for row in self.db[stmt.table]
                               if not self.eval_condition(stmt.condition, row)]

        # Número de filas eliminadas.
        deleted = original - len(self.db[stmt.table])

        print(f"\nFilas eliminadas: {deleted}")

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

        is_array = lambda type_att: f"ARRAY[{type_att[1].name}]" if isinstance(type_att, list) else type_att.name
        is_pk = lambda pk: " PRIMARY KEY" if pk else ""

        columns = [att + " " + is_array(type_att) + is_pk(pk)
                   for att, type_att, pk in atribute_and_type]

        # Construimos la consulta
        query = sql.SQL("CREATE TABLE {table} ({atributes})").format(
            table=sql.Identifier(stmt.name.lower()),
            atributes=sql.SQL(', ').join(sql.SQL(col) for col in columns)
        )

        # Ejecutamos la consulta
        cursor.execute(query)

        # Que se guarde el nombre de las tablas en minúsculas.
        columnas = {col[0].lower(): None for col in stmt.columns}

        # inicializacion vacía con encabezados
        self.db[stmt.name] = [columnas.copy()]

        # Guardamos los cambios
        self.conection.commit()
        cursor.close()

        print(f"\nTabla '{stmt.name}' creada con columnas: {list(columnas.keys())}")

    def visit_create_from_file(self, stmt):
        if stmt.name in self.db:
            raise ValueError(f"Tabla '{stmt.name}' ya existe")

        with open(stmt.file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            self.db[stmt.name] = [row for row in reader]

        print(f"\nTabla '{stmt.name}' creada desde archivo con {len(self.db[stmt.name])} filas")

    # Para que el row? xd

    # No soporta operaciones binarias
    def eval_condition(self, exp, row=None):

        # Si por alguna razón entra, se considera.
        if exp is None:
            return True

        return self.visit(exp, row)

    def visit(self, node, row=None):

        result = 0

        match node:
            case IdExp():

                if row:
                    result = row.get(node.name.lower())
                else:
                    result = node.name.lower()

            case NumberExp():
                result = node.value
            case BoolExp():
                result = node.boolean
            case StringExp():
                result = node.value
            case BinaryExp():

                v1 = self.visit(node.left, row)
                v2 = self.visit(node.right, row)
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
                atributo = self.visit(node.atribute, row)
                v1 = self.visit(node.left, row)
                v2 = self.visit(node.right, row)

                result = int(v1 <= atributo <= v2)

        return result
