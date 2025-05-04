import csv
from Objects import IdExp, BoolExp, NumberExp, BinaryExp, StringExp, BetweenExp
from Constantes import BinaryOp


class VisitorExecutor:
    def __init__(self, db):
        self.db = db

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

    def visit_insert(self, stmt):

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

        # Insertar todas las filas.
        for RowToInsert in stmt.values:
            new_row = {att: None for att in TablaAtributos}
            for attName, content in zip(stmt.atributos, RowToInsert):
                new_row[attName.lower()] = self.eval_condition(content, RowToInsert)

            self.db[table_name].append(new_row)

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

        # Que se guarde el nombre de las tablas en minúsculas.
        columnas = {col[0].lower(): None for col in stmt.columns}

        # Aun falta incializarlos con sus tipos correspondientes.
        self.db[stmt.name] = [columnas.copy()]  # inicializacion vacía con encabezados
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
