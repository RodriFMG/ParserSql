import csv

class VisitorExecutor:
    def __init__(self, db):
        self.db = db

    def visit_select(self, stmt):
        table_name = stmt.table
        if table_name not in self.db:
            raise ValueError(f"Tabla '{table_name}' no encontrada")

        rows = self.db[table_name]
        selected_rows = []

        for row in rows:
            if stmt.condition:
                if not self.eval_condition(stmt.condition, row):
                    continue
            if stmt.atributos == "*":
                selected_rows.append(row)
            else:
                lower_row = {k.lower(): v for k, v in row.items()}
                selected_rows.append({attr: lower_row.get(attr.lower(), None) for attr in stmt.atributos})

        print("\nResultado del SELECT:")
        for r in selected_rows:
            print(r)

    def visit_insert(self, stmt):
        if stmt.table not in self.db:
            raise ValueError(f"Tabla '{stmt.table}' no encontrada")

        table_data = self.db[stmt.table]
        if len(stmt.values) != len(table_data[0]):
            raise ValueError("Cantidad de valores no coincide con columnas")

        new_row = {k: v for k, v in zip(table_data[0].keys(), stmt.values)}
        self.db[stmt.table].append(new_row)
        print("\nInserción realizada con éxito:", new_row)

    def visit_delete(self, stmt):
        if stmt.table not in self.db:
            raise ValueError(f"Tabla '{stmt.table}' no encontrada")

        original = len(self.db[stmt.table])
        self.db[stmt.table] = [r for r in self.db[stmt.table] if not self.eval_condition(stmt.condition, r)]
        deleted = original - len(self.db[stmt.table])
        print(f"\nFilas eliminadas: {deleted}")

    def visit_create(self, stmt):
        if stmt.name in self.db:
            raise ValueError(f"Tabla '{stmt.name}' ya existe")

        columnas = {col[0]: None for col in stmt.columns}
        self.db[stmt.name] = [columnas.copy()]  # inicializacion vacía con encabezados
        print(f"\nTabla '{stmt.name}' creada con columnas: {list(columnas.keys())}")

    def visit_create_from_file(self, stmt):
        if stmt.name in self.db:
            raise ValueError(f"Tabla '{stmt.name}' ya existe")

        with open(stmt.file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            self.db[stmt.name] = [row for row in reader]

        print(f"\nTabla '{stmt.name}' creada desde archivo con {len(self.db[stmt.name])} filas")

    def eval_condition(self, exp, row):
        if exp is None:
            return True

        if hasattr(exp, 'op'):
            if exp.op == 'AND':
                return self.eval_condition(exp.left, row) and self.eval_condition(exp.right, row)
            elif exp.op == 'OR':
                return self.eval_condition(exp.left, row) or self.eval_condition(exp.right, row)
            elif exp.op == 'NOT':
                return not self.eval_condition(exp.right, row)
            else:
                left = self.eval_condition(exp.left, row)
                right = self.eval_condition(exp.right, row)
                op = exp.op.value[0]
                if op == '=':
                    if left is None or right is None:
                        return False
                    return str(left).strip().lower() == str(right).strip().lower()
                elif op == '<':
                    return float(left) < float(right)
                elif op == '>':
                    return float(left) > float(right)
                elif op == '<=':
                    return float(left) <= float(right)
                elif op == '>=':
                    return float(left) >= float(right)
                elif op == '<>':
                    return str(left) != str(right)

        # Identificadores (nombre de columnas)
        if hasattr(exp, 'name'):
            return row.get(exp.name.lower())

        # Literales (numeros, strings)
        if hasattr(exp, 'value'):
            return exp.value

        # Booleanos
        if hasattr(exp, 'boolean'):
            return exp.boolean

        raise ValueError("Expresión no soportada")

