from Token import Token, Type

def is_space_white(c: str):
    return c in [' ', '\n', '\t', '\r']

class Scanner:
    def __init__(self, code: str):
        self.code = code
        self.first = 0
        self.current = 0

    def next_token(self):
        while self.current < len(self.code) and is_space_white(self.code[self.current]):
            self.current += 1
        if self.current >= len(self.code):
            return Token(Type.EOF)

        #Operadores dobles (<=, >=, <>)
        if self.code[self.current:self.current + 2] == "<=":
            self.current += 2
            return Token(Type.EQLESS, "<=")
        elif self.code[self.current:self.current + 2] == ">=":
            self.current += 2
            return Token(Type.EQMAYOR, ">=")
        elif self.code[self.current:self.current + 2] == "<>":
            self.current += 2
            return Token(Type.NEQ, "<>")

        c = self.code[self.current]
        self.first = self.current
        self.current += 1

        #Palabras clave y booleanos
        if c.isalpha():
            while self.current < len(self.code) and self.code[self.current].isalnum():
                self.current += 1
            word = self.code[self.first:self.current].upper()

            keyword_map = {
                "SELECT": Type.SELECT,
                "FROM": Type.FROM,
                "WHERE": Type.WHERE,
                "BETWEEN": Type.BETWEEN,
                "SERIAL": Type.SERIAL,
                "INSERT": Type.INSERT,
                "INTO": Type.INTO,
                "VALUES": Type.VALUES,
                "DELETE": Type.DELETE,
                "CREATE": Type.CREATE,
                "TABLE": Type.TABLE,
                "FILE": Type.FILE,
                "USING": Type.USING,
                "INDEX": Type.INDEX,
                "PRIMARY": Type.PRIMARY,
                "KEY": Type.KEY,
                "INT": Type.INT,
                "TEXT": Type.TEXT,
                "DATE": Type.DATE,
                "ARRAY": Type.ARRAY,
                "FLOAT": Type.FLOAT,
                "TRUE": Type.TRUE,
                "FALSE": Type.FALSE,
                "AND": Type.AND,
                "OR": Type.OR,
                "NOT": Type.NOT,
                'BOOLEAN': Type.BOOLEAN,
                'HASH': Type.HASH,
                'BTREE': Type.BTREE,
                "RTREE": Type.RTREE,
                "SEQ": Type.SEQ,
                "AVL": Type.AVL,
                "ON": Type.ON,
                "DROP": Type.DROP,
                "ISAM": Type.ISAM,
                "ALTER": Type.ALTER,
                "ADD": Type.ADD
            }

            if word in keyword_map:
                return Token(keyword_map[word], word)
            else:
                return Token(Type.ID, word)

        #Numeros
        elif c.isdigit():
            while self.current < len(self.code) and self.code[self.current].isdigit():
                self.current += 1

            if self.code[self.current] == '.':
                self.current += 1
                while self.current < len(self.code) and self.code[self.current].isdigit():
                    self.current += 1

                return Token(Type.FLOAT, self.code[self.first:self.current])

            else:
                return Token(Type.NUMBER, self.code[self.first:self.current])

        # Strings con comillas simples o dobles
        elif c == '"' or c == "'":
            quote_type = c  # guarda si fue " o '
            start = self.current
            while self.current < len(self.code) and self.code[self.current] != quote_type:
                self.current += 1
            text = self.code[start:self.current]
            self.current += 1  # Saltar la comilla de cierre
            return Token(Type.STRING, text)

        #Simbolos
        symbol_map = {
            '*': Type.STAR,
            '=': Type.EQ,
            ',': Type.COMA,
            ';': Type.SEMICOLON,
            '(': Type.LPAREN,
            ')': Type.RPAREN,
            '[': Type.LBRACKET,
            ']': Type.RBRACKET,
            '+': Type.PLUS,
            '-': Type.MINUS,
            '/': Type.DIV,
            '<': Type.LESS,
            '>': Type.MAYOR
        }

        if c in symbol_map:
            return Token(symbol_map[c], c)

        return Token(Type.ERR, c)
