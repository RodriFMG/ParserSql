from token import Token, Type

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

        # operadores de dos caracteres
        if self.code[self.current:self.current + 2] == "<=":
            self.current += 2
            return Token(Type.LE, "<=")
        elif self.code[self.current:self.current + 2] == ">=":
            self.current += 2
            return Token(Type.GE, ">=")
        elif self.code[self.current:self.current + 2] == "<>":
            self.current += 2
            return Token(Type.NEQ, "<>")

        c = self.code[self.current]
        self.first = self.current

        # palabras clave o identificador
        if c.isalpha():
            self.current += 1
            while self.current < len(self.code) and (self.code[self.current].isalnum() or self.code[self.current] == '_'):
                self.current += 1
            word = self.code[self.first:self.current].upper()

            keyword_map = {
                "SELECT": Type.SELECT,
                "FROM": Type.FROM,
                "WHERE": Type.WHERE,
                "BETWEEN": Type.BETWEEN,
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
            }

            if word in keyword_map:
                return Token(keyword_map[word], word)
            else:
                return Token(Type.ID, word)

        # numeros
        elif c.isdigit():
            self.current += 1
            while self.current < len(self.code) and self.code[self.current].isdigit():
                self.current += 1
            return Token(Type.NUMBER, self.code[self.first:self.current])

        # string entre comillas dobles
        elif c == '"':
            self.current += 1  # saltar comilla inicial
            start = self.current
            while self.current < len(self.code) and self.code[self.current] != '"':
                self.current += 1
            text = self.code[start:self.current]
            self.current += 1  # saltar comilla final
            return Token(Type.STRING, text)

        # símbolos de un carácter
        symbol_map = {
            '+': Type.PLUS,
            '-': Type.MINUS,
            '*': Type.MUL,
            '/': Type.DIV,
            '=': Type.EQ,
            '<': Type.LT,
            '>': Type.GT,
            ',': Type.COMA,
            ';': Type.SEMICOLON,
            '(': Type.LPAREN,
            ')': Type.RPAREN,
            '[': Type.LBRACKET,
            ']': Type.RBRACKET,
        }

        if c in symbol_map:
            self.current += 1
            return Token(symbol_map[c], c)

        # si no se reconoce el carácter
        self.current += 1
        return Token(Type.ERR, c)


