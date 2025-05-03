import self

from Scanner import Scanner
from Parser import ParserSQL
from Visitor import VisitorExecutor

def ver_tokens(code):
    scanner = Scanner(code)
    while True:
        token = scanner.next_token()
        print(token)  # Usará __repr__
        if token.type.name == "EOF":
            break

with open("news_es.csv", encoding="utf-8") as f:

    code = 'CREATE TABLE NOTICIAS FROM FILE "news_es.csv" USING INDEX HASH("url"); SELECT categoria FROM NOTICIAS WHERE categoria = "Otra";'
    ver_tokens(code)


scanner = Scanner(code)
parser = ParserSQL(scanner)
program = parser.ParseProgram()

db = {}
executor = VisitorExecutor(db)
program.accept(executor)

