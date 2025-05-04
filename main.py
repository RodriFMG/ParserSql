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

if __name__ == "__main__":

    with open("news_es.csv", encoding="utf-8") as f:
        code = ('CREATE TABLE newTable(x INT PRIMARY KEY, y BOOLEAN INDEX RTREE, z ARRAY[INT]);'
                'CREATE TABLE NOTICIAS FROM FILE "news_es.csv" USING INDEX HASH("url"); '
                'SELECT categoria FROM NOTICIAS WHERE categoria BETWEEN "A" AND "N";'
                'INSERT INTO newTable (x, y) VALUES (not 1>=2 and  not 0 <= 2, 0), (2, 3), (3, 4)')
        ver_tokens(code)

    scanner = Scanner(code)
    parser = ParserSQL(scanner)
    program = parser.ParseProgram()

    # Este diccionario lo tenemos que alimentar previo con toda la data que hay actualmente
    db = {}
    executor = VisitorExecutor(db)
    program.accept(executor)

