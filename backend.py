from flask import Flask, request, jsonify
from exec_cv import ExecuteSQLParser  # Ajusta la importación según tu proyecto

app = Flask(__name__)

def validar_identificador(name):
    return name.isidentifier()


# "ejemplo http://localhost:5000/table/users"
@app.route('/table', methods=['POST'])
def get_all(table_name, atributo):
    try:
        data = request.get_json()
        
        executor = ExecuteSQLParser(data)
        

        if not executor:
            return jsonify({"message": "No records found"}), 404

        return jsonify(executor), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



# "ejemplo http://localhost:5000/table/users"
@app.route('/table/<string:table_name>', methods=['GET'])
def get_all(table_name, atributo):
    if not validar_identificador(table_name) or not validar_identificador(atributo):
        return jsonify({"error": "Invalid table name or attribute"}), 400

    try:
        query = f"SELECT * FROM {table_name}"
        executor = ExecuteSQLParser(query)
        

        if not executor:
            return jsonify({"message": "No records found"}), 404

        return jsonify(executor), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



# "ejemplo http://localhost:5000/table/users/name?valor=Juan"
@app.route('/table/<string:table_name>/<string:atributo>', methods=['GET'])
def get_by_atributo(table_name, atributo):
    if not validar_identificador(table_name) or not validar_identificador(atributo):
        return jsonify({"error": "Invalid table name or attribute"}), 400

    valor = request.args.get("valor")
    if valor is None:
        return jsonify({"error": "Missing 'valor' query parameter"}), 400

    try:
        query = f"SELECT * FROM {table_name} WHERE {atributo} = '{valor}'"
        executor = ExecuteSQLParser(query)
        
        if not executor:
            return jsonify({"message": "No records found"}), 404

        return jsonify(executor), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# GET /table/users/age/range?id_start=20&id_end=30
@app.route('/table/<string:table_name>/<string:atributo>/range', methods=['GET'])
def get_search_range(table_name, atributo):
    if not validar_identificador(table_name) or not validar_identificador(atributo):
        return jsonify({"error": "Invalid table name or attribute"}), 400

    id_start = request.args.get("id_start")
    id_end = request.args.get("id_end")

    if id_start is None or id_end is None:
        return jsonify({"error": "Missing 'id_start' or 'id_end' query parameters"}), 400


    try:

        query = f"SELECT * FROM {table_name} WHERE {atributo} BETWEEN {id_start} AND {id_end}"
        executor = ExecuteSQLParser(query)
    
        if not query:
            return jsonify({"message": "No records found"}), 404


        return jsonify(executor), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# POST /table/users
@app.route('/table/<string:table_name>', methods=['POST'])
def insert_table(table_name):
    if not validar_identificador(table_name):
        return jsonify({"error": "Invalid table name"}), 400

    data = request.get_json()
    if not data or not isinstance(data, dict):
        return jsonify({"error": "Invalid or missing JSON body"}), 400

    try:
        columns = ', '.join(data.keys())
        values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in data.values()])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values}) RETURNING *"

        executor = ExecuteSQLParser(query)
        

        if not executor:
            return jsonify({"message": "Insert failed"}), 500

        return jsonify({"message": "Record inserted", "record": executor}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# DELETE /table/users/id?valor=5
@app.route('/table/<string:table_name>/<string:id>', methods=['DELETE'])
def delete_by_atributo(table_name, id):
    if not validar_identificador(table_name):
        return jsonify({"error": "Invalid table name"}), 400

    valor = request.args.get("valor")
    if valor is None:
        return jsonify({"error": "Missing 'valor' query parameter"}), 400

    try:
       
        query = f"DELETE FROM {table_name} WHERE {id} = {valor} "
        executor = ExecuteSQLParser(query)
        
        if not executor:
            return jsonify({"message": f"No record with {id} {valor} found"}), 404

        return jsonify({"message": "Record deleted", "deleted": id}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
