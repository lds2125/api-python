import oracledb
from flask import Flask, jsonify, request, abort


app = Flask(__name__)

def get_conexao():
    return oracledb.connect( user="rm559004", password="fiap24", dsn="oracle.fiap.com.br/orcl")

@app.route('/users/', methods=['POST'])
def create_user():
    data = request.get_json()
    if not data or 'name' not in data or 'email' not in data:
        abort(400)
    
    with get_conexao() as conn:
        with conn.cursor() as cur:
            sql = "INSERT INTO users (name, email, age) VALUES (:1, :2, :3)"
            cur.execute(sql, (data['name'], data['email'], data.get('age', None)))
            conn.commit()
    
    return jsonify({"name": data['name'], "email": data['email'], "age": data.get('age')}), 201

@app.route('/users/', methods=['GET'])
def get_users():
    with get_conexao() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, email, age FROM users")
            rows = cur.fetchall()
            users = [{"id": row[0], "name": row[1], "email": row[2], "age": row[3]} for row in rows]
    
    return jsonify(users)

@app.route('/users/<int:user_id>', methods=['GET'])
def get_user(user_id):
    with get_conexao() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, email, age FROM users WHERE id = :1", (user_id,))
            row = cur.fetchone()
            if row is None:
                abort(404)
            return jsonify({"id": row[0], "name": row[1], "email": row[2], "age": row[3]})

@app.route('/users/<int:user_id>', methods=['PUT'])
def update_user(user_id):
    data = request.get_json()
    if not data or 'name' not in data or 'email' not in data:
        abort(400)

    with get_conexao() as conn:
        with conn.cursor() as cur:
            sql = "UPDATE users SET name = :1, email = :2, age = :3 WHERE id = :4"
            cur.execute(sql, (data['name'], data['email'], data.get('age', None), user_id))
            conn.commit()
            if cur.rowcount == 0:
                abort(404)
    
    return jsonify({"id": user_id, "name": data['name'], "email": data['email'], "age": data.get('age')})

@app.route('/users/<int:user_id>', methods=['DELETE'])
def delete_user(user_id):
    with get_conexao() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE id = :1", (user_id,))
            conn.commit()
            if cur.rowcount == 0:
                abort(404)
    
    return jsonify({"detail": "User deleted"}), 204

if __name__ == '__main__':
    app.run(debug=True)
