from flask import Flask, request

app = Flask(__name__)

@app.route("/print", methods=["POST"])
def print_body():
    app.logger.debug("Otrzymano body: %s", request.get_json())
    return {"status": "ok"}, 200