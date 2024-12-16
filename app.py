from flask import Flask, jsonify
import subprocess
import sys

app = Flask(__name__)

@app.route('/run', methods=['GET'])
def run():
    """Run file.py and parser.py sequentially."""
    try:
        # Run file.py
        # file_result = subprocess.run(['python', 'file2.py'], capture_output=True, text=True)
        file_result = subprocess.run([sys.executable, 'file2.py'], capture_output=True, text=True)
        if file_result.returncode != 0:
            return jsonify({
                "status": "error",
                "step": "file.py",
                "error": file_result.stderr
            }), 500

        # Run parser.py
        # parser_result = subprocess.run(['python', 'parser.py'], capture_output=True, text=True)
        parser_result = subprocess.run([sys.executable, 'parser.py'], capture_output=True, text=True)
        if parser_result.returncode != 0:
            return jsonify({
                "status": "error",
                "step": "parser.py",
                "error": parser_result.stderr
            }), 500

        return jsonify({
            "status": "success",
            "message": "Both file.py and parser.py executed successfully.",
            "file_output": file_result.stdout,
            "parser_output": parser_result.stdout
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": "An unexpected error occurred.",
            "error": str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True)
