import json
import sqlite3
from http.server import BaseHTTPRequestHandler, HTTPServer

from scripts.run_sessions_sequencer import fill_profiles, profiles
from scripts.run_sessions_typifier import typify_sessions
from stock_market_research_kit.day import day_from_json
from stock_market_research_kit.session import session_from_json, json_from_sessions

DATABASE_PATH = "stock_market_research_2025.db"
conn = sqlite3.connect(DATABASE_PATH)
c = conn.cursor()


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        days_split_parts = self.path.split("/api/days/")
        if len(days_split_parts) == 2 and days_split_parts[0] == "":
            symbol = days_split_parts[1]
            # sql injections, welcom =)
            c.execute("""SELECT data FROM days WHERE symbol = ?""", (symbol,))
            days_rows = c.fetchall()

            if len(days_rows) == 0:
                self.send_response(404)
                self.end_headers()
                print(f"Symbol {symbol} not found in days table")
                return

            days = [json.loads(x[0]) for x in days_rows]


            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(days, indent=4).encode())
            return

        sessions_split_parts = self.path.split("/api/sessions/")
        if len(sessions_split_parts) == 2 and sessions_split_parts[0] == "":
            symbol = sessions_split_parts[1]
            # sql injections, welcom =)
            c.execute("""SELECT data FROM days WHERE symbol = ?""", (symbol,))
            days_rows = c.fetchall()

            if len(days_rows) == 0:
                self.send_response(404)
                self.end_headers()
                print(f"Symbol {symbol} not found in days table")
                return

            sessions = typify_sessions([day_from_json(x[0]) for x in days_rows])

            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json_from_sessions(sessions).encode())
            return

        profiles_split_parts = self.path.split("/api/profiles/")
        if len(profiles_split_parts) == 2 and profiles_split_parts[0] == "":
            symbol = profiles_split_parts[1]

            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(profiles, indent=4).encode())
            return

        self.send_response(400)
        self.end_headers()
        print(f"Bad request to {self.path}")
        return


if __name__ == '__main__':
    try:
        fill_profiles("BTCUSDT", 2024)
        server = HTTPServer(('localhost', 8000), RequestHandler)
        print('Starting server at http://localhost:8000')
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
