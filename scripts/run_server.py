import json
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, HTTPServer

from scripts.run_sessions_sequencer import fill_profiles
from scripts.run_sessions_typifier import typify_sessions
from stock_market_research_kit.db_layer import select_days
from stock_market_research_kit.session import json_from_sessions
from stock_market_research_kit.session_quantiles import quantile_session_year_thr
from stock_market_research_kit.session_thresholds import btc_universal_threshold


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        days_split_parts = self.path.split("/api/days/")
        if len(days_split_parts) == 2 and days_split_parts[0] == "":
            symbol = days_split_parts[1]
            # sql injections, welcom =)

            days = select_days(2024, symbol)
            if len(days) == 0:
                self.send_response(404)
                self.end_headers()
                print(f"Symbol {symbol} not found in days table")
                return

            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps([asdict(x) for x in days], indent=4).encode())
            return

        sessions_split_parts = self.path.split("/api/sessions/")
        if len(sessions_split_parts) == 2 and sessions_split_parts[0] == "":
            symbol = sessions_split_parts[1]
            # sql injections, welcom =)
            days = select_days(2024, symbol)
            if len(days) == 0:
                self.send_response(404)
                self.end_headers()
                print(f"Symbol {symbol} not found in days table")
                return

            sessions = typify_sessions(days, lambda session_name, _: symbol_thresholds[session_name])

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
        symbol_thresholds = quantile_session_year_thr("CRVUSDT", 2024)
        _, _, profiles = fill_profiles(
            "CRVUSDT", 2024, lambda session_name, _: symbol_thresholds[session_name])
        server = HTTPServer(('localhost', 8000), RequestHandler)
        print('Starting server at http://localhost:8000')
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
