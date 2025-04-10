include .env

.PHONY: print_chat_ids
.SILENT: print_chat_ids
print_chat_ids:
	#curl -s "https://api.telegram.org/bot$(TG_BOT_TOKEN)/getUpdates" | jq '.result' | jq 'map(select(.my_chat_member))' | jq 'map( { (.my_chat_member.chat.title): (.my_chat_member.chat.id) } )'
	curl -s "https://api.telegram.org/bot$(TG_BOT_TOKEN)/getUpdates" | jq '.result'

.PHONY: notify_test
.SILENT: notify_test
notify_test:
	curl -X POST \
      	 -H 'Content-Type: application/json' \
         -d '{"chat_id": "$(SESSIONS_STAT_CHANNEL_ID)", "text": "test message", "disable_notification": true}' \
         https://api.telegram.org/bot$(TG_BOT_TOKEN)/sendMessage

.PHONY: recalculate_db
.SILENT: recalculate_db
recalculate_db:
	python -m scripts.setup_db \
		&& python -m scripts.run_series_raw_loader \
		&& python -m scripts.run_day_markuper


.PHONY: show_cycles
.SILENT: show_cycles
show_cycles:
	pydeps ./scripts/run_sessions_backtester.py --show-cycles
