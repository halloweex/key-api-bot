СЕМЕЙСТВО: прочее — условие без специального ранбука.

Ходы:
1. psql: SELECT * FROM app.alert_series WHERE state='firing' ORDER BY updated_at DESC LIMIT 10;
2. psql: SELECT condition_key, event_type, at, delivered_to FROM app.alert_events ORDER BY at DESC LIMIT 15;
3. docker logs keycrm-web --since 30m | grep -iE "error|warning|critical" | tail -30
4. curl /api/health целиком.
Действуй по общим правилам; если данных мало — так и скажи, гипотеза лучше молчания, но помечай её гипотезой.
