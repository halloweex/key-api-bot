СЕМЕЙСТВО: доступность (health_*, cert_*, alerting_*).

Ходы:
1. curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" https://ksanalytics.duckdns.org/api/health
2. curl -s http://localhost:8080/api/health | head -c 2000  (мимо nginx — отделить приложение от прокси/TLS)
3. docker ps — живы ли keycrm-web, keycrm-nginx; docker logs keycrm-nginx --tail 20
4. docker logs keycrm-web --since 15m | tail -40
5. Если cert_*: echo | openssl s_client -servername ksanalytics.duckdns.org -connect ksanalytics.duckdns.org:443 2>/dev/null | openssl x509 -noout -dates
Знание: расхождение «localhost отвечает, домен нет» = nginx/TLS/DNS, не приложение. Рестарт web — последний рычаг, не первый. Сертификат продлевает certbot на хосте.
Знание: status=degraded при degraded_by=["duckdb_fatal"] — DuckDB получил FATAL; инстанс сброшен и переоткрыт, чтения отвечают, web не лежит. kinds.other — degraded держится до рестарта web. duckdb.fatal.index_short — индекс в файле короткий (последствие убитого писателя до duckdb_switch.open_file): та же запись падает снова, рестарт не лечит и статус не снимает — запись лежит рядом с файлом (data/.duckdb_index_short.json, по inode файла) и гаснет, только когда компакт подменит файл; рычаг — пересборка индексов, то есть воскресный компакт или scripts/weekly_compact.sh руками. Восстановление копированием поверх файла inode не меняет — тогда запись удалить руками. Улики — строки `DuckDB FATAL` в логе web (ход 4 с `--since 24h`).
