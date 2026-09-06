СЕМЕЙСТВО: диск (disk:*).

Ходы:
1. df -h /
2. du -xd1 /opt/key-api-bot/data | sort -h | tail -10
3. du -xd1 /var/lib/docker | sort -h | tail -5  (только чтение размеров)
4. docker ps --size | tail -8
5. psql: SELECT pg_size_pretty(pg_database_size('ks'));
Знание: известный пожиратель — WAL/файлы рядом с analytics.duckdb (27 ГБ в августе); недельный компакт (вс 02:00 UTC) — единственная автоуборка, он ОСТАНАВЛИВАЕТ web и bot, его нельзя дёргать автоматикой. Ротация docker-логов 5×50 МБ на контейнер.
