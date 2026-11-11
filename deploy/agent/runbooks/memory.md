СЕМЕЙСТВО: память (memory:web:*, memory:bot:*, *:oom).

Ходы:
1. docker stats --no-stream — живое потребление всех контейнеров.
2. free -h — хост целиком.
3. docker logs keycrm-web --since 30m | grep -iE "memory|oom" | tail -20
   (строки «Memory OK/WARN/CRITICAL» пишет монитор каждые 30 мин; у бота —
   «Bot memory OK» в логах keycrm-bot).
4. Если *:oom — dmesg недоступен; смотри docker logs убитого контейнера на
   момент рестарта и /api/health uptime_seconds (маленький = недавний OOM).
Знание: working set = memory.current МИНУС page cache — кэш ядро отдаст само,
он не давление. Лимиты: web 7g, bot 512m. DUCKDB_MEMORY_LIMIT в compose
читает только образ до шага 14 (цель отката): web шага 14 DuckDB не грузит,
так что память web — это Python, pandas прогноза и пул Postgres. Счётчик
oom_kills ядра сбрасывается при пересоздании контейнера — мониторы сравнивают
с СОХРАНЁННЫМ сэмплом. Рычаг человека: поднять лимит web или найти, что его
съело (прогноз, выгрузка, запрос без LIMIT); рестарт лечит симптом на часы и
прячет причину.
