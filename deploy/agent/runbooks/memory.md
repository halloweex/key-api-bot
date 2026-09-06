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
он не давление; на этом хосте кэш растёт с размером DuckDB-файла. Лимиты:
web 7g (внутри DUCKDB_MEMORY_LIMIT=4g — полный rebuild склада упирается в
него; 3g в августе дало OOM-шторм и обрезанный Gold), bot 512m. Счётчик
oom_kills ядра сбрасывается при пересоздании контейнера — мониторы сравнивают
с СОХРАНЁННЫМ сэмплом. Рычаг человека: поднять лимит или DUCKDB_MEMORY_LIMIT;
рестарт лечит симптом на часы и прячет причину.
