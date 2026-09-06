СЕМЕЙСТВО: доступность (health_*, cert_*, alerting_*).

Ходы:
1. curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" https://ksanalytics.duckdns.org/api/health
2. curl -s http://localhost:8080/api/health | head -c 2000  (мимо nginx — отделить приложение от прокси/TLS)
3. docker ps — живы ли keycrm-web, keycrm-nginx; docker logs keycrm-nginx --tail 20
4. docker logs keycrm-web --since 15m | tail -40
5. Если cert_*: echo | openssl s_client -servername ksanalytics.duckdns.org -connect ksanalytics.duckdns.org:443 2>/dev/null | openssl x509 -noout -dates
Знание: расхождение «localhost отвечает, домен нет» = nginx/TLS/DNS, не приложение. Рестарт web — последний рычаг, не первый. Сертификат продлевает certbot на хосте.
