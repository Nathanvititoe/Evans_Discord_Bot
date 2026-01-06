View this in fancy way in VS Code `ctrl + shift + v'
- copy env.default and name .env

**Filewatcher**
- start up filewatcher with
```bash
cd filewatcher && docker compose up -d
```
- view logs with 
```bash
docker logs -f filewatcher
```
- shutdown filewatcher
```bash
docker compose down -v
```