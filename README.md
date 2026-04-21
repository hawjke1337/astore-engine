# astore-engine

Основной движок `engine.astoredirect.ru`: Flask-приложение с логикой сайта, Telegram-интеграцией, парсингом и API.

## Что хранится в репозитории

- `store.py` — основной сервер приложения
- `templates/store.html` — основной HTML-шаблон
- `logo.svg` — логотип
- `.env.example` — пример настроек

## Что не хранится в репозитории

- `.env`
- `app.db`, `store.db` и любые другие базы
- папка `uploads/`
- папка `sessions/`
- любые `*.session`

Эти файлы должны лежать на сервере в отдельной общей директории и переживать обновления кода.

## Локальный запуск

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python store.py
```

## Прод-логика

Рекомендуемая схема:

1. Код хранится на GitHub
2. На сервере лежит клон репозитория
3. Базы и загрузки лежат отдельно, вне git
4. После правок на GitHub сервер делает `git pull` и перезапуск сервиса

## Рекомендуемая структура на сервере

```text
/opt/astore-engine/
  app/        # git clone
  shared/
    app.db
    uploads/
```

Пример переменных:

```env
DATABASE_PATH=/opt/astore-engine/shared/app.db
UPLOAD_FOLDER=/opt/astore-engine/shared/uploads
```
