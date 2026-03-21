# Pocket Partners Telegram Bot

Telegram-бот для автоматического получения статистики с дашборда Pocket Partners.

## Стек

- **Python 3.12+**
- **aiogram 3.x** — Telegram Bot framework
- **Playwright** — браузерная автоматизация
- **python-dotenv** — конфигурация через .env

## Возможности

- `/start` — главное меню с выбором периода
- **Сегодня / Вчера / 7 дней / 30 дней** — быстрые периоды
- **Свободный период** — ввод `DD.MM.YYYY - DD.MM.YYYY`
- Кэширование данных на 90 секунд
- Ограничение доступа по Telegram user ID

## Получаемые данные

CTR, RTD, FTD #, Депозиты #, FTD $, Депозиты $, Реги #, Выводы $, Сделки #, Клиенты #

## Установка на Replit

### 1. Переменные окружения (Secrets)

В разделе **Secrets** добавьте:

| Ключ | Описание |
|------|----------|
| `TELEGRAM_BOT_TOKEN` | Токен от @BotFather |
| `PP_LOGIN` | Email от Pocket Partners |
| `PP_PASSWORD` | Пароль от Pocket Partners |
| `ALLOWED_USER_IDS` | ID пользователей через запятую (пусто = все) |

### 2. Установка зависимостей

```bash
pip install -r requirements.txt
playwright install chromium
playwright install-deps chromium
```

### 3. Запуск

```bash
python main.py
```

## Структура проекта

```
├── main.py                  # Точка входа
├── config.py                # Конфигурация из env
├── requirements.txt
├── bot/
│   ├── handlers.py          # Обработчики команд и callback
│   └── keyboards.py         # Inline-клавиатуры
├── parser/
│   └── pocket_parser.py     # Playwright-скрапер
└── utils/
    └── date_parser.py       # Парсинг дат
```

## Настройка Playwright на Replit

На Replit при первом запуске Playwright скачает браузер автоматически.
Убедитесь, что переменная `HEADLESS=true` установлена (по умолчанию).

## Примечание о скрапере

Если сайт Pocket Partners обновит разметку, возможно потребуется обновить
CSS-селекторы в `parser/pocket_parser.py`. Все ключевые места помечены логами уровня DEBUG.
