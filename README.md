<div align="center">

```
┌────────────────────────────┐
│       crowed_room          │
└────────────────────────────┘
```

### 🎙️ Telegram audio transcription

![Python](https://img.shields.io/badge/python-3.13-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/API-FastAPI-009688?logo=fastapi&logoColor=white)
![Kafka](https://img.shields.io/badge/queue-Kafka-231F20?logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/database-PostgreSQL-4169E1?logo=postgresql&logoColor=white)

</div>

---

## 🎯 Features

- Transcribes voice messages and audio files using OpenAI.
- Processes tasks in the background and stores statuses and results in PostgreSQL.
- Sends text to the chat or delivers long transcripts as UTF-8 files.

## ⚙️ How it works

```text
Telegram → Bot → API → Kafka → Worker → OpenAI
```

The worker saves results to PostgreSQL through the API. The bot polls the API and sends the result to the user.

## 🚀 Quick start

Requires Docker Engine and Docker Compose. Run commands from the project root.

1. Create the configuration file:

   ```bash
   cp .env.example .env
   ```

2. In `.env`, set the bot token `TELEGRAM_BOT_TOKEN`, the OpenAI key `OPENAI_API_KEY`, and the shared service token `INTERNAL_API_TOKEN`. Generate the service token with `openssl rand -hex 32`.

3. Build and start the project:

   ```bash
   bash 'bin/build_&_up_project.sh'
   ```

Migrations run automatically. Once the project is running, send the bot a voice message or an audio file.

[API documentation](http://localhost:8000/docs) — use `INTERNAL_API_TOKEN` to authorize.

## 🧪 Tests

With the environment running:

```bash
docker compose -f docker/docker-compose.dev.yml exec api python -B -m unittest discover -v
docker compose -f docker/docker-compose.dev.yml exec worker python -B -m unittest discover -v
docker compose -f docker/docker-compose.dev.yml exec telegram_bot python -B -m unittest discover -v
```

Tests use local HTTP servers and test doubles for Kafka and Telegram; persistence tests run against PostgreSQL. No paid API calls are made.
