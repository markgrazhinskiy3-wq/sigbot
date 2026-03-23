FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    libglib2.0-0 libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libdbus-1-3 libxcb1 libxkbcommon0 libx11-6 \
    libxcomposite1 libxdamage1 libxext6 libxfixes3 libxrandr2 \
    libgbm1 libpango-1.0-0 libcairo2 libasound2 libatspi2.0-0 \
    wget ca-certificates fonts-liberation git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY signal_bot/requirements.txt ./
RUN pip install --no-cache-dir aiogram==3.13.1 playwright==1.48.0 aiosqlite==0.20.0 pandas==2.2.3 python-dotenv==1.0.1 aiofiles==24.1.0 "numpy>=1.26.4"
RUN pip install --no-cache-dir --pre pandas-ta
RUN playwright install chromium
RUN playwright install-deps chromium

COPY signal_bot/ ./signal_bot/

CMD ["python3", "signal_bot/main.py"]
