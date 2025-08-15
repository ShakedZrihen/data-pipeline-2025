# 🛠️ Installations Guide

This guide walks you through installing all the required tools for the course on **Python 3.11**, **Docker**, **PostgreSQL**, and optional tools for **Telegram Bots** and **OpenAI integration**.

---

## 📦 1. Install Python 3.11

### ▶ Windows

1. Download Python 3.11 from:  
   https://www.python.org/downloads/windows/
2. Run the installer:

   - ✅ Check **"Add Python to PATH"**
   - ✅ Choose **Customize Installation**, and enable `pip`
   - ✅ Install

3. Verify installation:
   ```powershell
   python --version
   pip --version
   ```

### ▶ macOS

Using Homebrew:

```bash
brew install python@3.11
```

Check version:

```bash
python3.11 --version
```

(Optional: Create alias so `python` uses 3.11)

```bash
echo 'alias python=python3.11' >> ~/.zshrc && source ~/.zshrc
```

---

## 🐳 2. Install Docker Desktop

> We'll use Docker to run our services like PostgreSQL, API, and more.

### ▶ Windows & macOS

- Download Docker Desktop from:  
  https://www.docker.com/products/docker-desktop/

**After install:**

- Launch Docker
- Make sure it's running in the system tray (🐳 icon)

Verify installation:

```bash
docker --version
docker compose version
```

---

## 🗄 3. Install PostgreSQL (via Docker)

We'll use **Docker Compose** to run PostgreSQL, so no need to install it natively.

Create a `docker-compose.yml` like this:

```yaml
version: "3.9"
services:
  db:
    image: postgres:15
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: products
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
```

Run with:

```bash
docker compose up
```

---

## 🧪 4. Install Required Python Libraries

Create a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
```

Then install the following:

```bash
pip install requests beautifulsoup4 pandas sqlalchemy psycopg2-binary fastapi uvicorn python-telegram-bot openai
```

---

## 💬 5. Install Slack

> We'll use Slack for notifications.

### ▶ Windows & macOS

1. Download Slack from:  
   https://slack.com/downloads/

2. Install and launch the application

3. Sign in to our course workspace - link to join provided in moodle


### ▶ Web Version (Alternative)

If you prefer not to install the desktop app, you can use Slack in your browser:
- Go to https://slack.com/
- Sign in to your workspace


## ✅ Summary Checklist

| Tool             | Installed via                 |
| ---------------- | ----------------------------- |
| Python 3.11      | python.org / Homebrew         |
| pip + venv       | Built-in                      |
| Docker + Compose | docker.com                    |
| PostgreSQL       | via Docker                    |
| VSCode or Editor | https://code.visualstudio.com |
