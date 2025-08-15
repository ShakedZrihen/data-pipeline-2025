# 🛠️ Full-Stack Data Pipeline Course

## 📌 Overview

This course introduces end-to-end development of a **data pipeline**, starting from **web scraping** supermarket websites, through **data processing and analysis**, storing in a **cloud database**, and finally exposing the data via **microservice APIs** and an **interactive chatbot** (e.g., Telegram bot).

A central goal of the course is to **understand the data flow**: how raw, messy information is collected, cleaned, transformed, and made usable and accessible through APIs and intelligent interfaces.

Throughout the course, we will:

- Use **Docker** and **Docker Compose** to containerize and isolate components.
- Apply **microservices architecture** and handle **inter-service communication**.
- Integrate **AI elements** such as natural language queries and RAG (Retrieval-Augmented Generation) to enrich data and answer user questions.

> 🎯 Target Audience: Software engineering students with basic knowledge in Python and SQL, and some experience with APIs. No prior experience with Docker or bots is required.

---

## 🧪 Final Project

You will build a **Shopping List Assistant Bot**, which will:

- Scrape product prices from multiple supermarket websites.
- Build and update a **product price database**.
- Find the **cheapest basket** for a given shopping list (and suggest cheaper substitutes).
- Expose data and logic via a **FastAPI** microservice backend.
- Communicate with users through a **Telegram chatbot**.

> 🎯 **Course Focus Areas:**
>
> - **Understanding the full data pipeline** – from raw, unstructured data to clean, enriched, queryable data.
> - Practicing **DevOps principles** using Docker and Compose.
> - Building **microservice-based systems** with clear responsibility boundaries.
> - Designing and exposing **REST APIs** to serve processed data.
> - **Applying AI techniques** like RAG and natural language queries to make data more accessible.

---

## 🗓️ Course Schedule

1. **Intro to Data Pipelines & Environment Setup**

   - What is a data pipeline?
   - Docker basics, running Python in containers
   - Designing your pipeline architecture
   - Setup your local dev environment

2. **Web Scraping Techniques**

   - HTML/CSS, DOM parsing
   - Scraping using `requests`, `BeautifulSoup`, and `Selenium`
   - Handle pagination, JavaScript-rendered content

3. **APIs & Structured Data Collection**

   - Working with external/public APIs
   - Designing your own FastAPI endpoints
   - API documentation with Swagger

4. **Data Cleaning & Storage**

   - Normalize data, handle missing values
   - Enrich product info with AI
   - Store structured data in a cloud-hosted DB

5. **Advanced Docker & Microservices**

   - Docker Compose: Orchestrating multi-container setups
   - Microservice design and communication

6. **Integrating AI (RAG & Natural Language)**

   - Using OpenAI API for answering queries
   - Building a small RAG system to match products
   - Natural language interface over your data

7. **Final Project Build & Demo**
   - Assemble all components
   - Prepare your bot for real use
   - Final presentations & review

---

## 🧰 Tech Stack

- **Python 3.11**
- **Docker & Docker Compose**
- **FastAPI**
- **BeautifulSoup & Selenium**
- **PostgreSQL / MongoDB**
- **OpenAI API**
- **Telegram Bot API**

---

## 📦 Installations

For installation instructions, see [`Installations.md`](./Installations.md)

## 🤝 Contributing

For guidelines on contributing to this course, see [`CONTRIBUTING.md`](./CONTRIBUTING.md)

---

## 👩‍🏫 Instructor

Led by [Shaked Zohar](https://github.com/ShakedZrihen) — Senior Full-Stack Developer.

---

## 📁 Repository Structure

```
course-root/
│
├── examples/ # course examples for the core concepts
├── assignments/ # student assignments and exercises
│   └── warm-up/ # warm-up exercises to get comfortable with tools
└── Installations.md # Setup guide
```

---

Let’s build something real 🚀
