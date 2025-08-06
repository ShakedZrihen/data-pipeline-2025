## 🎯 Project Overview
This is a **Data Pipeline project** with a FastAPI backend and web crawler functionality. Instead of writing code yourself, follow these rules to guide your development process.

## 📚 Class Progression & Current Focus

### Class 2 (Today): GZ Files & S3 Upload
- **Goal**: Download .gz files from supermarket websites and upload to S3
- **Methods needed**:
  - `crawl()` - Find .gz files on websites
  - `find_gz_files()` - Extract .gz file URLs from HTML
  - `download_gz_file()` - Download individual .gz files
  - `upload_to_s3()` - Upload files to S3
- **No data extraction yet** - just file handling

### Class 3 (Next Week): Data Extraction
- **Goal**: Extract and parse data from the .gz files
- **Will add**: Product parsing, data validation, database storage
- **Focus**: Working with the downloaded data

### Class 4+ (Future): Advanced Features
- **Goal**: Scheduling, monitoring, error handling
- **Will add**: Cron jobs, logging, retry mechanisms

## 📋 Before You Start Coding

### 1. **Always Check Existing Examples First**
- Look in `examples/` directory for similar implementations
- Check `assignments/warm-up/` for reference code
- Review `examples/simple-crawler/` for crawler patterns
- Examine `examples/simple-fast-api-server/` for API patterns

### 2. **Use the Cheat Sheets**
- `examples/BeautifulSoup Cheat Sheet.md` - for web scraping
- `examples/Selenium Cheat Sheet.md` - for browser automation
- `examples/Python Cheat Sheet.md` - for Python syntax

### 3. **Follow the Development Guidelines**
- Read `DEVELOPMENT_GUIDELINES.md` for coding standards
- Check `CONTRIBUTING.md` for contribution rules
- Review `Installations.md` for setup instructions

## 🚀 Step-by-Step Development Process

### Phase 1: Setup & Planning
1. **Environment Setup**
   - Use Docker: `docker-compose up --build`
   - Check if services are running: `http://localhost:8000/health`
   - Verify database connection

2. **Requirements Analysis**
   - What data do you need to collect?
   - What endpoints do you need?
   - What database schema is required?

### Phase 2: Implementation Strategy
1. **Start with Existing Code**
   - Copy relevant examples from `examples/` directory
   - Modify existing `salim/crawler/base.py` instead of starting from scratch
   - Use `examples/simple-crawler/` as your crawler template

2. **API Development**
   - Follow the pattern in `salim/app/routes/api/health.py`
   - Add new endpoints in `salim/app/routes/api/`
   - Use FastAPI best practices from `examples/simple-fast-api-server/`

3. **Database Operations**
   - Use PostgreSQL (already configured in Docker)
   - Follow SQLAlchemy patterns if needed
   - Check connection at `http://localhost:8000/health/detailed`

### Phase 3: Testing & Validation
1. **Test Your Endpoints**
   - Use Swagger UI: `http://localhost:8000/docs`
   - Test health endpoints first
   - Validate data formats

2. **Check Data Quality**
   - Verify crawler output
   - Test database connections
   - Validate API responses

## 🛠️ Common Tasks & Solutions

### Adding a New API Endpoint
1. Create new file in `salim/app/routes/api/`
2. Follow the pattern from `health.py`
3. Import and register in `salim/app/main.py`
4. Test at `http://localhost:8000/docs`

### Implementing a Web Crawler
1. Extend the `crawler` class in `salim/crawler/base.py`
2. Use BeautifulSoup patterns from `examples/BeautifulSoup Cheat Sheet.md`
3. For dynamic content, use Selenium from `examples/Selenium Cheat Sheet.md`
4. Test with a simple URL first

### Database Operations
1. Use the existing PostgreSQL setup
2. Connection details: localhost:5432, database: salim_db
3. Username: postgres, Password: postgres
4. Test connection via health endpoint

## 🔍 Debugging Checklist

### When Something Doesn't Work:
1. **Check Docker Status**
   ```bash
   docker-compose ps
   docker-compose logs
   ```

2. **Verify API Health**
   - `http://localhost:8000/health`
   - `http://localhost:8000/health/detailed`

3. **Check Database**
   - Test connection via health endpoint
   - Verify tables exist

4. **Review Logs**
   - API logs: `docker-compose logs api`
   - Database logs: `docker-compose logs db`

## 📚 Reference Materials

### Code Examples to Copy From:
- **Crawler**: `examples/simple-crawler/bs4-example.py`
- **API**: `examples/simple-fast-api-server/app/api/routes.py`
- **Docker**: `examples/s3-simulator/docker-compose.yml`
- **Requirements**: `examples/requirements.txt`

### Documentation:
- **API Docs**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`
- **Project README**: `salim/README.md`

## ⚠️ Important Rules

1. **Never Start from Scratch**
   - Always copy and modify existing examples
   - Use the cheat sheets for syntax
   - Follow established patterns

2. **Test Incrementally**
   - Test each endpoint as you create it
   - Verify data at each step
   - Use the health endpoints to validate

3. **Use Docker**
   - Don't install dependencies locally
   - Use `docker-compose up --build` for changes
   - Check logs when debugging

4. **Follow the Project Structure**
   - Keep API routes in `salim/app/routes/api/`
   - Put crawler logic in `salim/crawler/`
   - Use existing configuration patterns

## 🎯 Success Metrics

Your implementation is successful when:
- ✅ All Docker services start without errors
- ✅ Health endpoints return 200 OK
- ✅ API documentation is accessible at `/docs`
- ✅ Database connection is established
- ✅ Your crawler can extract data
- ✅ Your API endpoints return expected responses

## 🆘 When You're Stuck

1. **Check the examples first**
2. **Review the cheat sheets**
3. **Test the health endpoints**
4. **Check Docker logs**
5. **Verify your code follows existing patterns**

Remember: **Copy, modify, test** - don't write from scratch! 