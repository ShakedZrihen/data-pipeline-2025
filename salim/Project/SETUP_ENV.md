# Environment Variables Setup

## Overview
This pipeline uses environment variables to keep sensitive information secure. Follow these steps to set up your environment.

## Quick Setup

### 1. Create .env file
Create a `.env` file in the `salim/Project/` directory:

```bash
cd salim/Project
touch .env
```

### 2. Add your environment variables
Add the following to your `.env` file:

```bash
# PostgreSQL Database
POSTGRES_PASSWORD=your_actual_postgres_password

# Supabase Configuration  
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_actual_supabase_key

# Claude API
CLAUDE_API_KEY=your_actual_claude_api_key

# Optional: Override database URL
DATABASE_URL=postgresql://postgres:your_password@postgres:5432/postgres
```

### 3. Replace placeholder values
- `your_actual_postgres_password` → Your PostgreSQL password
- `your_actual_supabase_key` → Your Supabase anon/public key
- `your_actual_claude_api_key` → Your Claude API key

## Required API Keys

### Claude API Key
1. Go to [Anthropic Console](https://console.anthropic.com/)
2. Create an account or sign in
3. Generate a new API key
4. Copy the key (starts with `sk-ant-api03-...`)

### Supabase Key
1. Go to your [Supabase Project](https://supabase.com/)
2. Navigate to Settings → API
3. Copy the `anon` public key

## Verification

After setting up your `.env` file:

1. **Start the pipeline:**
   ```bash
   docker-compose up -d
   ```

2. **Check if services are running:**
   ```bash
   docker-compose ps
   ```

3. **Check enricher logs:**
   ```bash
   docker-compose logs enricher
   ```

## Security Notes

- **Never commit your `.env` file** - it's already in `.gitignore`
- **Keep your API keys secure** - don't share them
- **Use different keys for development/production**
- **Rotate keys regularly** for security

## Troubleshooting

### "Environment variable not set" errors
- Ensure your `.env` file is in the correct directory
- Check that variable names match exactly
- Restart Docker containers after changing `.env`

### API authentication failures
- Verify your API keys are correct
- Check if keys have expired
- Ensure keys have the right permissions

## Example .env file

```bash
# PostgreSQL
POSTGRES_PASSWORD=mySecurePassword123

# Supabase  
SUPABASE_URL=https://abcdefghijklm.supabase.co
SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

# Claude API
CLAUDE_API_KEY=sk-ant-api03-abc123def456...
```

## Next Steps

After setting up environment variables:
1. Run the pipeline: `docker-compose up -d`
2. Monitor progress: `docker-compose logs -f`
3. Test the API: `curl http://localhost:3001/health`
4. Check the README for full usage instructions
