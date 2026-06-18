# SER594-Team4-SkillSync
SkillSync: A Multi-Agent Platform for Learning and Project Partner Recommendation

SkillSync was developed because it is very annoying trying to find someone who can join you as your study partner or collaborate with you on your project. Either way, both of them would try posting on a Discord or on LinkedIn and still not find people. Therefore, an effective solution had to be implemented.

How we did it: when users enter their required details regarding their skills and goals, we generate a semantic vector for them using the text that they have entered and store it in DB. The same process is repeated when a user enters his query, and a vector is generated for it, and the most similar vectors are pulled from the database. Recommendations are made only on the basis of semantic similarity and not on any keyword search or filters.

## Live Demo

https://pacific-expression-production-2a5a.up.railway.app

---

## Team 4

| Name                     | ASU ID     | GitHub Username |
|--------------------------|------------|-----------------|
| Aarya Bhatt              | 1235522719 | AaryaBhatt9      |
| Dhvey Patel              | 1235668761 | Dhvey0201        |
| Sujan Uppalli Jayadevappa| 1234540166 | sujanu           |
| Tejas Shah               | 1235403730 | Hero4440         |


---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | Next.js 16 + Tailwind CSS |
| Backend | FastAPI (Python 3.11) |
| Database | PostgreSQL 15 |
| Vector Store | ChromaDB |
| Embeddings | sentence-transformers (all-MiniLM-L6-v2) |
| Authentication | JWT (python-jose + passlib) |
| Deployment | Docker Compose |

---

## Setup Instructions

### Prerequisites

- Python 3.11+
- Node.js 18+
- Docker
- Git

---

## Run in Local
Steps to follow: 

```bash
git clone https://github.com/AaryaBhatt9/SER594-Team4-SkillSync.git
cd SER594-Team4-SkillSync
cp .env.example .env
docker compose up --build -d
docker compose exec backend python -m scripts.ingest_profiles
```

After that, here's where everything lives:
- Frontend → http://localhost:3000
- Backend API → http://localhost:8000
- Swagger Docs → http://localhost:8000/docs
- PostgreSQL → localhost:5432
- ChromaDB → localhost:8001

---

## Running Without Docker

Alternatively, if you prefer not to use Docker or would like to run modules independently, then the following should do the trick:

### Backend

```bash
cd backend
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt

export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/skillsync
export SECRET_KEY=<any-your-secret-key-for-jwt>

psql -U postgres -d skillsync -f migrations/001_initial_schema.sql
psql -U postgres -d skillsync -f migrations/002_recommendation_history.sql
python -m scripts.ingest_profiles
uvicorn app.main:app --reload --port 8000
```

### Frontend

```bash
cd frontend
npm install
npm run dev
```

Frontend runs at http://localhost:3000.

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://skillsync:skillsync@localhost:5432/skillsync` |
| `SECRET_KEY` | JWT signing secret | `super-secret-dev-key` |
| `ALGORITHM` | JWT algorithm | `HS256` |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | Token expiry in minutes | `60` |
| `CHROMA_MODE` | `local` for embedded persistent Chroma, `http` for external Chroma | `http` |
| `CHROMA_HOST` | ChromaDB host | `localhost` |
| `CHROMA_PORT` | ChromaDB port | `8001` |
| `CHROMA_PERSIST_DIR` | Local Chroma storage path when `CHROMA_MODE=local` | `./chroma_data` |
| `NEXT_PUBLIC_API_URL` | Backend URL for frontend | `http://localhost:8000` |
| `NEXT_PUBLIC_API_BASE_URL` | Backend URL for frontend | `http://localhost:8000` |
| `CORS_ORIGINS` | Comma-separated allowed frontend origins | `http://localhost:3000` |

---

## Authentication

### Create an account

```bash
curl -X POST http://localhost:8000/auth/register \
  -H "Content-Type: application/json" \
  -d '{"name": "User One", "email": "user1@example.com", "password": "password123"}'
```

```json
{"id": 1, "name": "User One", "email": "user1@example.com"}
```

### Log in

```bash
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email": "user1@example.com", "password": "password123"}'
```

```json
{"access_token": "eyJ...", "token_type": "bearer"}
```

### Hit a protected route

```bash
curl http://localhost:8000/auth/me \
  -H "Authorization: Bearer <your_token>"
```

---

## Technique #1 For Artificial Intelligence (AI) – Semantic Matching Through ChromaDB

And this is where the magic happens. Here is how:

### First step: Load seed profiles. 

Seed profiles are ready for you to use inside `app/data/seed_profiles.json`. In total, there are 10 seed profiles. No need for user accounts just yet. Run this script below to load them:

**Then try a search:**

```bash
curl -X POST http://localhost:8000/match/search \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <your_token>" \
  -d '{
    "skills": ["Python", "FastAPI"],
    "goals": ["build AI projects"],
    "availability": "weekends",
    "skill_level": "intermediate",
    "top_k": 3
  }'
```

```json
{
  "matches": [
    {
      "user_id": "user_1",
      "score": 0.1745,
      "explanation": "Common interests/skills: availability, build, goals, intermediate, level"
    },
  ]
}
```

The `score` is cosine similarity — 0.91 means the profiles are very closely aligned. Anything above 0.75 is generally a solid match.

---
## Technique #2 For Artificial Intelligence (AI) – Multi-Agent Recommendation Pipeline

Beyond semantic embeddings, SkillSync uses a multi-agent pipeline to score and rank candidates. The pipeline is orchestrated in `backend/app/services/recommendation_pipeline.py` and consists of 5 agents:

1. **Profile Parser** (`agents/profile_parser.py`) — Normalizes the user's raw input (skills, goals, availability, skill level) into a clean, searchable profile.
2. **Candidate Retriever** (`agents/candidate_retriever.py`) — Queries ChromaDB with the parsed profile's embedding to retrieve the top-K semantically similar candidates.
3. **Skill Compatibility** (`agents/skill_compatibility.py`) — Computes Jaccard similarity between the user's skill set and each candidate's skill set, producing a score and reasoning.
4. **Goal Alignment** (`agents/goal_alignment.py`) — Same Jaccard approach but on goals, scoring how well each candidate's goals align with the user's.
5. **Recommendation Ranker** (`agents/recommendation_ranker.py`) — Combines skill (50%), goal (30%), and availability (20%) scores into a final compatibility score, ranks all candidates, and flags risk factors.

Each agent operates independently with a single responsibility, making the system easy to test and extend. For a deeper discussion of why we chose this architecture, see [`docs/DESIGN.md`](docs/DESIGN.md).

### Evaluation

We evaluated the pipeline's recommendation relevance (Precision@K) against a random baseline, and measured response latency. Full results are in [`eval/evaluation_report.md`](eval/evaluation_report.md), and the evaluation script can be run with:

```bash
python eval/run_evaluation.py
```

---

## Deployment

The system is deployed via Docker Compose. To run the full stack:

```bash
docker compose up --build -d
```

To reset the database (required if schema changes):

```bash
docker compose down -v
docker compose up --build -d
docker compose exec backend python -m scripts.ingest_profiles
```

---

## Tests

```bash
cd backend
pip install pytest httpx
pytest tests/ -v
```

## Architecture

See the architecture diagram: `docs/architecture-diagram.pdf`.
It's an oversimplification of the following architecture:
Next.js interacts with FastAPI through REST,
FastAPI saves user information in Postgres and embeddings in ChromaDB,
When searching, we generate embeddings and run similarity search in vector store.
we have an layer with JWT tokens on the backend side.