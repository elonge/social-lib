# Book Shelf Playground

A full-stack application for extracting and enriching book metadata from images of bookshelves.

## Architecture
- **Backend**: FastAPI (Python) running on GCP Cloud Run.
- **Frontend**: Next.js (TypeScript/Tailwind) running on Vercel.

---

## 💻 Local Development

### 1. Backend Setup (FastAPI)
From the root `server/` directory:

```bash
# Install dependencies
pip install -r requirements.txt

# Set your Gemini API Key
export GOOGLE_API_KEY="your_api_key_here"

# Start the server
python server.py
```
The backend will be available at `http://127.0.0.1:8000`.

### 2. Frontend Setup (Next.js)
From the `server/playground/` directory:

```bash
# Install dependencies
npm install

# Create local env file
echo "NEXT_PUBLIC_API_URL=http://127.0.0.1:8000" > .env.local

# Start development server
npm run dev
```
Open [http://localhost:3000](http://localhost:3000) in your browser.

---

## 🚀 Deployment

### Backend (GCP Cloud Run)
The backend is containerized and deployed to Google Cloud.

1.  **Configure Project**:
    ```bash
    gcloud config set project social-lib-487109
    ```

2.  **Deploy**:
    From the `server/` directory:
    ```bash
    gcloud run deploy book-extractor-api \
        --source . \
        --region us-central1 \
        --allow-unauthenticated \
        --set-env-vars="GOOGLE_API_KEY=YOUR_API_KEY"
    ```

### Frontend (Vercel)
Deployment is automated via Git.

1.  **Push Changes**:
    Simply `git push` to your main branch.
2.  **Configuration**:
    Ensure the **Root Directory** in Vercel is set to `server/playground`.
3.  **Environment Variables**:
    Ensure `NEXT_PUBLIC_API_URL` is set to your Cloud Run Service URL in the Vercel Dashboard.

---

## 🛠 Features
- **Instant Upload**: Parallel processing of multiple frames.
- **Skeleton States**: Visual feedback during image upload and Gemini processing.
- **Review Modal**: "See all" functionality to view every book detected in a specific frame.
- **Advanced Deduplication**: Merges results from multiple frames, selecting the highest quality metadata for each unique title.
- **Metadata Enrichment**: Optional integration with Google Books and Open Library.
