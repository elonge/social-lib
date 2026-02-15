This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Deploy to GCP (Cloud Run)

Use the provided script in `server/deploy_gcp.sh`. It deploys the API to Cloud Run.

Prereqs:
- `gcloud` installed and authenticated

Run from repo root:

```bash
./server/deploy_gcp.sh
```

If your key is in a specific env file:

```bash
ENV_FILE=.env.local ./server/deploy_gcp.sh
```

```bash
PROJECT_ID=my-gcp-project REGION=us-central1 SERVICE_NAME=book-extractor-api ./server/deploy_gcp.sh
```

## Team Setup & Secrets

We use **Google Secret Manager** to share the `service-account.json`.

1.  **Download the secret** (Run this to get the file locally):
    ```bash
    gcloud secrets versions access latest --secret="firebase-service-account" > server/service-account.json
    ```

## Production Deployment

To deploy to the production project (`social-lib-487109`), run:

```bash
PROJECT_ID=social-lib-487109 ./server/deploy_gcp.sh
```

This script will:
1.  Use your local `server/service-account.json` credentials.
2.  Inject them securely into the Cloud Run container.
3.  Deploy the service to the specified project.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Frontend Setup

The frontend needs Firebase configuration keys.

1.  Copy the example env file:
    ```bash
    cp server/playground/.env.example server/playground/.env.local
    ```
2.  Edit `server/playground/.env.local` and fill in your Firebase keys (from Project Settings).

## Deploy on Vercel

1.  Import this repo into Vercel.
2.  Set the **Root Directory** to `server/playground`.
3.  Go to **Settings > Environment Variables**.
4.  Add all the keys from `server/playground/.env.example` (e.g., `NEXT_PUBLIC_FIREBASE_API_KEY`, `NEXT_PUBLIC_API_URL`, etc.).
    - Note: For `NEXT_PUBLIC_API_URL`, use your deployed backend URL (e.g., Cloud Run URL).
