# Book Shelf Playground

This is a Next.js web application that allows you to interact with the Book Shelf Extractor API.

## Prerequisites

- Node.js installed
- Python installed with dependencies for the server (FastAPI, etc.)

## Getting Started

### 1. Start the Backend Server

From the `server` directory, run:

```bash
python server.py
```

The server should be running at `http://localhost:8000`.

### 2. Start the Frontend App

From the `server/playground` directory, run:

```bash
npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

## Features

- **Upload Frames**: Upload one or more images (frames) of bookshelves.
- **Review & Select**: See books detected in each frame and select which frames to include in the final result.
- **Merge & Deduplicate**: Merge results from all selected frames, deduplicating books by title.
- **Merge & Enrich**: Same as merge, but also enriches book metadata using external APIs (Google Books, Open Library) and Gemini.