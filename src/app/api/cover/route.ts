import { NextRequest, NextResponse } from "next/server";

const GOOGLE_API_KEY = process.env.GOOGLE_SEARCH_CX_API_KEY;
const GOOGLE_SEARCH_CX = process.env.GOOGLE_SEARCH_CX;

async function searchGoogleCustomSearch(title: string, author: string, apiKey: string | undefined) {
  if (!apiKey || !GOOGLE_SEARCH_CX) {
    if (!GOOGLE_SEARCH_CX) console.warn("GOOGLE_SEARCH_CX not defined, skipping Custom Search");
    return null;
  }

  const executeSearch = async (query: string) => {
    const url = `https://www.googleapis.com/customsearch/v1?key=${apiKey}&cx=${GOOGLE_SEARCH_CX}&q=${encodeURIComponent(query)}&searchType=image&num=1&imgSize=large`;
    const response = await fetch(url);
    return await response.json();
  };

  try {
    // Search for "title author book cover"
    let query = `${title} ${author} book cover`;
    let data = await executeSearch(query);

    // If there's a corrected query, try searching with it
    if (data.spelling?.htmlCorrectedQuery) {
      // Strip HTML tags from the corrected query
      const correctedQuery = data.spelling.htmlCorrectedQuery.replace(/<[^>]*>?/gm, '');
      console.log(`Spelling correction found: "${query}" -> "${correctedQuery}"`);
      data = await executeSearch(correctedQuery);
    }

    if (data.items && data.items.length > 0) {
      return data.items[0].link;
    }
  } catch (error) {
    console.error("Google Custom Search failed:", error);
  }
  return null;
}

async function searchGoogleBooks(title: string, author: string, language: string | null, apiKey: string | undefined) {
  if (!apiKey) return null;

  try {
    const cleanTitle = encodeURIComponent(title);
    const cleanAuthor = encodeURIComponent(author);
    let googleUrl = `https://www.googleapis.com/books/v1/volumes?q=intitle:${cleanTitle}+inauthor:${cleanAuthor}&key=${apiKey}`;
    if (language) {
      googleUrl += `&langRestrict=${language}`;
    }

    const response = await fetch(googleUrl);
    const data = await response.json();

    if (data.items && data.items.length > 0) {
      const volumeInfo = data.items[0].volumeInfo;
      let coverUrl = volumeInfo.imageLinks?.thumbnail || volumeInfo.imageLinks?.smallThumbnail;

      if (coverUrl) {
        // Enforce HTTPS and higher resolution if possible
        return coverUrl.replace("http://", "https://").replace("&zoom=1", "&zoom=0");
      }
    }
  } catch (error) {
    console.error("Google Books search failed:", error);
  }
  return null;
}

async function searchOpenLibrary(title: string, author: string) {
  try {
    const cleanTitle = encodeURIComponent(title);
    const cleanAuthor = encodeURIComponent(author);
    const olUrl = `https://openlibrary.org/search.json?title=${cleanTitle}&author=${cleanAuthor}&limit=1`;
    
    const response = await fetch(olUrl);
    const data = await response.json();

    if (data.docs && data.docs.length > 0) {
      const book = data.docs[0];
      if (book.cover_i) {
        return `https://covers.openlibrary.org/b/id/${book.cover_i}-L.jpg`;
      } else if (book.isbn && book.isbn.length > 0) {
        return `https://covers.openlibrary.org/b/isbn/${book.isbn[0]}-L.jpg`;
      }
    }
  } catch (error) {
    console.warn("Open Library search failed:", error);
  }
  return null;
}

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const title = searchParams.get("title");
  const author = searchParams.get("author");
  const language = searchParams.get("language");

  if (!title || !author) {
    return NextResponse.json({ error: "Missing title or author" }, { status: 400 });
  }

  // 1. Google Custom Search
  const cseCover = await searchGoogleCustomSearch(title, author, GOOGLE_API_KEY);
  if (cseCover) {
    return NextResponse.json({ url: cseCover, source: "GoogleCustomSearch" });
  }

  // 2. Google Books
  const gbCover = await searchGoogleBooks(title, author, language, GOOGLE_API_KEY);
  if (gbCover) {
    return NextResponse.json({ url: gbCover, source: "GoogleBooks" });
  }

  if (language === "he") {
    // Skip Open Library for Hebrew
    return NextResponse.json({ url: null, source: null });
  }

  const olCover = await searchOpenLibrary(title, author);
  if (olCover) {
    return NextResponse.json({ url: olCover, source: "OpenLibrary" });
  }


  return NextResponse.json({ url: null, source: null });
}
