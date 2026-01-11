import { SchemaType } from "@google/generative-ai";
import { Book } from "./types";
import { runGeminiAgent } from "./agent";
import { createFindCoverImageTool } from "./tools";

// Define the output schema for the final answer tool

interface BookCover {
  url: string | null;
  source: 'OpenLibrary' | 'GoogleBooks' | null;
}

export async function findBookCover(title: string, author: string, language?: string): Promise<BookCover> {
  try {
    const params = new URLSearchParams({ title, author });
    if (language) {
      params.append("language", language);
    }
    const response = await fetch(`/api/cover?${params.toString()}`);
    
    if (response.ok) {
      return await response.json();
    }
  } catch (error) {
    console.error("Failed to fetch book cover from internal API", error);
  }

  return { url: null, source: null };
}
const BookSchemaJSON = {
  type: SchemaType.OBJECT,
  properties: {
    title: { type: SchemaType.STRING },
    author: { type: SchemaType.STRING },
    language: { type: SchemaType.STRING, nullable: true, description: "ISO 2 letter language code (e.g. en, fr, es)" },
    confidence: { type: SchemaType.STRING, nullable: true, description: "Confidence level: High, Medium, Low" }
  },
  required: ['title', 'author', 'language', 'confidence']
};

const FinalAnswerSchemaJSON = {
  type: SchemaType.OBJECT,
  properties: {
    books: {
      type: SchemaType.ARRAY,
      items: BookSchemaJSON
    }
  },
  required: ['books']
};

/*
3. For EACH identified book, you MUST use the 'find_cover_image' tool to search for a real cover image URL. 
   - Pass the extracted title and author to the tool.
   - Use the URL returned by the tool.
4. Once you have the details and cover images for all books, use the 'submit_final_book_list' tool to return the result.
*/
const SYSTEM_PROMPT = `
You are an expert digital librarian.
Your goal is to extract book data *only* from clearly visible text in the image.

**Strict Anti-Hallucination Rules:**
1. **DO NOT GUESS.** If a spine is blurry, shadowed, or the text is illegible, mark it as unidentified.
2. **VERBATIM MATCHING:** You must be able to read the actual letters on the book. Do not infer a book based on color or logo alone.

**Process:**
1. Scan the image for text that is clearly legible.
2. Extract the Title and Author exactly as they appear.
3. Determine the language (2-letter ISO code).
4. If a spine is visible but the text is unreadable, create an entry with title "Unidentified Spine" and author "Unknown".

**Output Format (JSON):**
Return a JSON array of objects.
{
  "title": "Exact text found on spine",
  "author": "Exact text found on spine",
  "confidence": "High/Medium/Low", 
  "language": "en"
}
`;

export async function identifyBooksFromImage(base64Image: string): Promise<Book[]> {
  const apiKey = process.env.NEXT_PUBLIC_GEMINI_API_KEY;
  if (!apiKey) {
    console.error("Missing API Key");
    return [];
  }

  // Remove data URL prefix if present
  const base64Data = base64Image.replace(/^data:image\/\w+;base64,/, "");

  const userMessage = [
    {
      inlineData: {
        data: base64Data,
        mimeType: "image/jpeg",
      },
    },
    { text: "Please identify the books in this image and find their covers." }
  ];

  const tools = [
    // createFindCoverImageTool()
  ];

  try {
    const result = await runGeminiAgent<{ books: any[] }>(
      apiKey,
      SYSTEM_PROMPT,
      [],
      userMessage,
      "submit_final_book_list",
      FinalAnswerSchemaJSON
    );

    if (result && result.books) {
      const booksWithCovers = await Promise.all(
        result.books.map(async (b) => {
          console.log("Identified book:", b.title, "by", b.author, "lang:", b.language);
          
          const isUnidentified = b.title.toLowerCase().includes("unidentified spine");
          let coverUrl = undefined;

          if (!isUnidentified) {
             const cover = await findBookCover(b.title, b.author, b.language);
             coverUrl = cover.url || undefined;
          }
          
          return {
            id: Math.random().toString(36).substring(2, 11),
            title: b.title,
            author: b.author,
            language: b.language,
            coverImage: coverUrl,
            isUnidentified,
            confidence: b.confidence
          };
        })
      );
      return booksWithCovers;
    }

    return [];
  } catch (error) {
    console.error("Agent workflow failed:", error);
    return [];
  }
}