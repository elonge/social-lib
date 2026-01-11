import { SchemaType } from "@google/generative-ai";
import { Book } from "./types";
import { runGeminiAgent } from "./agent";

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

const OCRSchema = {
  type: SchemaType.OBJECT,
  properties: {
    texts: {
      type: SchemaType.ARRAY,
      items: { type: SchemaType.STRING },
      description: "List of raw text strings found on the book spines."
    }
  },
  required: ['texts']
};

const MetadataSchema = {
  type: SchemaType.OBJECT,
  properties: {
    title: { type: SchemaType.STRING },
    author: { type: SchemaType.STRING },
    language: { type: SchemaType.STRING, nullable: true, description: "ISO 2 letter language code (e.g. en, fr, es)" },
    confidence: { type: SchemaType.STRING, nullable: true, description: "Confidence level: High, Medium, Low" }
  },
  required: ['title', 'author', 'confidence']
};

const OCR_PROMPT = `
You are an advanced OCR (Optical Character Recognition) engine specializing in reading book spines.
Your task is to identify and transcribe the text from every distinct book spine visible in the image.

Rules:
1. Return a list of strings. Each string represents the full text found on a single spine.
2. Be purely technical. Do not attempt to correct spelling or guess missing words.
3. If a spine is clearly visible but the text is illegible or too blurry to read, include the string "UNIDENTIFIED_SPINE" in the list.
4. Do not include text from non-book objects.
`;

const METADATA_PROMPT = `
You are a digital librarian expert.
Your task is to analyze a raw text string extracted from a book spine and identify the book's metadata.

Rules:
1. Identify the Title and Author.
2. Determine the Language (2-letter ISO code).
3. Assign a Confidence level (High, Medium, Low).
   - High: Title and Author are clear and match known books.
   - Medium: Partial match or some ambiguity.
   - Low: Text is fragmentary or does not look like a book title.
4. If the text does not contain enough information to identify a book (e.g. just a logo or single word like "The"), set Title to "Unknown", Author to "Unknown", and Confidence to "Low".
`;

export async function identifyBooksFromImage(base64Image: string): Promise<Book[]> {
  const apiKey = process.env.NEXT_PUBLIC_GEMINI_API_KEY;
  if (!apiKey) {
    console.error("Missing API Key");
    return [];
  }

  // Remove data URL prefix if present
  const base64Data = base64Image.replace(/^data:image\/\w+;base64,/, "");

  try {
    // 1. OCR Step
    console.log("Starting OCR Step...");
    const ocrResult = await runGeminiAgent<{ texts: string[] }>(
      apiKey,
      OCR_PROMPT,
      [],
      [
        { inlineData: { data: base64Data, mimeType: "image/jpeg" } },
        { text: "Extract all text from the book spines in this image." }
      ],
      "submit_ocr_results",
      OCRSchema
    );

    if (!ocrResult || !ocrResult.texts) {
      console.warn("OCR returned no results");
      return [];
    }
    console.log(`OCR found ${ocrResult.texts.length} spines.`);

    // 2. Metadata Step (Parallel)
    const booksPromises = ocrResult.texts.map(async (text) => {
      if (text.trim() === "UNIDENTIFIED_SPINE" || text.includes("UNIDENTIFIED_SPINE")) {
        return {
          id: Math.random().toString(36).substring(2, 11),
          title: "Unidentified Spine",
          author: "Unknown",
          isUnidentified: true,
          confidence: "Low"
        } as Book;
      }

      try {
        const metadata = await runGeminiAgent<{ title: string, author: string, language?: string, confidence?: string }>(
          apiKey,
          METADATA_PROMPT,
          [],
          `Analyze this book spine text: "${text}"`,
          "submit_book_metadata",
          MetadataSchema
        );

        if (metadata) {
          let coverUrl = undefined;
          // Only fetch cover if we have a valid title
          if (metadata.title && metadata.title !== "Unknown" && metadata.confidence !== "Low") {
             const cover = await findBookCover(metadata.title, metadata.author, metadata.language);
             coverUrl = cover.url || undefined;
          }

          // If confidence is Low, we might want to flag it as Review Needed or Unidentified?
          // For now, we trust the agent's output. If title is "Unknown", user will see it.

          return {
            id: Math.random().toString(36).substring(2, 11),
            title: metadata.title || "Unknown Title",
            author: metadata.author || "Unknown Author",
            language: metadata.language,
            confidence: metadata.confidence || "Low",
            coverImage: coverUrl
          } as Book;
        }
      } catch (e) {
        console.error("Metadata extraction failed for text:", text, e);
      }
      
      // Fallback
      return {
        id: Math.random().toString(36).substring(2, 11),
        title: text.substring(0, 50),
        author: "Unknown",
        confidence: "Low"
      } as Book;
    });

    const books = await Promise.all(booksPromises);
    return books;

  } catch (error) {
    console.error("Agent workflow failed:", error);
    return [];
  }
}
