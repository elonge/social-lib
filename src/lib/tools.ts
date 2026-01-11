import { tool } from "@openai/agents";
import { z } from "zod";

export const createFindCoverImageTool = () => 
  tool({
    name: "find_cover_image",
    description: "Find a book cover image URL by searching for the title and author.",
    parameters: z.object({
      title: z.string().describe("The title of the book"),
      author: z.string().describe("The author of the book"),
    }),
    execute: async ({ title, author }) => {
      console.log(`🔎 [Tool: find_cover_image] Searching for: ${title} by ${author}`);
      try {
        const query = encodeURIComponent(`${title} ${author}`);
        const response = await fetch(`https://www.googleapis.com/books/v1/volumes?q=${query}&maxResults=1`);
        const data = await response.json();
        
        if (data.items && data.items.length > 0) {
          const volumeInfo = data.items[0].volumeInfo;
          const imageLinks = volumeInfo.imageLinks;
          if (imageLinks) {
            // Prefer thumbnail, then smallThumbnail
            const url = imageLinks.thumbnail || imageLinks.smallThumbnail;
            // HTTPS fix
            return url.replace('http:', 'https:');
          }
        }
        return "";
      } catch (error) {
        console.error("Error fetching cover image:", error);
        return "";
      }
    }
  });
