"use server";

import clientPromise from "@/lib/mongodb";
import { Book } from "@/lib/types";

// Define the database and collection names
const DB_NAME = "Books";
const COLLECTION_NAME = "personal_books";

export async function saveBooksToLibrary(books: Book[]) {
  try {
    const client = await clientPromise;
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);

    // Hardcoded dummy owner for now
    const ownerId = "user@example.com";

    // Prepare documents for insertion
    const bookDocs = books.map((book) => ({
      title: book.title,
      author: book.author,
      isbn: book.isbn,
      coverImage: book.coverImage,
      ownerId: ownerId,
      addedAt: new Date(),
    }));

    if (bookDocs.length === 0) {
        return { success: true, count: 0 };
    }

    const result = await collection.insertMany(bookDocs);
    
    return { success: true, count: result.insertedCount };
  } catch (error) {
    console.error("Failed to save books to database:", error);
    return { success: false, error: "Failed to save books" };
  }
}

export async function getLibraryBooks(ownerId: string = "user@example.com") {
  try {
    const client = await clientPromise;
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);

    // Fetch books for specific owner, sorted by newest first
    const books = await collection.find({ ownerId }).sort({ addedAt: -1 }).toArray();
    
    // Convert _id to string and return simple objects
    return books.map((book) => ({
      id: book._id.toString(),
      title: book.title,
      author: book.author,
      isbn: book.isbn,
      coverImage: book.coverImage,
    })) as Book[];
  } catch (error) {
    console.error("Failed to fetch library:", error);
    return [];
  }
}
