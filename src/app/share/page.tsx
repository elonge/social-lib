"use client";

import { useState, useEffect } from "react";
import { Search, Library as LibraryIcon, Book as BookIcon, ScanLine, ArrowRight } from "lucide-react";
import { Book } from "@/lib/types";
import Link from "next/link";
import { getLibraryBooks } from "@/app/actions";

export default function SharedLibraryPage() {
  const [library, setLibrary] = useState<Book[]>([]);
  const [search, setSearch] = useState("");
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const fetchBooks = async () => {
      try {
        const books = await getLibraryBooks();
        setLibrary(books);
      } catch (error) {
        console.error("Failed to load library", error);
      } finally {
        setIsLoading(false);
      }
    };
    fetchBooks();
  }, []);

  const filteredBooks = library.filter(book => 
    book.title.toLowerCase().includes(search.toLowerCase()) || 
    book.author.toLowerCase().includes(search.toLowerCase())
  );

  return (
    <div className="min-h-screen bg-zinc-50 flex flex-col">
      <header className="sticky top-0 bg-white border-b border-zinc-100 z-10">
        <div className="max-w-5xl mx-auto px-6 py-6">
          <div className="flex items-center justify-between mb-6">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-zinc-100 rounded-xl">
                <LibraryIcon className="w-5 h-5 text-zinc-900" />
              </div>
              <h1 className="text-2xl font-bold text-zinc-900 tracking-tight">Shared Library</h1>
            </div>
            <Link 
              href="/capture"
              className="px-4 py-2 bg-zinc-900 text-white text-sm font-semibold rounded-full hover:bg-zinc-800 transition-colors flex items-center gap-2"
            >
              <ScanLine className="w-4 h-4" />
              Create your own library
            </Link>
          </div>

          <div className="relative">
            <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-400" />
            <input 
              type="text"
              placeholder="Search collection..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="w-full bg-zinc-100 border-none rounded-2xl py-3 pl-12 pr-4 text-sm focus:ring-2 focus:ring-zinc-200 transition-all placeholder:text-zinc-400"
            />
          </div>
        </div>
      </header>

      <main className="flex-1 max-w-5xl mx-auto w-full px-6 py-8">
        {isLoading ? (
          <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-6">
            {[...Array(5)].map((_, i) => (
              <div key={i} className="animate-pulse">
                <div className="aspect-[2/3] bg-zinc-200 rounded-2xl mb-3"></div>
                <div className="h-4 bg-zinc-200 rounded w-3/4 mb-2"></div>
                <div className="h-3 bg-zinc-100 rounded w-1/2"></div>
              </div>
            ))}
          </div>
        ) : filteredBooks.length > 0 ? (
          <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-6">
            {filteredBooks.map((book) => (
              <div key={book.id} className="group">
                <div className="aspect-[2/3] bg-zinc-200 rounded-2xl mb-3 shadow-sm group-hover:shadow-md transition-all overflow-hidden flex flex-col items-center justify-center p-4 text-center border border-zinc-100 bg-gradient-to-br from-white to-zinc-50 relative">
                  {book.coverImage ? (
                    <img src={book.coverImage} alt={book.title} className="absolute inset-0 w-full h-full object-cover" />
                  ) : (
                    <BookIcon className="w-8 h-8 text-zinc-300 mb-2" />
                  )}
                  <div className="absolute inset-x-0 bottom-0 p-4 bg-gradient-to-t from-zinc-900/10 to-transparent opacity-0 group-hover:opacity-100 transition-opacity" />
                </div>
                <h3 className="font-semibold text-zinc-900 text-sm leading-snug line-clamp-2">{book.title}</h3>
                <p className="text-zinc-500 text-xs mt-1">{book.author}</p>
              </div>
            ))}
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center py-20 text-center">
            <div className="w-20 h-20 bg-zinc-100 rounded-full flex items-center justify-center mb-6">
              <BookIcon className="w-8 h-8 text-zinc-300" />
            </div>
            <h2 className="text-xl font-semibold text-zinc-900 mb-2">Empty Library</h2>
            <p className="text-zinc-500 max-w-xs mb-8">
              This library hasn't been populated yet.
            </p>
            <Link 
              href="/capture"
              className="bg-zinc-900 text-white px-8 py-3 rounded-full font-semibold hover:bg-zinc-800 transition-all flex items-center gap-2"
            >
              Start Your Own <ArrowRight className="w-4 h-4" />
            </Link>
          </div>
        )}
      </main>
    </div>
  );
}
