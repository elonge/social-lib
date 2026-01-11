"use client";

import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { Check, Edit2, Trash2, Plus, Sparkles, Loader2, RefreshCw, AlertCircle, HelpCircle } from "lucide-react";
import { Book } from "@/lib/types";
import { cn } from "@/lib/utils";
import { identifyBooksFromImage, findBookCover } from "@/lib/gemini";

import { saveBooksToLibrary, getLibraryBooks } from "@/app/actions";
import { get, del } from "idb-keyval";

export default function VerifyPage() {
  const [isProcessing, setIsProcessing] = useState(true);
  const [books, setBooks] = useState<Book[]>([]);
  const [image, setImage] = useState<string | null>(null);
  const [progressStep, setProgressStep] = useState(0);
  const [isSaving, setIsSaving] = useState(false);
  const [refetchingId, setRefetchingId] = useState<string | null>(null);
  const [libraryBooks, setLibraryBooks] = useState<Book[]>([]);
  const [showAuthModal, setShowAuthModal] = useState(false);
  const router = useRouter();

  useEffect(() => {
    const processImage = async () => {
      try {
        const savedImage = await get("captured_image");
        
        if (!savedImage) {
          router.push("/capture");
          return;
        }
        setImage(savedImage);

        // Fetch library books for duplicate checking
        const existingLibrary = await getLibraryBooks();
        setLibraryBooks(existingLibrary);

        // Step 1: Uploading the image (simulated fast step since it's already in IndexedDB)
        setProgressStep(1);
        await new Promise(resolve => setTimeout(resolve, 600));

        // Step 2: Running OCR
        setProgressStep(2);
        const detectedBooks = await identifyBooksFromImage(savedImage);
        
        // Step 3: Fetching Books information (metadata/covers already happening inside identifyBooksFromImage, but we mark it here)
        setProgressStep(3);
        await new Promise(resolve => setTimeout(resolve, 400));
        
        setBooks(detectedBooks);
      } catch (error) {
        console.error("Failed to process image", error);
      } finally {
        setProgressStep(4);
        setIsProcessing(false);
      }
    };

    processImage();
  }, [router]);

  const handleSave = async () => {
    setIsSaving(true);
    setShowAuthModal(false);
    try {
      const result = await saveBooksToLibrary(books);
      if (result.success) {
        await del("captured_image");
        router.push("/library");
      } else {
        alert("Failed to save books. Please try again.");
      }
    } catch (error) {
      console.error("Error saving books:", error);
      alert("An error occurred.");
    }
  };

  const handleConfirmClick = () => {
     setShowAuthModal(true);
  };

  const removeBook = (id: string) => {
    setBooks(books.filter(b => b.id !== id));
  };

  const handleRefetchCover = async (book: Book) => {
    setRefetchingId(book.id);
    try {
      const cover = await findBookCover(book.title, book.author, book.language);
      if (cover.url) {
        setBooks(books.map(b => b.id === book.id ? { ...b, coverImage: cover.url || undefined } : b));
      } else {
        alert("No cover found.");
      }
    } catch (e) {
      console.error("Error refetching cover", e);
    } finally {
      setRefetchingId(null);
    }
  };

  const isDuplicate = (book: Book) => {
    return libraryBooks.some(
      (libBook) => 
        libBook.title.toLowerCase().trim() === book.title.toLowerCase().trim() && 
        libBook.author.toLowerCase().trim() === book.author.toLowerCase().trim()
    );
  };

  if (isProcessing) {
    return (
      <div className="min-h-screen bg-zinc-50 flex flex-col items-center justify-center p-6 text-center">
        <div className="relative mb-8">
          <div className="w-24 h-24 bg-zinc-900 rounded-[2.5rem] flex items-center justify-center animate-pulse">
            <Sparkles className="w-12 h-12 text-white" />
          </div>
          <div className="absolute -inset-4 bg-zinc-900/5 rounded-full animate-ping -z-10" />
        </div>
        
        <h2 className="text-3xl font-bold mb-3 tracking-tight">The Magic is happening</h2>
        <p className="text-zinc-500 max-w-xs mx-auto mb-10">
          Gemini is analyzing your shelf to identify titles, authors, and covers.
        </p>

        <div className="w-full max-w-xs space-y-4">
          {[
            { label: "Uploading image", done: progressStep >= 1 },
            { label: "Running OCR", done: progressStep >= 2 },
            { label: "Fetching book information", done: progressStep >= 3 },
          ].map((step, i) => (
            <div key={i} className="flex items-center gap-3 text-left">
              <div className={cn(
                "w-5 h-5 rounded-full flex items-center justify-center transition-colors",
                step.done ? "bg-green-100 text-green-600" : "bg-zinc-100 text-zinc-300"
              )}>
                {step.done ? <Check className="w-3 h-3" /> : <Loader2 className="w-3 h-3 animate-spin" />}
              </div>
              <span className={cn("text-sm font-medium", step.done ? "text-zinc-900" : "text-zinc-400")}>
                {step.label}
              </span>
            </div>
          ))}
        </div>
      </div>
    );
  }

  const highConfidenceBooks = books.filter(b => !b.isUnidentified && b.confidence === 'High');
  const reviewNeededBooks = books.filter(b => !b.isUnidentified && b.confidence !== 'High');
  const unidentifiedBooks = books.filter(b => b.isUnidentified);

  const renderBookRow = (book: Book) => {
    const duplicate = isDuplicate(book);
    return (
      <div key={book.id} className={cn(
        "bg-white p-5 rounded-3xl border shadow-sm flex items-center gap-4 group transition-colors",
        book.isUnidentified ? "border-amber-200 bg-amber-50" : "border-zinc-100",
        duplicate && "border-blue-200 bg-blue-50"
      )}>
        <div className="relative w-12 h-16 bg-zinc-100 rounded-lg flex items-center justify-center text-zinc-400 overflow-hidden shrink-0 group/cover">
          {book.coverImage ? (
            <img src={book.coverImage} alt={book.title} className="w-full h-full object-cover" />
          ) : (
            <Check className="w-6 h-6" />
          )}
          
          <button
            onClick={() => handleRefetchCover(book)}
            className={cn(
              "absolute inset-0 bg-black/50 flex items-center justify-center transition-opacity",
              refetchingId === book.id ? "opacity-100" : "opacity-0 group-hover/cover:opacity-100"
            )}
            title="Refetch Cover"
          >
            <RefreshCw className={cn("w-4 h-4 text-white", refetchingId === book.id && "animate-spin")} />
          </button>
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            {book.isUnidentified && (
              <span className="px-2 py-0.5 bg-amber-200 text-amber-800 text-[10px] uppercase font-bold tracking-wider rounded-full flex items-center gap-1">
                <HelpCircle className="w-3 h-3" />
                Unidentified
              </span>
            )}
            {duplicate && (
              <span className="px-2 py-0.5 bg-blue-200 text-blue-800 text-[10px] uppercase font-bold tracking-wider rounded-full flex items-center gap-1">
                <AlertCircle className="w-3 h-3" />
                Duplicate
              </span>
            )}
            {!book.isUnidentified && book.confidence !== 'High' && (
               <span className="px-2 py-0.5 bg-zinc-200 text-zinc-600 text-[10px] uppercase font-bold tracking-wider rounded-full">
                 Review Needed
               </span>
            )}
          </div>
          <input
            value={book.title}
            onChange={(e) => setBooks(books.map(b => b.id === book.id ? { ...b, title: e.target.value } : b))}
            placeholder="Book Title"
            className="w-full font-semibold text-zinc-900 bg-transparent border-none p-0 focus:ring-0 placeholder:text-zinc-300"
          />
          <input
            value={book.author}
            onChange={(e) => setBooks(books.map(b => b.id === book.id ? { ...b, author: e.target.value } : b))}
            placeholder="Author Name"
            className="w-full text-sm text-zinc-500 bg-transparent border-none p-0 focus:ring-0 placeholder:text-zinc-200"
          />
        </div>
        <button 
          onClick={() => removeBook(book.id)}
          className="p-2 text-zinc-300 hover:text-red-500 transition-colors opacity-0 group-hover:opacity-100"
        >
          <Trash2 className="w-5 h-5" />
        </button>
      </div>
    );
  };

  return (
    <div className="min-h-screen bg-zinc-50 p-6 pb-32">
      <div className="max-w-2xl mx-auto">
        <header className="mb-8 flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-zinc-900">Verify Books</h1>
            <p className="text-zinc-500">We found {books.length} books. Make sure they're correct.</p>
          </div>
          <button 
            onClick={() => setBooks([...books, { id: Math.random().toString(), title: "", author: "" }])}
            className="p-3 bg-white border border-zinc-200 rounded-2xl hover:bg-zinc-50 transition-colors shadow-sm"
          >
            <Plus className="w-5 h-5 text-zinc-600" />
          </button>
        </header>

        <div className="space-y-8">
            {highConfidenceBooks.length > 0 && (
                <section>
                    <h2 className="text-sm font-semibold text-zinc-500 uppercase tracking-wider mb-3">High Confidence</h2>
                    <div className="space-y-3">
                        {highConfidenceBooks.map(renderBookRow)}
                    </div>
                </section>
            )}
            
            {reviewNeededBooks.length > 0 && (
                <section>
                    <h2 className="text-sm font-semibold text-zinc-500 uppercase tracking-wider mb-3">Review Needed</h2>
                    <div className="space-y-3">
                        {reviewNeededBooks.map(renderBookRow)}
                    </div>
                </section>
            )}

            {unidentifiedBooks.length > 0 && (
                <section>
                    <h2 className="text-sm font-semibold text-zinc-500 uppercase tracking-wider mb-3">Unidentified Spines</h2>
                    <div className="space-y-3">
                        {unidentifiedBooks.map(renderBookRow)}
                    </div>
                </section>
            )}
        </div>

        <div className="fixed bottom-0 left-0 right-0 p-6 bg-gradient-to-t from-zinc-50 via-zinc-50 to-transparent pointer-events-none">
          <div className="max-w-2xl mx-auto pointer-events-auto">
            <button
              onClick={handleConfirmClick}
              disabled={isSaving}
              className={cn(
                "w-full bg-zinc-900 text-white py-4 rounded-2xl font-semibold flex items-center justify-center gap-2 shadow-xl hover:bg-zinc-800 transition-all active:scale-95",
                isSaving && "opacity-70 cursor-not-allowed"
              )}
            >
              {isSaving ? (
                <>
                  <Loader2 className="w-5 h-5 animate-spin" />
                  Saving to Library...
                </>
              ) : (
                "Confirm & Add to Library"
              )}
            </button>
          </div>
        </div>
      </div>

      {showAuthModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-6 bg-black/40 backdrop-blur-sm">
          <div className="bg-white rounded-3xl p-8 w-full max-w-sm shadow-2xl animate-in fade-in zoom-in duration-200">
            <h3 className="text-xl font-bold text-zinc-900 mb-2">Save your library</h3>
            <p className="text-zinc-500 text-sm mb-6">
              Create an account to save your books and access them from any device.
            </p>
            
            <button
              onClick={handleSave}
              className="w-full bg-zinc-900 text-white py-3 rounded-xl font-semibold mb-3 hover:bg-zinc-800 transition-colors"
            >
              Sign up with Google
            </button>
            <button
              onClick={() => setShowAuthModal(false)}
              className="w-full py-3 text-zinc-500 font-medium hover:text-zinc-900 transition-colors"
            >
              Cancel
            </button>
          </div>
        </div>
      )}
    </div>
  );
}