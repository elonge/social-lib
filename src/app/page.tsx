"use client";

import Link from "next/link";
import { Book, Camera, Sparkles, Library } from "lucide-react";

export default function Home() {
  return (
    <div className="flex min-h-screen flex-col items-center bg-white text-zinc-900 font-sans">
      <main className="flex-1 flex flex-col items-center justify-center px-6 text-center max-w-4xl mx-auto">
        <div className="mb-8 p-4 bg-zinc-100 rounded-2xl">
          <Library className="w-12 h-12 text-zinc-900" />
        </div>
        
        <h1 className="text-5xl font-bold tracking-tight mb-4">
          The Social Library
        </h1>
        
        <p className="text-xl text-zinc-600 mb-12 max-w-lg">
          Digitize your physical library in seconds just by taking a picture.
        </p>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-8 mb-16 text-left">
          <div className="p-6 border border-zinc-100 rounded-2xl hover:border-zinc-200 transition-colors">
            <Camera className="w-6 h-6 mb-4 text-zinc-900" />
            <h3 className="font-semibold mb-2">Capture</h3>
            <p className="text-sm text-zinc-500">Snap a photo of your bookshelf or a stack of covers.</p>
          </div>
          <div className="p-6 border border-zinc-100 rounded-2xl hover:border-zinc-200 transition-colors">
            <Sparkles className="w-6 h-6 mb-4 text-zinc-900" />
            <h3 className="font-semibold mb-2">Gemini Magic</h3>
            <p className="text-sm text-zinc-500">Our AI identifies every book and fetches details automatically.</p>
          </div>
          <div className="p-6 border border-zinc-100 rounded-2xl hover:border-zinc-200 transition-colors">
            <Book className="w-6 h-6 mb-4 text-zinc-900" />
            <h3 className="font-semibold mb-2">Digital Shelf</h3>
            <p className="text-sm text-zinc-500">Organize, share, and track your physical collection digitally.</p>
          </div>
        </div>

        <div className="flex flex-col sm:flex-row gap-4">
          <Link
            href="/capture"
            className="bg-zinc-900 text-white px-8 py-4 rounded-full text-lg font-semibold hover:bg-zinc-800 transition-all transform hover:scale-105"
          >
            Get Started
          </Link>
          <button
            onClick={() => alert("Google Auth placeholder")}
            className="bg-white text-zinc-900 border border-zinc-200 px-8 py-4 rounded-full text-lg font-semibold hover:bg-zinc-50 transition-all"
          >
            Sign In
          </button>
        </div>
      </main>

      <footer className="py-8 text-zinc-400 text-sm flex flex-col gap-2 items-center">
        <span>Built for book lovers.</span>
        <Link href="/share" className="text-zinc-300 hover:text-zinc-500 underline underline-offset-4">
          View Shared Library Demo
        </Link>
      </footer>
    </div>
  );
}