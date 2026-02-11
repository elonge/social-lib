'use client';

import React, { useState } from 'react';

interface Book {
  title: string;
  author?: string;
  publisher?: string;
  year?: string;
  other_text?: string;
}

interface FrameResult {
  id: string;
  image: string;
  books: Book[];
  rawResult: any;
  selected: boolean;
}

export default function Home() {
  const [frames, setFrames] = useState<FrameResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [enriching, setEnriching] = useState(false);
  const [finalResult, setFinalResult] = useState<{ books: Book[], stats?: any } | null>(null);

  const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    if (!files) return;

    setLoading(true);
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      const formData = new FormData();
      formData.append('file', file);

      try {
        const response = await fetch('http://127.0.0.1:8000/upload_next_frame', {
          method: 'POST',
          body: formData,
        });

        const data = await response.json();
        
        // Create a preview URL for the image
        const reader = new FileReader();
        const imagePromise = new Promise<string>((resolve) => {
          reader.onloadend = () => resolve(reader.result as string);
          reader.readAsDataURL(file);
        });
        const imageUrl = await imagePromise;

        setFrames((prev) => [
          ...prev,
          {
            id: Math.random().toString(36).substr(2, 9),
            image: imageUrl,
            books: data.books || [],
            rawResult: data,
            selected: true,
          },
        ]);
      } catch (error) {
        console.error('Error uploading frame:', error);
      }
    }
    setLoading(false);
  };

  const handleCompleteUpload = async (enrich: boolean = false) => {
    const selectedFrames = frames.filter(f => f.selected);
    if (selectedFrames.length === 0) return;

    setEnriching(true);
    try {
      const response = await fetch('http://127.0.0.1:8000/complete_upload', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          results: selectedFrames.map(f => f.rawResult),
          enrich: enrich,
        }),
      });

      const data = await response.json();
      setFinalResult({
        books: data.books,
        stats: data.enrichment_stats
      });
    } catch (error) {
      console.error('Error completing upload:', error);
    }
    setEnriching(false);
  };

  const toggleFrameSelection = (id: string) => {
    setFrames(frames.map(f => f.id === id ? { ...f, selected: !f.selected } : f));
  };

  const removeFrame = (id: string) => {
    setFrames(frames.filter(f => f.id !== id));
  };

  const selectedCount = frames.filter(f => f.selected).length;

  return (
    <main className="min-h-screen p-8 bg-gray-50 text-gray-900">
      <div className="max-w-6xl mx-auto">
        <h1 className="text-4xl font-bold mb-8 text-center text-blue-600">Book Shelf Playground</h1>
        
        <div className="bg-white p-6 rounded-lg shadow-md mb-8">
          <h2 className="text-xl font-semibold mb-4">Step 1: Upload Frames</h2>
          <div className="flex items-center gap-4">
            <input
              type="file"
              multiple
              accept="image/*"
              onChange={handleFileUpload}
              className="block w-full text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded-full file:border-0 file:text-sm file:font-semibold file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100"
            />
            {loading && <span className="animate-pulse text-blue-500 font-medium">Processing...</span>}
          </div>
        </div>

        {frames.length > 0 && (
          <div className="mb-8">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-xl font-semibold">Step 2: Review Frames ({selectedCount} selected)</h2>
              <div className="flex gap-2">
                <button
                  onClick={() => handleCompleteUpload(false)}
                  disabled={enriching || selectedCount === 0}
                  className="bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700 disabled:bg-gray-400"
                >
                  Merge & Deduplicate
                </button>
                <button
                  onClick={() => handleCompleteUpload(true)}
                  disabled={enriching || selectedCount === 0}
                  className="bg-purple-600 text-white px-4 py-2 rounded-lg hover:bg-purple-700 disabled:bg-gray-400"
                >
                  {enriching ? 'Enriching...' : 'Merge & Enrich'}
                </button>
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {frames.map((frame) => (
                <div key={frame.id} className={`bg-white rounded-lg shadow-sm overflow-hidden border-2 transition-colors ${frame.selected ? 'border-blue-500' : 'border-gray-200 opacity-60'}`}>
                  <div className="relative h-48 cursor-pointer" onClick={() => toggleFrameSelection(frame.id)}>
                    <img src={frame.image} alt="frame" className="w-full h-full object-cover" />
                    <div className="absolute top-2 left-2">
                      <input 
                        type="checkbox" 
                        checked={frame.selected} 
                        onChange={() => {}} // Handled by div onClick
                        className="w-5 h-5"
                      />
                    </div>
                    <button 
                      onClick={(e) => { e.stopPropagation(); removeFrame(frame.id); }}
                      className="absolute top-2 right-2 bg-red-500 text-white w-8 h-8 rounded-full flex items-center justify-center hover:bg-red-600"
                    >
                      ×
                    </button>
                  </div>
                  <div className="p-4">
                    <p className="font-medium text-sm text-gray-500 mb-2">{frame.books.length} books detected</p>
                    <ul className="text-xs space-y-1 max-h-32 overflow-y-auto">
                      {frame.books.slice(0, 5).map((book, idx) => (
                        <li key={idx} className="truncate">
                          <span className="font-bold">{book.title}</span> {book.author && `- ${book.author}`}
                        </li>
                      ))}
                      {frame.books.length > 5 && <li className="text-gray-400 italic">...and {frame.books.length - 5} more</li>}
                    </ul>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {finalResult && (
          <div className="bg-white p-6 rounded-lg shadow-lg border-2 border-green-500 mt-8">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-2xl font-bold text-green-700">Final Results ({finalResult.books.length} unique books)</h2>
              <button 
                onClick={() => setFinalResult(null)}
                className="text-gray-400 hover:text-gray-600"
              >
                Clear Results
              </button>
            </div>
            {finalResult.stats && (
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6 bg-green-50 p-4 rounded-lg text-sm font-medium">
                <div>Google Books: {finalResult.stats.google_books_hits}</div>
                <div>OpenLibrary: {finalResult.stats.open_library_hits}</div>
                <div>Gemini calls: {finalResult.stats.gemini_calls}</div>
                <div>Duration: {finalResult.stats.total_duration_seconds.toFixed(1)}s</div>
              </div>
            )}
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200 border">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Title</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Author</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Publisher</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Year</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {finalResult.books.map((book, idx) => (
                    <tr key={idx} className="hover:bg-gray-50">
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-semibold">{book.title}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{book.author || '-'}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{book.publisher || '-'}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{book.year || '-'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </main>
  );
}
