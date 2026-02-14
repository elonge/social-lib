'use client';

import React, { useState } from 'react';
import { useAuth } from '../context/AuthContext';

interface Book {
  title: string;
  author?: string;
  publisher?: string;
  year?: string;
  other_text?: string;
  cover_link?: string;
}

interface FrameResult {
  id: string;
  image?: string;
  books: Book[];
  rawResult: any;
  selected: boolean;
  status: 'uploading' | 'processing' | 'complete' | 'error';
}

export default function Home() {
  const { user, logout } = useAuth();
  const [frames, setFrames] = useState<FrameResult[]>([]);
  const [enriching, setEnriching] = useState(false);
  const [finalResult, setFinalResult] = useState<{ books: Book[], stats?: any } | null>(null);
  const [viewingFrame, setViewingFrame] = useState<FrameResult | null>(null);

  // Helper to ensure auth token is ready
  const getAuthHeaders = async () => {
    // apiRequest handles token automatically, but for FormData we might need manual token if we don't use apiRequest (which supports json mostly)
    // Actually apiRequest in lib/api.ts is designed for JSON.
    // Let's import apiRequest and use it.
    // CAUTION: apiRequest assumes JSON content-type by default unless body is FormData.
    // We should verify api.ts support for FormData.
    return {};
  }

  const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    if (!files) return;

    if (!user) {
      alert("Please login first");
      return;
    }

    const newFrames: FrameResult[] = [];
    for (let i = 0; i < files.length; i++) {
      const id = Math.random().toString(36).substr(2, 9);
      newFrames.push({
        id,
        books: [],
        rawResult: null,
        selected: true,
        status: 'uploading',
      });
    }
    setFrames((prev) => [...prev, ...newFrames]);

    // Import apiRequest dynamically or at top level (doing it here for this block context)
    const { apiRequest } = await import('../lib/api');

    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      const placeholderId = newFrames[i].id;

      const reader = new FileReader();
      reader.onloadend = () => {
        setFrames(prev => prev.map(f => f.id === placeholderId ? { ...f, image: reader.result as string } : f));
      };
      reader.readAsDataURL(file);

      const formData = new FormData();
      formData.append('file', file);
      // user_id is NOT sent anymore, server extracts it from token

      try {
        setFrames(prev => prev.map(f => f.id === placeholderId ? { ...f, status: 'processing' } : f));

        // apiRequest automatically attaches valid Firebase ID token
        // We need to make sure apiRequest handles FormData correctly (it should if we don't set Content-Type manually to json)
        const data = await apiRequest('/upload_frame', {
          method: 'POST',
          body: formData,
        });

        setFrames((prev) => prev.map(f => f.id === placeholderId ? {
          ...f,
          books: data.books || [],
          rawResult: data,
          status: 'complete'
        } : f));

      } catch (error) {
        console.error('Error uploading frame:', error);
        setFrames((prev) => prev.map(f => f.id === placeholderId ? { ...f, status: 'error' } : f));
      }
    }
    event.target.value = '';
  };

  const handleCompleteUpload = async (enrich: boolean = false) => {
    const selectedFrames = frames.filter(f => f.selected && f.status === 'complete');
    if (selectedFrames.length === 0) return;

    if (!user) {
      alert("Please login first");
      return;
    }

    setEnriching(true);
    const { apiRequest } = await import('../lib/api');

    try {
      console.log('Completing upload with frames:', selectedFrames.map(f => f.id), 'Enrich:', enrich);
      const data = await apiRequest('/complete_upload', {
        method: 'POST',
        body: JSON.stringify({
          results: selectedFrames.map(f => f.rawResult),
          enrich: enrich,
          // user_id removed
        }),
      });

      console.log('Complete upload response:', data);
      setFinalResult({
        books: data.books,
        stats: data.deduplication_stats // Updated key name from response
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

  const selectedCount = frames.filter(f => f.selected && f.status === 'complete').length;
  const isAnyProcessing = frames.some(f => f.status === 'uploading' || f.status === 'processing');

  return (
    <main className="min-h-screen p-8 bg-gray-50 text-gray-900">
      <div className="max-w-6xl mx-auto">
        <div className="flex justify-between items-center mb-8">
          <h1 className="text-4xl font-bold text-blue-600">Book Shelf Playground</h1>
          <div>
            {user ? (
              <div className="flex items-center gap-4">
                <span className="text-gray-700">Welcome, {user.displayName}</span>
                <button
                  onClick={logout}
                  className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition"
                >
                  Logout
                </button>
              </div>
            ) : (
              <a
                href="/login"
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
              >
                Login
              </a>
            )}
          </div>
        </div>

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
            {isAnyProcessing && <span className="animate-pulse text-blue-500 font-medium">Processing frames...</span>}
          </div>
        </div>

        {frames.length > 0 && (
          <div className="mb-8">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-xl font-semibold">Step 2: Review Frames ({selectedCount} selected)</h2>
              <div className="flex gap-2">
                <button
                  onClick={() => handleCompleteUpload(false)}
                  disabled={enriching || selectedCount === 0 || isAnyProcessing}
                  className="bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700 disabled:bg-gray-400 font-medium"
                >
                  Merge & Deduplicate
                </button>
                <button
                  onClick={() => handleCompleteUpload(true)}
                  disabled={enriching || selectedCount === 0 || isAnyProcessing}
                  className="bg-purple-600 text-white px-4 py-2 rounded-lg hover:bg-purple-700 disabled:bg-gray-400 font-medium"
                >
                  {enriching ? 'Enriching...' : 'Merge & Enrich'}
                </button>
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {frames.map((frame) => (
                <div key={frame.id} className={`bg-white rounded-lg shadow-sm overflow-hidden border-2 transition-all ${frame.selected ? 'border-blue-500' : 'border-gray-200 opacity-60'}`}>
                  <div className="relative h-48 cursor-pointer" onClick={() => toggleFrameSelection(frame.id)}>
                    {frame.image ? (
                      <img src={frame.image} alt="frame" className="w-full h-full object-cover" />
                    ) : (
                      <div className="w-full h-full bg-gray-200 animate-pulse flex items-center justify-center text-gray-400">
                        Loading image...
                      </div>
                    )}

                    {frame.status !== 'complete' && (
                      <div className="absolute inset-0 bg-black/40 flex items-center justify-center">
                        <div className="bg-white px-3 py-1 rounded-full text-xs font-bold uppercase tracking-wider animate-bounce">
                          {frame.status}
                        </div>
                      </div>
                    )}

                    <div className="absolute top-2 left-2">
                      <input
                        type="checkbox"
                        checked={frame.selected}
                        onChange={() => { }}
                        disabled={frame.status !== 'complete'}
                        className="w-5 h-5 cursor-pointer"
                      />
                    </div>
                    <button
                      onClick={(e) => { e.stopPropagation(); removeFrame(frame.id); }}
                      className="absolute top-2 right-2 bg-red-500 text-white w-8 h-8 rounded-full flex items-center justify-center hover:bg-red-600 transition-colors"
                    >
                      ×
                    </button>
                  </div>
                  <div className="p-4">
                    {frame.status === 'complete' ? (
                      <>
                        <div className="flex justify-between items-center mb-2">
                          <p className="font-medium text-sm text-gray-500">{frame.books.length} books detected</p>
                          {frame.books.length > 0 && (
                            <button
                              onClick={(e) => { e.stopPropagation(); setViewingFrame(frame); }}
                              className="text-xs text-blue-600 hover:underline font-semibold"
                            >
                              See all
                            </button>
                          )}
                        </div>
                        <ul className="text-xs space-y-1 max-h-32 overflow-hidden">
                          {frame.books.slice(0, 5).map((book, idx) => (
                            <li key={idx} className="truncate">
                              <span className="font-bold">{book.title || 'Unknown Title'}</span> {book.author && `- ${book.author}`}
                            </li>
                          ))}
                          {frame.books.length > 5 && <li className="text-gray-400 italic">...and {frame.books.length - 5} more</li>}
                          {frame.books.length === 0 && <li className="text-gray-400 italic">No books identified</li>}
                        </ul>
                      </>
                    ) : (
                      <div className="space-y-2">
                        <div className="h-3 bg-gray-200 rounded w-3/4 animate-pulse"></div>
                        <div className="h-2 bg-gray-200 rounded w-1/2 animate-pulse"></div>
                        <div className="h-2 bg-gray-200 rounded w-5/6 animate-pulse"></div>
                      </div>
                    )}
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
                className="text-gray-400 hover:text-gray-600 font-medium"
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
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Cover</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Author</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Publisher</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Year</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {finalResult.books.map((book, idx) => (
                    <tr key={idx} className="hover:bg-gray-50">
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-semibold">{book.title}</td>
                      <td className="px-6 py-4 text-sm">
                        {book.cover_link ? (
                          <div className="flex items-center gap-3 min-w-0">
                            <img
                              src={book.cover_link}
                              alt={`${book.title || 'Book'} cover`}
                              className="h-16 w-12 object-cover rounded border border-gray-200 shadow-sm"
                            />
                            <a
                              href={book.cover_link}
                              target="_blank"
                              rel="noreferrer"
                              className="text-xs text-blue-600 hover:underline max-w-[240px] truncate"
                            >
                              {book.cover_link}
                            </a>
                          </div>
                        ) : (
                          <span className="text-gray-400">-</span>
                        )}
                      </td>
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

      {/* Frame Detail Modal */}
      {viewingFrame && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center p-4 z-50 overflow-y-auto">
          <div className="bg-white rounded-xl max-w-2xl w-full max-h-[90vh] flex flex-col shadow-2xl">
            <div className="p-4 border-b flex justify-between items-center sticky top-0 bg-white rounded-t-xl">
              <h3 className="text-xl font-bold">Books in Frame</h3>
              <button
                onClick={() => setViewingFrame(null)}
                className="text-gray-500 hover:text-gray-700 text-2xl"
              >
                ×
              </button>
            </div>
            <div className="flex-1 overflow-y-auto p-4">
              <div className="mb-6 rounded-lg overflow-hidden border">
                <img src={viewingFrame.image} alt="frame" className="w-full h-auto" />
              </div>
              <table className="min-w-full divide-y divide-gray-200 border">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-2 text-left text-xs font-bold text-gray-600 uppercase">Title</th>
                    <th className="px-4 py-2 text-left text-xs font-bold text-gray-600 uppercase">Author</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {viewingFrame.books.map((book, idx) => (
                    <tr key={idx} className="hover:bg-gray-50">
                      <td className="px-4 py-2 text-sm font-medium">{book.title || 'Unknown'}</td>
                      <td className="px-4 py-2 text-sm text-gray-600">{book.author || '-'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="p-4 border-t text-right">
              <button
                onClick={() => setViewingFrame(null)}
                className="bg-gray-100 px-4 py-2 rounded-lg hover:bg-gray-200 font-medium transition-colors"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </main>
  );
}
