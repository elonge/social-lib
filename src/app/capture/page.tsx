"use client";

import { useState, useRef } from "react";
import { useRouter } from "next/navigation";
import { Camera, Upload, X, ArrowRight } from "lucide-react";
import { cn } from "@/lib/utils";
import { set } from "idb-keyval";

export default function CapturePage() {
  const [image, setImage] = useState<string | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const router = useRouter();

  const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      if (file.name.toLowerCase().endsWith(".heic") || file.type === "image/heic") {
        try {
          const heic2any = (await import("heic2any")).default;
          const convertedBlob = await heic2any({
            blob: file,
            toType: "image/jpeg",
            quality: 0.8,
          });
          
          const blob = Array.isArray(convertedBlob) ? convertedBlob[0] : convertedBlob;
          const reader = new FileReader();
          reader.onloadend = () => {
            setImage(reader.result as string);
          };
          reader.readAsDataURL(blob);
          return;
        } catch (error) {
          console.error("Error converting HEIC:", error);
          alert("Failed to process HEIC image");
          return;
        }
      }

      const reader = new FileReader();
      reader.onloadend = () => {
        setImage(reader.result as string);
      };
      reader.readAsDataURL(file);
    }
  };

  const handleCapture = async () => {
    if (!image) return;
    setIsUploading(true);
    
    try {
      // Store in IndexedDB to avoid quota limits
      await set("captured_image", image);
      router.push("/verify");
    } catch (error) {
      console.error("Failed to save image locally:", error);
      alert("Failed to save image. Please try again.");
      setIsUploading(false);
    }
  };

  return (
    <div className="min-h-screen bg-zinc-50 flex flex-col items-center justify-center p-6">
      <div className="w-full max-w-md bg-white rounded-3xl shadow-xl overflow-hidden">
        <div className="p-8">
          <h1 className="text-2xl font-bold mb-2 text-zinc-900">Capture your books</h1>
          <p className="text-zinc-500 mb-8 text-sm">
            Take a clear photo of your bookshelf or a stack of covers.
          </p>

          <div 
            onClick={() => !image && fileInputRef.current?.click()}
            className={cn(
              "relative aspect-[3/4] rounded-2xl border-2 border-dashed border-zinc-200 flex flex-col items-center justify-center cursor-pointer transition-all overflow-hidden",
              image ? "border-none" : "hover:border-zinc-300 hover:bg-zinc-50"
            )}
          >
            {image ? (
              <>
                <img src={image} alt="Captured books" className="w-full h-full object-cover" />
                <button 
                  onClick={(e) => {
                    e.stopPropagation();
                    setImage(null);
                  }}
                  className="absolute top-4 right-4 p-2 bg-black/50 text-white rounded-full hover:bg-black/70 transition-colors"
                >
                  <X className="w-5 h-5" />
                </button>
              </>
            ) : (
              <div className="flex flex-col items-center gap-4 text-zinc-400">
                <div className="p-4 bg-zinc-100 rounded-full">
                  <Camera className="w-8 h-8" />
                </div>
                <span className="text-sm font-medium">Click to take photo or upload</span>
              </div>
            )}
          </div>

          <input 
            type="file" 
            ref={fileInputRef} 
            className="hidden" 
            accept="image/*,.heic,.heif" 
            onChange={handleFileChange}
          />

          <div className="mt-8 flex flex-col gap-3">
            <button
              disabled={!image || isUploading}
              onClick={handleCapture}
              className={cn(
                "w-full py-4 rounded-2xl font-semibold flex items-center justify-center gap-2 transition-all cursor-pointer",
                image && !isUploading 
                  ? "bg-zinc-900 text-white hover:bg-zinc-800" 
                  : "bg-zinc-100 text-zinc-400 cursor-not-allowed"
              )}
            >
              {isUploading ? (
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                  Processing...
                </div>
              ) : (
                <>
                  Analyze Image
                  <ArrowRight className="w-4 h-4" />
                </>
              )}
            </button>
            
            <button
              onClick={() => router.back()}
              className="w-full py-4 text-zinc-500 font-medium hover:text-zinc-700 transition-colors"
            >
              Cancel
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
