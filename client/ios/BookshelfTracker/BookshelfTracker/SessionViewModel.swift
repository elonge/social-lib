import SwiftUI
import UIKit

class SessionViewModel: ObservableObject {
    @Published var capturedCount = 0
    @Published var pendingUploads = 0
    @Published var statusMessage = "Point at books to start"
    @Published var isTracking = false
    @Published var isRecording = false
    @Published var isFinalizing = false
    @Published var identifiedBooks: [Book] = []
    
    private var sessionResults: [[String: Any]] = []
    
    func startRecording() {
        print("🟢 UI: Starting session...")
        capturedCount = 0
        pendingUploads = 0
        sessionResults = []
        identifiedBooks = []
        isRecording = true
        statusMessage = "Scanning active"
    }
    
    func stopRecording() {
        print("🔴 UI: Stopping session. Frames snapped: \(capturedCount), Pending uploads: \(pendingUploads)")
        isRecording = false
        
        if pendingUploads > 0 {
            statusMessage = "Finishing uploads (\(pendingUploads) left)..."
            isFinalizing = true
        } else if !sessionResults.isEmpty {
            finalizeSession()
        } else {
            statusMessage = "No snapshots to process"
            print("⚠️ No results to finalize")
        }
    }
    
    func incrementCapturedCount() {
        capturedCount += 1
        pendingUploads += 1
    }
    
    func addResult(_ json: [String: Any]) {
        pendingUploads -= 1
        sessionResults.append(json)
        statusMessage = "Snap #\(sessionResults.count) processed"
        print("📥 Added result. Remaining pending: \(pendingUploads)")
        
        // If we were waiting for the last upload to finish after user clicked stop
        if !isRecording && pendingUploads == 0 {
            finalizeSession()
        }
    }
    
    func handleUploadFailure() {
        pendingUploads -= 1
        print("❌ Upload failed. Remaining pending: \(pendingUploads)")
        if !isRecording && pendingUploads == 0 {
            finalizeSession()
        }
    }
    
    private func finalizeSession() {
        guard !sessionResults.isEmpty else {
            isFinalizing = false
            statusMessage = "No books found in snapshots"
            return
        }
        
        print("🏁 Finalizing session with \(sessionResults.count) frames...")
        isFinalizing = true
        statusMessage = "Deduplicating books..."
        
        NetworkManager.shared.completeUpload(results: sessionResults) { [weak self] result in
            DispatchQueue.main.async {
                self?.isFinalizing = false
                switch result {
                case .success(let books):
                    print("📚 Server returned \(books.count) books.")
                    self?.identifiedBooks = books
                    if books.isEmpty {
                        self?.statusMessage = "Scan complete: No books identified"
                    } else {
                        self?.statusMessage = "Scan complete: Found \(books.count) books"
                    }
                case .failure(let error):
                    print("❌ Finalization API failed: \(error)")
                    self?.statusMessage = "Error: \(error.localizedDescription)"
                }
            }
        }
    }
}
