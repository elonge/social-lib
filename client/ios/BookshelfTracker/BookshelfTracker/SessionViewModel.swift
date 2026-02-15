import SwiftUI
import UIKit

enum PanoDirection {
    case left
    case right
    case unknown
}

enum PanoSpeedState {
    case idle
    case ok
    case tooFast
    case tooSlow
}

enum DistanceState {
    case optimal
    case tooClose
    case tooFar
    case unknown
}

struct PanoSnapshot: Identifiable {
    let id = UUID()
    let image: UIImage? // Optional to support missing skeletons
    let progress: CGFloat
}

class SessionViewModel: ObservableObject {
    @Published var capturedCount = 0
    @Published var pendingUploads = 0
    @Published var statusMessage = "Point at books to start"
    @Published var isTracking = false
    @Published var isRecording = false
    @Published var isFinalizing = false
    @Published var identifiedBooks: [Book] = []
    @Published var panoProgress: CGFloat = 0
    @Published var panoDirection: PanoDirection = .unknown
    @Published var panoSpeed: PanoSpeedState = .idle
    @Published var panoLevelOffset: CGFloat = 0
    @Published var panoLevelBroken: Bool = false
    @Published var panoRoll: CGFloat = 0
    @Published var panoPitch: CGFloat = 0
    @Published var panoYaw: CGFloat = 0
    @Published var panoSnapshots: [PanoSnapshot] = []
    @Published var distanceState: DistanceState = .unknown
    @Published var isDebugEnabled = true
    @Published var endOfShelfDetections: [String: Bool] = [:]
    @Published var isEndOfShelfDetected = false
    @Published var recordingDuration: TimeInterval = 0
    
    private var sessionResults: [[String: Any]] = []
    private let logFileURL = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0].appendingPathComponent("pano_debug.log")
    
    init() {
        clearLog()
    }
    
    func log(_ message: String) {
        guard isDebugEnabled else { return }
        let timestamp = ISO8601DateFormatter().string(from: Date())
        let logEntry = "[\(timestamp)] \(message)\n"
        
        if let data = logEntry.data(using: .utf8) {
            if FileManager.default.fileExists(atPath: logFileURL.path) {
                if let fileHandle = try? FileHandle(forWritingTo: logFileURL) {
                    fileHandle.seekToEndOfFile()
                    fileHandle.write(data)
                    fileHandle.closeFile()
                }
            } else {
                try? data.write(to: logFileURL)
            }
        }
    }
    
    func clearLog() {
        try? FileManager.default.removeItem(at: logFileURL)
    }
    
    func getLogURL() -> URL {
        return logFileURL
    }
    
    func startRecording() {
        print("🟢 UI: Starting session...")
        capturedCount = 0
        pendingUploads = 0
        sessionResults = []
        identifiedBooks = []
        isRecording = true
        panoProgress = 0
        panoDirection = .unknown
        panoSpeed = .idle
        panoLevelOffset = 0
        panoLevelBroken = false
        panoSnapshots = []
        statusMessage = "Scanning active"
    }
    
    func stopRecording() {
        print("🔴 UI: Stopping session. Frames snapped: \(capturedCount), Pending uploads: \(pendingUploads)")
        isRecording = false
        panoSpeed = .idle
        panoLevelOffset = 0
        panoLevelBroken = false
        
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
