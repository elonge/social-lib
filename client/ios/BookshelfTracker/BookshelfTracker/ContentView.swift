import SwiftUI
import ARKit
import RealityKit
import CoreImage
import UIKit

struct ContentView: View {
    @StateObject private var viewModel = SessionViewModel()
    @State private var showResults = false
    
    var body: some View {
        ZStack {
            // 1. The AR Camera View (Hidden during finalization)
            if !viewModel.isFinalizing {
                ARViewContainer(viewModel: viewModel)
                    .edgesIgnoringSafeArea(.all)
                    .transition(.opacity)
            } else {
                // Background for finalizing state
                Color.black.edgesIgnoringSafeArea(.all)
            }
            
            // 2. Finalizing UI (The "Nice Animation")
            if viewModel.isFinalizing {
                FinalizingView(status: viewModel.statusMessage)
                    .transition(.asymmetric(insertion: .opacity, removal: .move(edge: .bottom)))
            }
            
            // 3. UI Overlay (Only visible when not finalizing)
            if !viewModel.isFinalizing {
                VStack {
                    Spacer()
                    
                    HStack {
                        Circle()
                            .fill(viewModel.isTracking ? Color.green : Color.red)
                            .frame(width: 12, height: 12)
                        
                        Text(viewModel.statusMessage)
                            .font(.system(.caption, design: .monospaced))
                            .padding(8)
                            .background(.ultraThinMaterial)
                            .cornerRadius(8)
                        
                        Spacer()
                        
                        if viewModel.capturedCount > 0 {
                            Text("\(viewModel.capturedCount)")
                                .font(.title2.bold())
                                .foregroundColor(.white)
                                .frame(width: 44, height: 44)
                                .background(Color.blue)
                                .clipShape(Circle())
                                .shadow(radius: 4)
                        }
                    }
                    .padding()
                    
                    HStack {
                        Button(action: {
                            withAnimation(.spring()) {
                                if viewModel.isRecording {
                                    viewModel.stopRecording()
                                } else {
                                    viewModel.startRecording()
                                }
                            }
                        }) {
                            HStack {
                                Image(systemName: viewModel.isRecording ? "stop.fill" : "record.circle")
                                Text(viewModel.isRecording ? "Stop Scanning" : "Start Scanning")
                            }
                            .font(.headline)
                            .foregroundColor(.white)
                            .padding()
                            .frame(maxWidth: .infinity)
                            .background(viewModel.isRecording ? Color.red : Color.blue)
                            .cornerRadius(15)
                            .shadow(radius: 5)
                        }
                    }
                    .padding()
                    
                    if !viewModel.identifiedBooks.isEmpty && !viewModel.isRecording {
                        Button(action: { showResults = true }) {
                            Image(systemName: "books.vertical.fill")
                                .font(.title2)
                                .foregroundColor(.blue)
                                .padding()
                                .background(Color.white)
                                .clipShape(Circle())
                                .shadow(radius: 3)
                        }
                        .padding(.bottom)
                    }
                }
            }
        }
        .onChange(of: viewModel.identifiedBooks.count) { newValue in
            if newValue > 0 {
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
                    showResults = true
                }
            }
        }
        .sheet(isPresented: $showResults) {
            ResultsView(books: viewModel.identifiedBooks)
        }
    }
}

struct FinalizingView: View {
    let status: String
    @State private var isPulsing = false
    
    var body: some View {
        VStack(spacing: 30) {
            ZStack {
                Circle()
                    .stroke(Color.blue.opacity(0.3), lineWidth: 4)
                    .frame(width: 120, height: 120)
                    .scaleEffect(isPulsing ? 1.2 : 1.0)
                    .opacity(isPulsing ? 0.0 : 1.0)
                
                Circle()
                    .fill(Color.blue)
                    .frame(width: 80, height: 80)
                    .overlay(
                        Image(systemName: "sparkles")
                            .font(.largeTitle)
                            .foregroundColor(.white)
                    )
            }
            .onAppear {
                withAnimation(Animation.easeInOut(duration: 1.5).repeatForever(autoreverses: false)) {
                    isPulsing = true
                }
            }
            
            VStack(spacing: 10) {
                Text("Finalizing Library")
                    .font(.title2.bold())
                    .foregroundColor(.white)
                
                Text(status)
                    .font(.subheadline)
                    .foregroundColor(.gray)
                    .multilineTextAlignment(.center)
            }
            
            ProgressView()
                .progressViewStyle(CircularProgressViewStyle(tint: .white))
        }
        .padding(40)
        .background(Color.black.opacity(0.8))
        .cornerRadius(30)
        .shadow(color: .blue.opacity(0.3), radius: 20)
    }
}

struct ResultsView: View {
    let books: [Book]
    @Environment(\.dismiss) var dismiss
    
    var body: some View {
        NavigationView {
            List(books) { book in
                HStack(spacing: 15) {
                    if let coverLink = book.coverLink, let coverURL = URL(string: coverLink) {
                        CoverImageView(url: coverURL)
                            .frame(width: 48, height: 72)
                            .clipShape(RoundedRectangle(cornerRadius: 6))
                    } else {
                        Image(systemName: "book.closed.fill")
                            .foregroundColor(.blue)
                            .font(.title2)
                    }
                    
                    VStack(alignment: .leading, spacing: 4) {
                        Text(book.title ?? "Unknown Title")
                            .font(.headline)
                        if let author = book.author {
                            Text(author)
                                .font(.subheadline)
                                .foregroundColor(.secondary)
                        }
                        if let publisher = book.publisher {
                            Text(publisher)
                                .font(.caption)
                                .foregroundColor(.secondary)
                        }
                    }
                }
                .padding(.vertical, 4)
            }
            .navigationTitle("Shelf Summary")
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }
}

final class CoverImageLoader: ObservableObject {
    @Published var image: UIImage?
    @Published var didFail = false
    
    private static let downloadQueue = DispatchQueue(label: "cover-image-download", qos: .utility)
    private static let semaphore = DispatchSemaphore(value: 1)
    
    private let url: URL
    private var isLoading = false
    private var attempt = 0
    private let maxRetries = 4
    private let baseDelay: TimeInterval = 1.5
    
    init(url: URL) {
        self.url = url
    }
    
    func load() {
        guard !isLoading else { return }
        isLoading = true
        CoverImageLoader.downloadQueue.async {
            CoverImageLoader.semaphore.wait()
            self.startRequest()
        }
    }
    
    private func startRequest() {
        var request = URLRequest(url: url)
        request.cachePolicy = .returnCacheDataElseLoad
        
        URLSession.shared.dataTask(with: request) { data, response, error in
            defer {
                CoverImageLoader.semaphore.signal()
                if self.image != nil || self.didFail {
                    self.isLoading = false
                }
            }
            
            if let error = error {
                print("🖼️ Cover fetch error: \(error.localizedDescription) | url=\(self.url.absoluteString)")
                DispatchQueue.main.async {
                    self.didFail = true
                }
                return
            }
            
            if let http = response as? HTTPURLResponse {
                if http.statusCode == 429, self.attempt < self.maxRetries {
                    let retryDelay = self.retryDelaySeconds(from: http) ?? self.backoffDelay(for: self.attempt)
                    self.attempt += 1
                    print("🖼️ Cover fetch HTTP 429 | retrying in \(String(format: "%.2f", retryDelay))s (attempt \(self.attempt)/\(self.maxRetries)) | url=\(self.url.absoluteString)")
                    CoverImageLoader.downloadQueue.asyncAfter(deadline: .now() + retryDelay) {
                        CoverImageLoader.semaphore.wait()
                        self.startRequest()
                    }
                    return
                }
                
                if http.statusCode != 200 {
                    print("🖼️ Cover fetch HTTP \(http.statusCode) | url=\(self.url.absoluteString)")
                }
            } else if response != nil {
                print("🖼️ Cover fetch non-HTTP response | url=\(self.url.absoluteString)")
            }
            
            guard let data = data, !data.isEmpty else {
                print("🖼️ Cover fetch empty body | url=\(self.url.absoluteString)")
                DispatchQueue.main.async {
                    self.didFail = true
                }
                return
            }
            
            if let image = UIImage(data: data) {
                DispatchQueue.main.async {
                    self.image = image
                }
            } else {
                print("🖼️ Cover fetch invalid image data | url=\(self.url.absoluteString)")
                DispatchQueue.main.async {
                    self.didFail = true
                }
            }
        }.resume()
    }
    
    private func retryDelaySeconds(from response: HTTPURLResponse) -> TimeInterval? {
        guard let retryAfter = response.value(forHTTPHeaderField: "Retry-After") else { return nil }
        if let seconds = TimeInterval(retryAfter.trimmingCharacters(in: .whitespacesAndNewlines)) {
            return seconds
        }
        return nil
    }
    
    private func backoffDelay(for attempt: Int) -> TimeInterval {
        let exponent = min(attempt, 6)
        let jitter = Double.random(in: 0.0...0.3)
        return (baseDelay * pow(2.0, Double(exponent))) + jitter
    }
}

struct CoverImageView: View {
    @StateObject private var loader: CoverImageLoader
    
    init(url: URL) {
        _loader = StateObject(wrappedValue: CoverImageLoader(url: url))
    }
    
    var body: some View {
        ZStack {
            RoundedRectangle(cornerRadius: 6)
                .fill(Color.gray.opacity(0.2))
            if let image = loader.image {
                Image(uiImage: image)
                    .resizable()
                    .scaledToFill()
            } else if loader.didFail {
                Image(systemName: "book.closed.fill")
                    .foregroundColor(.blue)
                    .font(.title2)
            } else {
                ProgressView()
            }
        }
        .onAppear { loader.load() }
    }
}

// The AR Logic
struct ARViewContainer: UIViewRepresentable {
    @ObservedObject var viewModel: SessionViewModel
    
    func makeUIView(context: Context) -> ARSCNView {
        let arView = ARSCNView(frame: .zero)
        arView.delegate = context.coordinator
        arView.session.delegate = context.coordinator
        
        let config = ARWorldTrackingConfiguration()
        config.planeDetection = [.vertical]
        arView.session.run(config)
        
        return arView
    }
    
    func updateUIView(_ uiView: ARSCNView, context: Context) {
        context.coordinator.viewModel = viewModel
    }
    
    func makeCoordinator() -> Coordinator {
        Coordinator(viewModel: viewModel)
    }
    
    class Coordinator: NSObject, ARSCNViewDelegate, ARSessionDelegate {
        var viewModel: SessionViewModel
        var lastAnchorPoint: simd_float3?
        var isCoolingDown = false
        
        init(viewModel: SessionViewModel) {
            self.viewModel = viewModel
        }
        
        func session(_ session: ARSession, didUpdate frame: ARFrame) {
            let state = frame.camera.trackingState
            DispatchQueue.main.async {
                self.viewModel.isTracking = (state == .normal)
            }
            
            guard !isCoolingDown, viewModel.isRecording, case .normal = state else { return }
            
            guard let anchor = lastAnchorPoint else {
                setNewAnchor(frame: frame)
                return
            }
            
            let cam = frame.camera
            let screenPoint = cam.projectPoint(anchor, orientation: .portrait, viewportSize: CGSize(width: 1, height: 1))
            
            if screenPoint.x < 0.2 || screenPoint.x > 0.8 {
                takeSnapshot(frame: frame)
            }
        }
        
        func setNewAnchor(frame: ARFrame) {
            let results = frame.hitTest(CGPoint(x: 0.5, y: 0.5), types: [.existingPlaneUsingExtent, .featurePoint])
            if let hit = results.first {
                let translation = hit.worldTransform.columns.3
                lastAnchorPoint = simd_float3(translation.x, translation.y, translation.z)
            }
        }
        
        func takeSnapshot(frame: ARFrame) {
            isCoolingDown = true
            
            DispatchQueue.main.async {
                self.viewModel.incrementCapturedCount()
                UIImpactFeedbackGenerator(style: .light).impactOccurred()
            }
            
            let pixelBuffer = frame.capturedImage
            DispatchQueue.global(qos: .userInitiated).async {
                let ciImage = CIImage(cvPixelBuffer: pixelBuffer)
                let context = CIContext(options: [.useSoftwareRenderer: false])
                guard let cgImage = context.createCGImage(ciImage, from: ciImage.extent) else {
                    DispatchQueue.main.async { self.viewModel.handleUploadFailure() }
                    return
                }
                
                let uiImage = UIImage(cgImage: cgImage, scale: 1.0, orientation: .right)
                
                NetworkManager.shared.uploadFrame(image: uiImage) { result in
                    DispatchQueue.main.async {
                        switch result {
                        case .success(let json):
                            self.viewModel.addResult(json)
                        case .failure(let error):
                            self.viewModel.handleUploadFailure()
                        }
                    }
                }
            }
            
            setNewAnchor(frame: frame)
            
            DispatchQueue.global().asyncAfter(deadline: .now() + 1.2) {
                self.isCoolingDown = false
            }
        }
    }
}
