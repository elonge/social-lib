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
                DistanceGuidanceView(state: viewModel.distanceState)
                PanoOverlayView(viewModel: viewModel, showResults: $showResults)
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

struct PanoOverlayView: View {
    @ObservedObject var viewModel: SessionViewModel
    @Binding var showResults: Bool
    
    static func angleStatus(for viewModel: SessionViewModel) -> (text: String, color: Color) {
        let roll = viewModel.panoRoll
        let pitch = viewModel.panoPitch
        let yaw = viewModel.panoYaw
        
        let adjustedRoll = roll + 90
        let debugInfo = " [R:\(Int(roll)) P:\(Int(pitch)) Y:\(Int(yaw))]"
        
        // New Ultra-Lenient Thresholds
        // Roll: Perfect 0-15, Warning 15-25, Error 25+
        if abs(adjustedRoll) > 25 { return ("KEEP IPHONE VERTICAL" + debugInfo, .red) }
        // Pitch: Perfect 0-35, Warning 35-45, Error 45+
        if abs(pitch) > 45 { return ("TILT IPHONE STRAIGHT" + debugInfo, .red) }
        // Yaw: Perfect 0-35, Warning 35-55, Error 55+
        if abs(yaw) > 55 { return ("DONT ROTATE IPHONE" + debugInfo, .red) }
        
        if abs(adjustedRoll) > 15 { return ("KEEP IPHONE VERTICAL" + debugInfo, .yellow) }
        if abs(pitch) > 35 { return ("TILT IPHONE STRAIGHT" + debugInfo, .yellow) }
        if abs(yaw) > 35 { return ("DONT ROTATE IPHONE" + debugInfo, .yellow) }
        
        return (debugInfo, .white)
    }
    
    static func statusText(for viewModel: SessionViewModel) -> String {
        if !viewModel.isRecording { return "TAP SHUTTER TO START" }
        if !viewModel.isTracking { return "MOVE IPHONE TO IMPROVE TRACKING" }
        
        // 1. Distance Check (Blocks Snapping)
        switch viewModel.distanceState {
        case .tooClose: return "TOO CLOSE - MOVE BACK"
        case .tooFar: return "TOO FAR - MOVE CLOSER"
        case .unknown: return "FINDING WALL..."
        case .optimal: break
        }
        
        // 2. Angle Check (Blocks Snapping)
        let angle = angleStatus(for: viewModel)
        // Check if there is an actual instruction before the debug bracket
        if let firstPart = angle.text.components(separatedBy: " [").first, !firstPart.isEmpty {
            return firstPart
        }
        
        // 3. Speed Check (Warning only)
        switch viewModel.panoSpeed {
        case .tooFast: return "SLOW DOWN"
        default: return "" // "MOVE IPHONE CONTINUOUSLY" replaced by green dot
        }
    }
    
    static func statusColor(for viewModel: SessionViewModel) -> Color {
        if !viewModel.isRecording || !viewModel.isTracking { return .white }
        
        if viewModel.distanceState != .optimal { return .red }
        
        let angle = angleStatus(for: viewModel)
        if angle.text.count > 20 { return angle.color }
        
        if viewModel.panoSpeed == .tooFast { return .yellow }
        return .green // All good
    }
    
    var body: some View {
        ZStack {
            VStack(spacing: 0) {
                LinearGradient(colors: [Color.black.opacity(0.85), Color.black.opacity(0.0)],
                               startPoint: .top, endPoint: .bottom)
                    .frame(height: 140)
                Spacer()
                LinearGradient(colors: [Color.black.opacity(0.0), Color.black.opacity(0.85)],
                               startPoint: .top, endPoint: .bottom)
                    .frame(height: 300) // Increased bottom gradient for moved UI
            }
            .ignoresSafeArea()
            
            VStack(spacing: 0) {
                // Top Header
                VStack(spacing: 6) {
                    Text("PANO")
                        .font(.system(size: 12, weight: .semibold, design: .monospaced))
                        .foregroundColor(.white.opacity(0.85))
                        .tracking(2)
                    
                    let text = PanoOverlayView.statusText(for: viewModel)
                    if text.isEmpty && viewModel.isRecording {
                        Circle()
                            .fill(.green)
                            .frame(width: 8, height: 8)
                            .shadow(color: .green.opacity(0.5), radius: 4)
                    } else {
                        Text(text)
                            .font(.system(size: 14, weight: .bold, design: .monospaced))
                            .foregroundColor(PanoOverlayView.statusColor(for: viewModel))
                            .multilineTextAlignment(.center)
                    }
                }
                .padding(.top, 18)
                
                Spacer()
                
                // Middle Guide (Now at the bottom)
                PanoGuideView(viewModel: viewModel,
                              progress: viewModel.panoProgress,
                              direction: viewModel.panoDirection,
                              isRecording: viewModel.isRecording,
                              levelOffset: viewModel.panoLevelOffset,
                              isLevelBroken: viewModel.panoLevelBroken)
                    .padding(.bottom, 20)
                
                PanoBottomBar(viewModel: viewModel, showResults: $showResults)
                    .padding(.bottom, 18)
            }
            .padding(.horizontal, 24)
        }
    }
}

struct PanoSnapshotView: View {
    let snapshot: PanoSnapshot
    let height: CGFloat
    let width: CGFloat
    @State private var flashOpacity: Double = 1.0
    
    var body: some View {
        ZStack(alignment: .leading) {
            if let image = snapshot.image {
                Image(uiImage: image)
                    .resizable()
                    .scaledToFill()
                    .frame(width: width, height: height)
                    .clipped()
                    .overlay(Color.white.opacity(flashOpacity))
                    .onAppear {
                        withAnimation(.linear(duration: 0.1)) {
                            flashOpacity = 0
                        }
                    }
            } else {
                // Missing Skeleton - Perfectly aligned to the frame size
                RoundedRectangle(cornerRadius: 1)
                    .strokeBorder(Color.red.opacity(0.8), style: StrokeStyle(lineWidth: 1.5, dash: [4]))
                    .background(Color.red.opacity(0.1))
                    .frame(width: width, height: height)
                    .overlay(
                        Image(systemName: "camera.badge.ellipsis")
                            .font(.system(size: 10))
                            .foregroundColor(.red.opacity(0.8))
                    )
            }
        }
        .frame(width: width, height: height, alignment: .leading)
    }
}

struct PanoGuideView: View {
    @ObservedObject var viewModel: SessionViewModel
    let progress: CGFloat
    let direction: PanoDirection
    let isRecording: Bool
    let levelOffset: CGFloat
    let isLevelBroken: Bool
    
    private let guideWidth: CGFloat = 310
    private let guideHeight: CGFloat = 100
    private let lineHeight: CGFloat = 1
    
    // Width of a single "slice" so 10 slices fill the 310pt bar perfectly
    private var sliceWidth: CGFloat {
        guideWidth / 10.0
    }
    
    private var clampedProgress: CGFloat {
        max(0.0, min(progress, 1.0))
    }
    
    private var arrowX: CGFloat {
        // Lead by one full slice width (31px) minus half the arrow width (approx 12px)
        (guideWidth * clampedProgress) + sliceWidth - 12
    }
    
    var body: some View {
        VStack(spacing: 24) {
            ZStack(alignment: .leading) {
                // Background Track
                RoundedRectangle(cornerRadius: 4)
                    .fill(Color.black.opacity(0.35))
                    .frame(width: guideWidth, height: guideHeight)
                    .overlay(
                        RoundedRectangle(cornerRadius: 4)
                            .stroke(Color.white.opacity(0.4), lineWidth: 0.5)
                    )
                
                // Filmstrip Trail
                ZStack(alignment: .leading) {
                    ForEach(viewModel.panoSnapshots) { snapshot in
                        PanoSnapshotView(snapshot: snapshot, height: guideHeight, width: sliceWidth)
                            .offset(x: snapshot.progress * guideWidth)
                            .transition(.opacity.combined(with: .scale))
                    }
                }
                .frame(width: guideWidth, height: guideHeight, alignment: .leading)
                
                // The Horizon Line
                HorizonLineView(width: guideWidth,
                                height: lineHeight,
                                isBroken: isLevelBroken)
                    .offset(y: levelOffset)
                
                // The Directional Arrow
                Image(systemName: "arrowtriangle.right.fill")
                    .font(.system(size: 24))
                    .foregroundColor(.white)
                    .offset(x: arrowX, y: levelOffset)
                    .opacity(clampedProgress > 0 ? 1 : 0)
                    .animation(.linear(duration: 0.1), value: clampedProgress)
            }
            .frame(width: guideWidth, height: guideHeight)
        }
    }
}

struct HorizonLineView: View {
    let width: CGFloat
    let height: CGFloat
    let isBroken: Bool
    
    var body: some View {
        if isBroken {
            // Feedback: "line will appear to 'break' or drift"
            // Broken state: two segments offset
            HStack(spacing: 12) {
                Rectangle()
                    .fill(Color.white)
                    .frame(width: (width - 12) / 2, height: height)
                    .offset(y: -2)
                Rectangle()
                    .fill(Color.white)
                    .frame(width: (width - 12) / 2, height: height)
                    .offset(y: 2)
            }
        } else {
            // Solid yellow line
            Rectangle()
                .fill(Color.yellow)
                .frame(width: width, height: height)
        }
    }
}

struct PanoBottomBar: View {
    @ObservedObject var viewModel: SessionViewModel
    @Binding var showResults: Bool
    
    var body: some View {
        HStack {
            if !viewModel.identifiedBooks.isEmpty && !viewModel.isRecording {
                Button(action: { showResults = true }) {
                    Image(systemName: "books.vertical.fill")
                        .font(.system(size: 16, weight: .semibold))
                        .foregroundColor(.white)
                        .frame(width: 44, height: 44)
                        .background(Color.black.opacity(0.6))
                        .clipShape(Circle())
                        .overlay(Circle().stroke(Color.white.opacity(0.3), lineWidth: 1))
                }
            } else if viewModel.isDebugEnabled {
                // Share Log Button
                ShareLink(item: viewModel.getLogURL()) {
                    Image(systemName: "doc.text.magnifyingglass")
                        .font(.system(size: 16, weight: .semibold))
                        .foregroundColor(.white)
                        .frame(width: 44, height: 44)
                        .background(Color.blue.opacity(0.6))
                        .clipShape(Circle())
                        .overlay(Circle().stroke(Color.white.opacity(0.3), lineWidth: 1))
                }
            } else {
                Color.clear.frame(width: 44, height: 44)
            }
            
            Spacer()
            
            Button(action: {
                withAnimation(.spring()) {
                    if viewModel.isRecording {
                        viewModel.stopRecording()
                    } else {
                        viewModel.startRecording()
                    }
                }
            }) {
                ZStack {
                    Circle()
                        .stroke(Color.white, lineWidth: 4)
                        .frame(width: 74, height: 74)
                    
                    if viewModel.isRecording {
                        RoundedRectangle(cornerRadius: 6, style: .continuous)
                            .fill(Color.red)
                            .frame(width: 32, height: 32)
                    } else {
                        Circle()
                            .fill(Color.white)
                            .frame(width: 58, height: 58)
                    }
                }
                .shadow(color: .black.opacity(0.4), radius: 6, x: 0, y: 4)
            }
            
            Spacer()
            
            if viewModel.capturedCount > 0 {
                Text("\(viewModel.capturedCount)")
                    .font(.system(size: 14, weight: .semibold))
                    .foregroundColor(.white)
                    .frame(width: 44, height: 44)
                    .background(Color.black.opacity(0.6))
                    .clipShape(Circle())
                    .overlay(Circle().stroke(Color.white.opacity(0.3), lineWidth: 1))
            } else {
                Color.clear.frame(width: 44, height: 44)
            }
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
        private var startYaw: Float?
        private var startPosition: SIMD3<Float>?
        private var lastYaw: Float?
        private var lastTimestamp: TimeInterval?
        private var lastCapturePosition: SIMD3<Float>?
        private var didAutoStop = false
        private var isCoolingDown = false
        private var lockedDirection: PanoDirection = .unknown
        private var distanceToShelf: Float = 0
        private var consecutiveRaycastFailures = 0
        
        private var hasCapturedFirstFrame = false
        private var optimalStateStartTime: TimeInterval?
        
        private let targetMeters: Float = 2.0
        private var dynamicCaptureStep: Float = 0.15 // Default fallback (15cm)
        private let maxYawSpeed: Float = 1.4
        private let minYawSpeed: Float = 0.08
        private let levelBreakThreshold: Float = 0.12
        
        init(viewModel: SessionViewModel) {
            self.viewModel = viewModel
        }
        
        func session(_ session: ARSession, didUpdate frame: ARFrame) {
            let state = frame.camera.trackingState
            let isNormalTracking = (state == .normal)
            DispatchQueue.main.async {
                self.viewModel.isTracking = isNormalTracking
            }
            
            if !viewModel.isRecording {
                resetPanoTracking()
                return
            }
            guard isNormalTracking else { return }
            
            let transform = frame.camera.transform
            let position = SIMD3<Float>(transform.columns.3.x, transform.columns.3.y, transform.columns.3.z)
            
            // Calculate Dynamic Step based on 50% FoV Overlap
            updateDynamicStep(session: session, frame: frame)
            
            // Use ARKit's built-in eulerAngles (x: pitch, y: yaw, z: roll)
            let currentEuler = frame.camera.eulerAngles
            let pitch = currentEuler.x
            let yaw = currentEuler.y
            let roll = currentEuler.z
            
            if startYaw == nil {
                startYaw = yaw
                startPosition = position
                lastYaw = yaw
                lastTimestamp = frame.timestamp
                lastCapturePosition = position
                didAutoStop = false
                lockedDirection = .unknown
                DispatchQueue.main.async {
                    self.viewModel.panoProgress = 0
                    self.viewModel.panoDirection = .unknown
                    self.viewModel.panoSpeed = .idle
                    self.viewModel.panoRoll = 0
                    self.viewModel.panoPitch = 0
                    self.viewModel.panoYaw = 0
                }
                return
            }
            
            let deltaYaw = yaw - (startYaw ?? yaw)
            let direction = directionFor(deltaYaw: deltaYaw)
            if lockedDirection == .unknown, direction != .unknown {
                lockedDirection = direction
            }
            
            let levelOffset = CGFloat(max(min(-pitch * 40, 10), -10))
            let isBroken = abs(pitch) > levelBreakThreshold
            
            // Distance Calculation (Horizontal only)
            let deltaPos = position - (startPosition ?? position)
            let horizontalDistance = sqrt(pow(deltaPos.x, 2) + pow(deltaPos.z, 2))
            let progress = min(max(horizontalDistance / targetMeters, 0.0), 1.0)
            
            let speedState = panoSpeedState(yaw: yaw, timestamp: frame.timestamp)
            
            // Convert to degrees for easier threshold comparison
            let rollDeg = CGFloat(roll * 180 / .pi)
            let pitchDeg = CGFloat(pitch * 180 / .pi)
            let yawDeg = CGFloat(deltaYaw * 180 / .pi)
            
            DispatchQueue.main.async {
                self.viewModel.panoProgress = CGFloat(progress)
                self.viewModel.panoDirection = self.lockedDirection
                self.viewModel.panoSpeed = speedState
                self.viewModel.panoLevelOffset = levelOffset
                self.viewModel.panoLevelBroken = isBroken
                self.viewModel.panoRoll = rollDeg
                self.viewModel.panoPitch = pitchDeg
                self.viewModel.panoYaw = yawDeg
                
                // Detailed UI Debug Log
                let guideWidth: Float = 310.0
                let sliceWidth: Float = guideWidth / 10.0
                let currentProgressX = progress * guideWidth
                let arrowX = currentProgressX + sliceWidth - 12.0
                
                // Logging
                let status = PanoOverlayView.statusText(for: self.viewModel)
                self.viewModel.log("Pos:\(String(format: "%.2f", horizontalDistance))m | UI_X:\(String(format: "%.1f", currentProgressX)) | Arrow_X:\(String(format: "%.1f", arrowX)) | R:\(Int(rollDeg)) P:\(Int(pitchDeg)) Y:\(Int(yawDeg)) | Dist:\(String(format: "%.2f", self.distanceToShelf))m | Status: \(status)")
            }
            
            // First Frame Logic: Capture immediately once optimal
            if !hasCapturedFirstFrame {
                let roll = rollDeg
                let pitch = pitchDeg
                let yaw = yawDeg
                
                let isPerfect = abs(roll + 90) <= 15 && abs(pitch) <= 35 && abs(yaw) <= 35
                let isDistanceOptimal = viewModel.distanceState == .optimal
                
                if isPerfect && isDistanceOptimal {
                    // Snap immediately, no 0.2s delay
                    self.lastCapturePosition = position
                    self.hasCapturedFirstFrame = true
                    takeSnapshot(frame: frame)
                }
            } else {
                // Normal distance-based logic (only after first frame is captured)
                if lockedDirection != .unknown, !isCoolingDown, let lastCapturePos = lastCapturePosition {
                    let distFromLast = distance(position, lastCapturePos)
                    if distFromLast >= dynamicCaptureStep {
                        self.lastCapturePosition = position
                        takeSnapshot(frame: frame)
                    }
                }
            }
            
            lastYaw = yaw
            lastTimestamp = frame.timestamp
        }
        
        private func updateDynamicStep(session: ARSession, frame: ARFrame) {
            // 1. Get Horizontal Field of View
            let intrinsics = frame.camera.intrinsics
            let resolution = frame.camera.imageResolution
            let focalLengthX = intrinsics[0][0]
            let hFov = 2 * atan(Float(resolution.width) / (2 * focalLengthX))
            
            // 2. Estimate distance to shelf (Robust Multi-Point Raycast)
            var currentState: DistanceState = .unknown
            var foundDistance: Float?
            
            // Raycast points: Center, slightly above, slightly below
            let points = [CGPoint(x: 0.5, y: 0.5), CGPoint(x: 0.5, y: 0.4), CGPoint(x: 0.5, y: 0.6)]
            
            for point in points {
                // Try 1: Existing Plane (Most accurate)
                let planeResults = frame.raycastQuery(from: point, allowing: .existingPlaneGeometry, alignment: .vertical)
                if let first = session.raycast(planeResults).first {
                    let hitPos = SIMD3<Float>(first.worldTransform.columns.3.x, first.worldTransform.columns.3.y, first.worldTransform.columns.3.z)
                    let camPos = SIMD3<Float>(frame.camera.transform.columns.3.x, frame.camera.transform.columns.3.y, frame.camera.transform.columns.3.z)
                    foundDistance = distance(hitPos, camPos)
                    self.viewModel.log("Raycast: Hit Existing Plane at \(foundDistance ?? 0)m")
                    break
                } 
                
                // Try 2: Estimated Plane
                let estimatedResults = frame.raycastQuery(from: point, allowing: .estimatedPlane, alignment: .vertical)
                if let first = session.raycast(estimatedResults).first {
                    let hitPos = SIMD3<Float>(first.worldTransform.columns.3.x, first.worldTransform.columns.3.y, first.worldTransform.columns.3.z)
                    let camPos = SIMD3<Float>(frame.camera.transform.columns.3.x, frame.camera.transform.columns.3.y, frame.camera.transform.columns.3.z)
                    foundDistance = distance(hitPos, camPos)
                    self.viewModel.log("Raycast: Hit Estimated Plane at \(foundDistance ?? 0)m")
                    break
                }
                
                // Try 3: Feature Points (Any Surface)
                let pointResults = frame.raycastQuery(from: point, allowing: .estimatedPlane, alignment: .any)
                if let first = session.raycast(pointResults).first {
                    let hitPos = SIMD3<Float>(first.worldTransform.columns.3.x, first.worldTransform.columns.3.y, first.worldTransform.columns.3.z)
                    let camPos = SIMD3<Float>(frame.camera.transform.columns.3.x, frame.camera.transform.columns.3.y, frame.camera.transform.columns.3.z)
                    foundDistance = distance(hitPos, camPos)
                    self.viewModel.log("Raycast: Hit Any Surface at \(foundDistance ?? 0)m")
                    break
                }
            }
            
            if foundDistance == nil {
                self.viewModel.log("Raycast: FAILED ALL STAGES across all points")
            }
            
            if let dist = foundDistance {
                // Smoothing (Low-pass filter: 90% old, 10% new)
                let alpha: Float = 0.1
                if self.distanceToShelf == 0 {
                    self.distanceToShelf = dist
                } else {
                    self.distanceToShelf = (self.distanceToShelf * (1.0 - alpha)) + (dist * alpha)
                }
                
                self.consecutiveRaycastFailures = 0
                
                // New Lenient range: 15cm (0.15m) - 100cm (1.0m)
                if self.distanceToShelf < 0.15 {
                    currentState = .tooClose
                } else if self.distanceToShelf > 1.00 {
                    currentState = .tooFar
                } else {
                    currentState = .optimal
                }
            } else {
                self.consecutiveRaycastFailures += 1
                
                // STICKY LOGIC: If we have a valid smoothed distance, and tracking is normal, 
                // we TRUST that the wall hasn't moved for up to 5 seconds (150 frames).
                if self.distanceToShelf > 0.15 && self.distanceToShelf < 1.0 && self.consecutiveRaycastFailures < 150 {
                    currentState = .optimal
                } else {
                    currentState = .unknown
                }
            }
            
            DispatchQueue.main.async {
                self.viewModel.distanceState = currentState
            }
            
            // 3. Visible Width = 2 * Distance * tan(FoV / 2)
            let currentDist = self.distanceToShelf > 0 ? self.distanceToShelf : 0.40 // Fallback width for calc
            let visibleWidth = 2 * currentDist * tan(hFov / 2)
            
            // 4. Step for 50% overlap = visibleWidth / 2
            // User requested "max 10 frames" over 2 meters, which implies a 20cm step (2.0 / 10 = 0.20)
            // We take the minimum of (50% overlap) and (20cm) to ensure quality but keep thumbnails large
            self.dynamicCaptureStep = max(min(visibleWidth / 2, 0.20), 0.05)
        }
        
        private func resetPanoTracking() {
            startYaw = nil
            startPosition = nil
            lastYaw = nil
            lastTimestamp = nil
            lastCapturePosition = nil
            didAutoStop = false
            isCoolingDown = false
            lockedDirection = .unknown
            hasCapturedFirstFrame = false
            optimalStateStartTime = nil
        }
        
        private func yaw(from transform: simd_float4x4) -> Float {
            // No longer used, using frame.camera.eulerAngles.y
            return 0
        }
        
        private func roll(from transform: simd_float4x4) -> Float {
            // No longer used, using frame.camera.eulerAngles.z
            return 0
        }
        
        private func directionFor(deltaYaw: Float) -> PanoDirection {
            if abs(deltaYaw) < 0.03 {
                return .unknown
            }
            return deltaYaw >= 0 ? .right : .left
        }
        
        private func panoSpeedState(yaw: Float, timestamp: TimeInterval) -> PanoSpeedState {
            guard let lastYaw = lastYaw, let lastTimestamp = lastTimestamp else {
                return .idle
            }
            let dt = max(timestamp - lastTimestamp, 0.0001)
            let speed = abs(yaw - lastYaw) / Float(dt)
            if speed > maxYawSpeed {
                return .tooFast
            }
            if speed < minYawSpeed {
                return .tooSlow
            }
            return .ok
        }
        
        private func pitch(from transform: simd_float4x4) -> Float {
            // No longer used, using frame.camera.eulerAngles.x
            return 0
        }
        
        func takeSnapshot(frame: ARFrame) {
            isCoolingDown = true
            
            let progress = viewModel.panoProgress
            let roll = viewModel.panoRoll
            let pitch = viewModel.panoPitch
            let yaw = viewModel.panoYaw
            let isPerfect = abs(roll + 90) <= 15 && abs(pitch) <= 35 && abs(yaw) <= 35
            
            // STRICT REQUIREMENT: Only capture if distance is Optimal
            let isDistanceOptimal = viewModel.distanceState == .optimal
            
            if !isPerfect || !isDistanceOptimal {
                let hasGoodNearby = viewModel.panoSnapshots.last { $0.image != nil && abs($0.progress - progress) < 0.03 } != nil
                if hasGoodNearby {
                    DispatchQueue.global().asyncAfter(deadline: .now() + 0.35) { self.isCoolingDown = false }
                    return
                }
                
                DispatchQueue.main.async {
                    self.viewModel.panoSnapshots.append(PanoSnapshot(image: nil, progress: progress))
                    UIImpactFeedbackGenerator(style: .soft).impactOccurred()
                }
                DispatchQueue.global().asyncAfter(deadline: .now() + 0.35) { self.isCoolingDown = false }
                return
            }
            
            self.viewModel.log("!!! CAPTURING FRAME at \(String(format: "%.2f", progress * 100))% | Dist: \(String(format: "%.2f", self.distanceToShelf))m")
            
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
                let thumbSize = CGSize(width: 60, height: 90)
                UIGraphicsBeginImageContextWithOptions(thumbSize, false, 0.0)
                uiImage.draw(in: CGRect(origin: .zero, size: thumbSize))
                let thumbnail = UIGraphicsGetImageFromCurrentImageContext()
                UIGraphicsEndImageContext()
                
                if let thumb = thumbnail {
                    DispatchQueue.main.async {
                        self.viewModel.panoSnapshots.append(PanoSnapshot(image: thumb, progress: progress))
                    }
                }
                
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
                        
            DispatchQueue.global().asyncAfter(deadline: .now() + 0.35) {
                self.isCoolingDown = false
            }
        }
    }
}

struct DistanceGuidanceView: View {
    let state: DistanceState
    
    var body: some View {
        ZStack {
            switch state {
            case .tooClose:
                Circle()
                    .stroke(Color.red, lineWidth: 3)
                    .frame(width: 80, height: 80)
                    .overlay(
                        Text("Move Back")
                            .font(.system(size: 14, weight: .bold))
                            .foregroundColor(.red)
                            .offset(y: 60)
                    )
                    .transition(.opacity.combined(with: .scale))
            case .tooFar:
                Circle()
                    .stroke(Color.white, style: StrokeStyle(lineWidth: 2, dash: [8]))
                    .frame(width: 120, height: 120)
                    .overlay(
                        Text("Move Closer")
                            .font(.system(size: 14, weight: .bold))
                            .foregroundColor(.white)
                            .offset(y: 80)
                    )
                    .transition(.opacity.combined(with: .scale))
            case .optimal:
                ZStack {
                    Rectangle()
                        .fill(Color.green)
                        .frame(width: 20, height: 2)
                    Rectangle()
                        .fill(Color.green)
                        .frame(width: 2, height: 20)
                }
                .frame(width: 40, height: 40)
                .transition(.opacity.combined(with: .scale))
            case .unknown:
                EmptyView()
            }
        }
        .animation(.easeInOut(duration: 0.3), value: state)
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .allowsHitTesting(false)
    }
}
