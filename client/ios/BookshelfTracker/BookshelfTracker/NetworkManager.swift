import Foundation
import UIKit

class NetworkManager {
    static let shared = NetworkManager()
    private let baseURL = "https://book-extractor-api-522989910118.us-central1.run.app"
    
    func uploadFrame(image: UIImage, completion: @escaping (Result<[String: Any], Error>) -> Void) {
        guard let url = URL(string: "\(baseURL)/upload_next_frame") else { return }
        guard let imageData = image.jpegData(compressionQuality: 0.8) else { return }
        
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        
        let boundary = "Boundary-\(UUID().uuidString)"
        request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")
        
        var body = Data()
        body.append("--\(boundary)\r\n".data(using: .utf8)!)
        body.append("Content-Disposition: form-data; name=\"file\"; filename=\"frame.jpg\"\r\n".data(using: .utf8)!)
        body.append("Content-Type: image/jpeg\r\n\r\n".data(using: .utf8)!)
        body.append(imageData)
        body.append("\r\n".data(using: .utf8)!)
        body.append("--\(boundary)--\r\n".data(using: .utf8)!)
        request.httpBody = body
        
        print("🚀 Uploading frame...")
        URLSession.shared.dataTask(with: request) { data, response, error in
            if let error = error {
                print("❌ Upload error: \(error)")
                completion(.failure(error))
                return
            }
            
            guard let data = data else {
                print("❌ No data received from upload")
                completion(.failure(NSError(domain: "No data", code: 0)))
                return
            }
            
            if let rawString = String(data: data, encoding: .utf8) {
                print("📝 Raw upload response: \(rawString)")
            }
            
            do {
                if let json = try JSONSerialization.jsonObject(with: data) as? [String: Any] {
                    completion(.success(json))
                } else {
                    completion(.failure(NSError(domain: "Invalid JSON", code: 0)))
                }
            } catch {
                print("❌ JSON Parse error: \(error)")
                completion(.failure(error))
            }
        }.resume()
    }
    
    func completeUpload(results: [[String: Any]], completion: @escaping (Result<[Book], Error>) -> Void) {
        guard let url = URL(string: "\(baseURL)/complete_upload") else { return }
        print("🏁 Completing upload with \(results.count) frames...")
        
        let payload: [String: Any] = [
            "results": results,
            "enrich": true,
            "metadata": [:]
        ]
        
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        do {
            request.httpBody = try JSONSerialization.data(withJSONObject: payload)
        } catch {
            print("❌ Serialization error: \(error)")
            completion(.failure(error))
            return
        }
        
        URLSession.shared.dataTask(with: request) { data, response, error in
            if let error = error {
                print("❌ Completion error: \(error)")
                completion(.failure(error))
                return
            }
            
            guard let data = data else {
                print("❌ No data received from completion")
                completion(.failure(NSError(domain: "No data", code: 0)))
                return
            }
            
            if let rawString = String(data: data, encoding: .utf8) {
                print("📝 Raw completion response: \(rawString)")
            }
            
            do {
                let decoder = JSONDecoder()
                struct Response: Codable {
                    let books: [Book]
                }
                let decodedResponse = try decoder.decode(Response.self, from: data)
                print("✅ Successfully decoded \(decodedResponse.books.count) books")
                let books = decodedResponse.books
                let coverLinks = books.compactMap { $0.coverLink?.trimmingCharacters(in: .whitespacesAndNewlines) }
                let missingCoverCount = books.count - coverLinks.count
                let invalidCoverCount = coverLinks.filter { URL(string: $0) == nil }.count
                if missingCoverCount > 0 {
                    let missingTitles = books
                        .filter { ($0.coverLink ?? "").trimmingCharacters(in: .whitespacesAndNewlines).isEmpty }
                        .compactMap { $0.title ?? $0.author }
                    print("⚠️ Missing cover_link for \(missingCoverCount) books. Samples: \(missingTitles.prefix(5))")
                }
                if invalidCoverCount > 0 {
                    let invalidTitles = books
                        .filter {
                            guard let link = $0.coverLink?.trimmingCharacters(in: .whitespacesAndNewlines), !link.isEmpty else { return false }
                            return URL(string: link) == nil
                        }
                        .compactMap { $0.title ?? $0.author }
                    print("⚠️ Invalid cover_link URLs for \(invalidCoverCount) books. Samples: \(invalidTitles.prefix(5))")
                }
                print("🖼️ cover_link summary: total=\(books.count) present=\(coverLinks.count) missing=\(missingCoverCount) invalid=\(invalidCoverCount)")
                completion(.success(books))
            } catch {
                print("❌ Decoding error: \(error)")
                completion(.failure(error))
            }
        }.resume()
    }
}
