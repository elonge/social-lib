import Foundation

struct Book: Identifiable, Codable {
    let id: UUID
    let title: String?
    let author: String?
    let publisher: String?
    let year: String?
    let otherText: String?
    
    enum CodingKeys: String, CodingKey {
        case title, author, publisher, year
        case otherText = "other_text"
    }
    
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.id = UUID()
        self.title = try container.decodeIfPresent(String.self, forKey: .title)
        self.author = try container.decodeIfPresent(String.self, forKey: .author)
        self.publisher = try container.decodeIfPresent(String.self, forKey: .publisher)
        self.year = try container.decodeIfPresent(String.self, forKey: .year)
        self.otherText = try container.decodeIfPresent(String.self, forKey: .otherText)
    }
    
    init(id: UUID = UUID(), title: String?, author: String?, publisher: String?, year: String?, otherText: String?) {
        self.id = id
        self.title = title
        self.author = author
        self.publisher = publisher
        self.year = year
        self.otherText = otherText
    }
}
