import Foundation

struct M3U8Playlist: Identifiable, Hashable {
    var id: Int {
        hashValue
    }
    
    let targetDuration: Int
    let version: Int
    let mediaSequence: Int
    let independentSegments: Bool
    let bandwidth: Int
    let resolution: Resolution
    let map: Map
    let fragments: [Fragment]

    struct Fragment: Identifiable, Hashable {
        var id: Int {
            hashValue
        }
        
        let duration: Double
        let byteRange: ByteRange
        let url: URL
    }

    struct Map: Hashable {
        let url: URL
        let byteRange: ByteRange
    }

    struct ByteRange: Hashable {
        let length: Int
        let start: Int
        
        var end: Int {
            start + length
        }
    }
    
    struct Resolution: Hashable, CustomStringConvertible {
        let width: Int
        let height: Int
        
        var description: String {
            "\(width)x\(height)"
        }
    }
    
    init?(masterVariant: M3U8MasterPlaylist.Variant, data: Data, baseUrl: URL) {
        guard let string = String(data: data, encoding: .utf8) else { return nil }
        self.init(masterVariant: masterVariant, string: string, baseUrl: baseUrl)
    }

    init?(masterVariant: M3U8MasterPlaylist.Variant, string: String, baseUrl: URL) {
        var targetDuration: Int? = nil
        var version: Int? = nil
        var mediaSequence: Int? = nil
        var independentSegments = false
        var mapUrl: URL? = nil
        var mapByteRange: ByteRange? = nil
        var fragments = [Fragment]()

        let lines = string.split(separator: "\n")

        var currentDuration: Double?
        var currentByteRange: ByteRange?

        for line in lines {
            if line.starts(with: "#EXTM3U") {
                continue
            } else if line.starts(with: "#EXT-X-TARGETDURATION:") {
                targetDuration = Int(line.replacingOccurrences(of: "#EXT-X-TARGETDURATION:", with: ""))
            } else if line.starts(with: "#EXT-X-VERSION:") {
                version = Int(line.replacingOccurrences(of: "#EXT-X-VERSION:", with: ""))
            } else if line.starts(with: "#EXT-X-MEDIA-SEQUENCE:") {
                mediaSequence = Int(line.replacingOccurrences(of: "#EXT-X-MEDIA-SEQUENCE:", with: ""))
            } else if line.starts(with: "#EXT-X-INDEPENDENT-SEGMENTS") {
                independentSegments = true
            } else if line.starts(with: "#EXT-X-MAP:") {
                let attributes = line.replacingOccurrences(of: "#EXT-X-MAP:", with: "").split(separator: ",")
                for attribute in attributes {
                    if attribute.starts(with: "URI=") {
                        let uriString = attribute.replacingOccurrences(of: "URI=", with: "").trimmingCharacters(in: .init(charactersIn: "\""))
                        mapUrl = baseUrl.deletingLastPathComponent().appendingPathComponent(uriString)
                    } else if attribute.starts(with: "BYTERANGE=") {
                        mapByteRange = parseByteRange(attribute.replacingOccurrences(of: "BYTERANGE=", with: "").trimmingCharacters(in: .init(charactersIn: "\"")))
                    }
                }
            } else if line.starts(with: "#EXTINF:") {
                let durationLine = line
                    .replacingOccurrences(of: "#EXTINF:", with: "")
                    .replacingOccurrences(of: ",", with: "")
                currentDuration = Double(durationLine)
            } else if line.starts(with: "#EXT-X-BYTERANGE:") {
                currentByteRange = parseByteRange(line.replacingOccurrences(of: "#EXT-X-BYTERANGE:", with: ""))
            } else if !line.starts(with: "#") {
                let url = baseUrl.deletingLastPathComponent().appendingPathComponent(String(line))
                if let duration = currentDuration, let byteRange = currentByteRange {
                    fragments.append(
                        Fragment(
                            duration: duration,
                            byteRange: byteRange,
                            url: url
                        )
                    )
                }
                currentDuration = nil
                currentByteRange = nil
            }
        }

        guard let targetDuration,
              let version,
              let mediaSequence,
              let mapUrl,
              let mapByteRange = mapByteRange else {
            return nil
        }

        self.targetDuration = targetDuration
        self.version = version
        self.mediaSequence = mediaSequence
        self.independentSegments = independentSegments
        self.map = Map(url: mapUrl, byteRange: mapByteRange)
        self.fragments = fragments
        self.bandwidth = masterVariant.bandwidth
        self.resolution = Resolution(
            width: masterVariant.resolution.width,
            height: masterVariant.resolution.height
        )
    }
}

struct M3U8MasterPlaylist: Identifiable, Hashable {
    struct Variant: Hashable {
        struct Resolution: Hashable {
            let width: Int
            let height: Int
        }
        
        let bandwidth: Int
        let resolution: Resolution
        let url: URL
    }
    
    var id: Int {
        hashValue
    }
    
    let url: URL
    let version: Int
    let variants: [Variant]
    
    init?(data: Data, baseUrl: URL) {
        guard let string = String(data: data, encoding: .utf8) else { return nil }
        self.init(string: string, baseUrl: baseUrl)
    }
    
    init?(string: String, baseUrl: URL) {
        self.url = baseUrl
        
        var version = 0
        var variants: [M3U8MasterPlaylist.Variant] = []
        
        let lines = string.components(separatedBy: .newlines)
        var index = 0
        
        while index < lines.count {
            let line = lines[index].trimmingCharacters(in: .whitespaces)
            
            if index == 0, !line.hasPrefix("#EXTM3U") {
                return nil
            } else if line.hasPrefix("#EXT-X-VERSION:") {
                if let versionNumber = Int(line.replacingOccurrences(of: "#EXT-X-VERSION:", with: "")) {
                    version = versionNumber
                }
            } else if line.hasPrefix("#EXT-X-STREAM-INF:") {
                let attributesString = line.replacingOccurrences(of: "#EXT-X-STREAM-INF:", with: "")
                let attributes = parseAttributes(attributesString)
                
                index += 1
                if index < lines.count {
                    let urlLine = lines[index].trimmingCharacters(in: .whitespaces)
                    let url = baseUrl.deletingLastPathComponent().appendingPathComponent(urlLine)
                    
                    guard let bandwidthString = attributes["BANDWIDTH"],
                          let bandwidth = Int(bandwidthString) else {
                        continue
                    }
                    
                    var resolution: Variant.Resolution?
                    if let resolutionString = attributes["RESOLUTION"] {
                        let resolutionComponents = resolutionString.components(separatedBy: "x")
                        if resolutionComponents.count == 2,
                           let width = Int(resolutionComponents[0]),
                           let height = Int(resolutionComponents[1]) {
                            resolution = Variant.Resolution(width: width, height: height)
                        }
                    }
                    
                    if let resolution {
                        let variant = M3U8MasterPlaylist.Variant(
                            bandwidth: bandwidth,
                            resolution: resolution,
                            url: url
                        )
                        
                        variants.append(variant)
                    }
                }
            }
            
            index += 1
        }
        
        self.version = version
        self.variants = variants
    }
}

private func parseAttributes(_ attributesString: String) -> [String: String] {
    var attributes: [String: String] = [:]
    var key = ""
    var value = ""
    var inKey = true
    var inQuotes = false

    var index = attributesString.startIndex

    while index < attributesString.endIndex {
        let char = attributesString[index]
        if inKey {
            if char == "=" {
                inKey = false
            } else {
                key.append(char)
            }
        } else {
            if char == "\"" {
                inQuotes.toggle()
                value.append(char)
            } else if char == "," && !inQuotes {
                attributes[key] = value
                key = ""
                value = ""
                inKey = true
            } else {
                value.append(char)
            }
        }
        index = attributesString.index(after: index)
    }

    if !key.isEmpty {
        attributes[key] = value
    }

    return attributes
}

private func parseByteRange(_ byteRangeString: String) -> M3U8Playlist.ByteRange? {
    let components = byteRangeString.split(separator: "@")
    guard components.count == 2,
          let length = Int(components[0]),
          let start = Int(components[1]) else {
        return nil
    }
    return M3U8Playlist.ByteRange(length: length, start: start)
}
