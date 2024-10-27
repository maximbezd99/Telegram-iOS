import AVFoundation

struct SampleBufferTimeProxy: CustomDebugStringConvertible {
    struct Extra {
        private let buf: CMSampleBuffer
        
        init(buf: CMSampleBuffer) {
            self.buf = buf
        }
        
        var timestamp: CMTime {
            CMSampleBufferGetPresentationTimeStamp(buf)
        }
        
        var decodeTimestamp: CMTime {
            CMSampleBufferGetDecodeTimeStamp(buf)
        }
        
        var outputDecodeTimestamp: CMTime {
            CMSampleBufferGetOutputDecodeTimeStamp(buf)
        }
        
        var duration: CMTime {
            CMSampleBufferGetDuration(buf)
        }
    }
    
    private let buf: CMSampleBuffer
    
    init(buf: CMSampleBuffer) {
        self.buf = buf
    }
    
    var timestamp: CMTime {
        CMSampleBufferGetOutputPresentationTimeStamp(buf)
    }
    
    var duration: CMTime {
        CMSampleBufferGetOutputDuration(buf)
    }
    
    var end: CMTime {
        CMTimeAdd(timestamp, duration)
    }
    
    var ext: Extra {
        Extra(buf: buf)
    }
    
    var debugDescription: String {
        "\(decimal(timestamp.seconds))-\(decimal(end.seconds)) d: \(decimal(duration.seconds))"
    }
}

private func decimal(_ value: Double, points: UInt8 = 3) -> String {
    String(format: "%.\(points)f", value)
}

extension CMSampleBuffer {
    var time: SampleBufferTimeProxy {
        SampleBufferTimeProxy(
            buf: self
        )
    }
    
    var isKey: Bool {
        if let attachments = CMSampleBufferGetSampleAttachmentsArray(self, createIfNecessary: false) as? [[CFString: Any]],
           let attachment = attachments.first {
            let isKeyFrame = !(attachment[kCMSampleAttachmentKey_NotSync] as? Bool ?? false)
            return isKeyFrame
        }
        
        return true
    }
}
