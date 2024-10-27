import AVFoundation
import SwiftSignalKit

class BuffersRenderer {
    private let sbLayer: AVSampleBufferDisplayLayer
    private let audio: AVSampleBufferAudioRenderer
    private let sync: AVSampleBufferRenderSynchronizer
    private let playerQueue: Queue
    
    private(set) var isRunning = false
    
    var time: CMTime {
        sync.currentTime()
    }
    
    var volume: Double {
        get {
            Double(audio.volume)
        }
        set {
            audio.volume = Float(newValue)
        }
    }
    
    var rate: Double {
        get {
            Double(sync.rate)
        }
        set {
            sync.rate = Float(newValue)
        }
    }
    
    var baseRate: Double = 1 {
        didSet {
            let value = Float(baseRate)
            guard sync.rate > 0, sync.rate != value else { return }
            sync.rate = value
        }
    }
    
    private var enqueueInProgress = false
    private let bufferingQueue = Queue(name: "hls.renderer.bufferingQueue")
    private var videoBuffers = RingBuffer<CMSampleBuffer>(capacity: 2000)
    private var audioBuffers = RingBuffer<CMSampleBuffer>(capacity: 500)
    
    private let logger = HLSLogger(module: "BuffersRenderer")
    
    init(playerQueue: Queue, sbLayer: AVSampleBufferDisplayLayer) {
        self.playerQueue = playerQueue
        self.sbLayer = sbLayer
        self.audio = AVSampleBufferAudioRenderer()
        self.sync = AVSampleBufferRenderSynchronizer()
        sync.addRenderer(sbLayer)
        sync.addRenderer(audio)
    }
    
    func schedule(hlsFragment: HLSFragment, completion: (() -> Void)?) {
        func schedule(buffers: [CMSampleBuffer], queue: inout RingBuffer<CMSampleBuffer>) {
            var cutIndex = 0
            for i in (0..<buffers.count).reversed() {
                let buffer = buffers[i]
                if CMTimeCompare(buffer.time.timestamp, time) <= 0 {
                    if buffer.isKey {
                        cutIndex = i
                        break
                    }
                }
            }
            for i in cutIndex..<buffers.count {
                queue.enqueue(buffers[i])
            }
        }
        
        bufferingQueue.async { [weak self] in
            guard let self else { return }
            logger.info("scheduling frag: \(hlsFragment.fragment.byteRange.length)@\(hlsFragment.fragment.byteRange.start)")
            schedule(buffers: hlsFragment.videoBuffers, queue: &videoBuffers)
            schedule(buffers: hlsFragment.audioBuffers, queue: &audioBuffers)
            completion?()
        }
    }
    
    func play() {
        guard !isRunning else { return }
        logger.info(tag: "play", "run")
        self.isRunning = true
        sync.rate = Float(baseRate)
    }
    
    func pause() {
        guard isRunning else { return }
        logger.info(tag: "play", "stop")
        self.isRunning = false
        sync.rate = 0
    }
    
    func seek(timestamp: Double) {
        let time = CMTime(seconds: timestamp, preferredTimescale: .max)
        flush(time: time)
    }
    
    func flush() {
        flush(time: nil)
    }
    
    func displayLinkTrigger() {
        bufferingQueue.async { [weak self] in
            guard let self, isRunning, !enqueueInProgress else { return }
            enqueueInProgress = true
            if !sbLayer.isReadyForMoreMediaData, let nextBuffer = videoBuffers.peek(), CMTimeSubtract(nextBuffer.time.timestamp, time) < .zero {
                sbLayer.flush()
            }
            while sbLayer.isReadyForMoreMediaData, let buffer = videoBuffers.dequeue() {
                sbLayer.enqueue(buffer)
            }
            while audio.isReadyForMoreMediaData, let buffer = audioBuffers.dequeue() {
                audio.enqueue(buffer)
            }
            enqueueInProgress = false
        }
    }
    
    private func flush(time: CMTime?) {
        isRunning = false
        if let time {
            sync.setRate(0, time: time)
        } else {
            sync.rate = 0
        }
        self.sbLayer.flush()
        self.audio.flush()
        bufferingQueue.async { [weak self] in
            self?.videoBuffers.flush()
            self?.audioBuffers.flush()
        }
    }
}
