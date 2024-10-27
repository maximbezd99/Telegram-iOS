import AVFoundation
import AccountContext
import SwiftSignalKit

enum PlayState {
    case pause
    case playing
    case finished
}

class AVHLSPlayer {
    private let renderer: BuffersRenderer
    private let playerQueue: Queue
    private let hls = HLS()
    
    private var hlsSession: HLSSession?
    private var timer: SwiftSignalKit.Timer?
    
    private(set) var playState = PlayState.pause {
        didSet {
            onPlayStateChanged?(playState)
        }
    }
    
    var isBuffering: Bool {
        return playState == .playing && !renderer.isRunning
    }
    
    private let logger = HLSLogger(module: "Player")
    
    var time: CMTime {
        renderer.time
    }
    
    var bufferedTime: Double {
        hlsSession?.loadingProgress ?? 0
    }
    
    var volume: Double {
        renderer.volume
    }
    
    var rate: Double {
        renderer.rate
    }
    
    var selectedQuality: UniversalVideoContentVideoQuality? {
        hlsSession?.abr.selectedQuality
    }
    
    var autoQuality: Int? {
        hlsSession?.abr.autoQuality(currentTimestamp: time.seconds, bufferedTimestamp: bufferedTime, playbackRate: rate)
    }
    
    var qualities: [Int]? {
        hlsSession?.abr.playlists.map { $0.resolution.height }
    }
    
    var onPlayStateChanged: ((PlayState) -> Void)?
    var onBufferedTimeUpdate: ((Double) -> Void)?
    var onCurrentTimeUpdate: ((Double) -> Void)?
    
    private let loadMasterDisposable = MetaDisposable()
    
    init(layer: AVSampleBufferDisplayLayer) {
        let playerQueue = Queue(name: "hls.player")
        self.playerQueue = playerQueue
        self.renderer = BuffersRenderer(playerQueue: playerQueue, sbLayer: layer)
        self.timer = SwiftSignalKit.Timer(timeout: 1.0 / 60.0, repeat: true, completion: { [weak self] in
            self?._displayLinkTrigger()
        }, queue: playerQueue)
        self.timer?.start()
    }
    
    deinit {
        invalidate()
    }
    
    func invalidate() {
        if let timer {
            timer.invalidate()
            self.timer = nil
        }
    }
    
    func load(masterUrl: URL) {
        let output = createHLSOutput()
        let signal = hls.loadSession(masterM3U8Url: masterUrl, output: output)
        |> deliverOn(playerQueue)
        loadMasterDisposable.set(signal.start(
            next: { [weak self] session in
                self?.hlsSession = session
                session.start()
            }
        ))
    }
    
    func play() {
        playerQueue.async { [weak self] in
            self?._play()
        }
    }
    
    func seek(timestamp: Double) {
        playerQueue.async { [weak self] in
            self?._seek(timestamp: timestamp)
        }
    }
    
    func pause() {
        playerQueue.async { [weak self] in
            self?._pause()
        }
    }
    
    func set(quality: UniversalVideoContentVideoQuality) {
        playerQueue.async { [weak self] in
            self?._set(quality: quality)
        }
    }
    
    func set(volume: Double) {
        playerQueue.async { [weak self] in
            self?.renderer.volume = volume
        }
    }
    
    func set(rate: Double) {
        playerQueue.async { [weak self] in
            self?.renderer.baseRate = rate
        }
    }
    
    private func createHLSOutput() -> HLSSession.Output {
        HLSSession.Output(
            currentTime: { [weak self] in
                guard let self else { return (.zero, 1) }
                var timestamp: CMTime = .zero
                var rate: Double = .zero
                self.playerQueue.sync {
                    (timestamp, rate) = (self.time, self.rate)
                }
                return (timestamp, rate)
            },
            onNewFragment: { [weak self] hlsSession, hlsFragment in
                self?.playerQueue.async { [weak self] in
                    self?._handle(session: hlsSession, newFragment: hlsFragment)
                }
            },
            onErrorLoadingMasterPlaylist: { [weak self] url, error in
                self?.playerQueue.async { [weak self] in
                    self?._handleMaterPlaylistLoading(url: url, error: error)
                }
            },
            onErrorLoadingPlaylist: { [weak self] variant, error in
                self?.playerQueue.async { [weak self] in
                    self?._handlePlaylistLoading(error: error, variant: variant)
                }
            },
            onErrorLoadingInitFragment: { [weak self] playlist, error in
                self?.playerQueue.async { [weak self] in
                    self?._handleInitFragmentLoading(error: error, playlist: playlist)
                }
            },
            onErrorLoadingFragment: { [weak self] playlist, fragment, error in
                self?.playerQueue.async { [weak self] in
                    self?._handleFragmentLoading(error: error, playlist: playlist, fragment: fragment)
                }
            }
        )
    }
    
    private func _play() {
        switch playState {
        case .pause:
            playState = .playing
        case .playing:
            break
        case .finished:
            hlsSession?.seek(timestamp: 0)
            renderer.seek(timestamp: 0)
            playState = .playing
        }
    }
    
    private func _seek(timestamp: Double) {
        hlsSession?.seek(timestamp: timestamp)
        renderer.seek(timestamp: timestamp)
        onCurrentTimeUpdate?(timestamp)
        onBufferedTimeUpdate?(timestamp)
    }
    
    private func _pause() {
        playState = .pause
        renderer.pause()
    }
    
    private func _set(quality: UniversalVideoContentVideoQuality) {
        hlsSession?.set(quality: quality)
        renderer.flush()
        onBufferedTimeUpdate?(renderer.time.seconds)
    }
    
    private func _displayLinkTrigger() {
        guard playState == .playing, let hlsSession else { return }
        
        let threshold = 0.05
        let timeBeforeBuffered = hlsSession.loadingProgress - renderer.time.seconds
        if (hlsSession.finishTime - renderer.time.seconds) < threshold {
            playState = .finished
            renderer.pause()
        } else if timeBeforeBuffered < threshold, !isBuffering {
            renderer.pause()
        } else if timeBeforeBuffered > threshold, isBuffering {
            renderer.play()
        }
        renderer.displayLinkTrigger()
        onCurrentTimeUpdate?(renderer.time.seconds)
        logger.verbose(tag: "displayLinkTrigger", "ts: \(renderer.time.seconds.description)")
    }
    
    private func _handle(session: HLSSession, newFragment: HLSFragment) {
        guard session.master.id == self.hlsSession?.master.id else { return }
        onBufferedTimeUpdate?(session.loadingProgress)
        renderer.schedule(hlsFragment: newFragment, completion: nil)
    }
    
    private func _handleMaterPlaylistLoading(url: URL, error: Error) {
        logger.info(tag: "playerError:", "\(#function), \(error)")
        playerQueue.after(1) { [weak self] in
            self?.load(masterUrl: url)
        }
    }
    
    private func _handlePlaylistLoading(error: Error, variant: M3U8MasterPlaylist.Variant) {
        logger.info(tag: "playerError:", "\(#function), \(error)")
    }
    
    private func _handleInitFragmentLoading(error: Error, playlist: M3U8Playlist) {
        logger.info(tag: "playerError:", "\(#function), \(error)")
    }
    
    private func _handleFragmentLoading(error: Error, playlist: M3U8Playlist, fragment: M3U8Playlist.Fragment) {
        logger.info(tag: "playerError:", "\(#function), \(error)")
    }
}
