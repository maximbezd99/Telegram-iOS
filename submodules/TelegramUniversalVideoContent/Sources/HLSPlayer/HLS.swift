import Foundation
import AVFoundation
import AccountContext
import SwiftSignalKit

enum HLSError: Error {
    case parseMaster
    case parsePlaylist
    case decodeMedia
    case localFilesAccess
    case emptyVideo
    case emptyAudio
}

struct HLSFragment {
    let fragment: M3U8Playlist.Fragment
    let basetime: CMTime
    let duration: CMTime
    let isLocal: Bool
    let videoBuffers: [CMSampleBuffer]
    let audioBuffers: [CMSampleBuffer]
}

final class HLS {
    fileprivate static let queue = Queue(name: "hls.base")
    fileprivate static let session = URLSession(configuration: .ephemeral)
    
    func loadSession(masterM3U8Url: URL, output: HLSSession.Output) -> Signal<HLSSession, Error> {
        loadM3U8(url: masterM3U8Url)
        |> deliverOn(HLS.queue)
        |> mapToSignal { masterData -> Signal<M3U8MasterPlaylist, Error> in
            guard let playlist = M3U8MasterPlaylist(data: masterData, baseUrl: masterM3U8Url) else {
                return Signal.fail(HLSError.parseMaster)
            }
            
            return Signal.single(playlist)
        }
        |> mapToSignal { masterPlaylist in
            let signals: [Signal<M3U8Playlist?, Error>] = masterPlaylist.variants
                .map { variant in
                    loadM3U8(url: variant.url)
                    |> map { data -> Data? in data }
                    |> `catch` { error in
                        output.onErrorLoadingPlaylist(variant, error)
                        return .single(nil)
                    }
                    |> map { data in
                        guard let data, let playlist = M3U8Playlist(masterVariant: variant, data: data, baseUrl: variant.url) else {
                            output.onErrorLoadingPlaylist(variant, HLSError.parsePlaylist)
                            return nil
                        }
                        return playlist
                    }
                }
            
            return combineLatest(queue: HLS.queue, signals) |> map { (masterPlaylist, $0) }
        }
        |> map { (master: M3U8MasterPlaylist, playlists: [M3U8Playlist?]) in
            HLSSession(master: master, mediaPlaylists: playlists.compactMap { $0 }, output: output, queue: HLS.queue)
        }
        |> take(1)
        |> mapError { error in
            output.onErrorLoadingMasterPlaylist(masterM3U8Url, error)
            return error
        }
    }
}

final class HLSSession {
    struct Output {
        let currentTime: () -> (timestamp: CMTime, rate: Double)
        
        let onNewFragment: (HLSSession, HLSFragment) -> Void
        
        let onErrorLoadingMasterPlaylist: (URL, Error) -> Void
        let onErrorLoadingPlaylist: (M3U8MasterPlaylist.Variant, Error) -> Void
        let onErrorLoadingInitFragment: (M3U8Playlist, Error) -> Void
        let onErrorLoadingFragment: (M3U8Playlist, M3U8Playlist.Fragment, Error) -> Void
    }
    
    struct TimeFragment {
        let timestamp: CMTime
        let duration: CMTime
    }
    
    let master: M3U8MasterPlaylist
    private(set) var currentFragmentIndex: Int
    private(set) var isStarted: Bool = false
    private(set) var loadingProgress: Double = 0
    let timeFragments: [TimeFragment]
    let abr: ABR
    
    var finishTime: Double {
        timeFragments.last.map { $0.timestamp + $0.duration }?.seconds ?? 0
    }
    
    private let loaders: [Int: HLSPlaylistLoader]
    
    private var loadedFragments: Set<Int> = []
    
    private let output: Output
    private let queue: Queue
    
    private var skipSheduled = false
    private var bufferingId: UUID
    
    private var timer: SwiftSignalKit.Timer?
    private var runDisposables = DisposableSet()
    
    private let logger = HLSLogger(module: "HLSSession")
    
    init(master: M3U8MasterPlaylist, mediaPlaylists: [M3U8Playlist], output: Output, queue: Queue) {
        self.master = master
        self.currentFragmentIndex = 0
        self.queue = queue
        self.output = output
        
        HLSFilesManager.shared.sessionStarting(master: master)
        
        let bufferingId = UUID()
        self.bufferingId = bufferingId
        var loaders = [Int: HLSPlaylistLoader]()
        mediaPlaylists.forEach {
            loaders[$0.id] = HLSPlaylistLoader(master: master, playlist: $0, bufferingId: bufferingId, queue: queue)
        }
        self.loaders = loaders
        
        var timestamp = CMTime.zero
        self.timeFragments = (0..<(mediaPlaylists[0].fragments.count)).map { i in
            let duration = CMTime(seconds: mediaPlaylists[0].fragments[i].duration, preferredTimescale: 10000)
            let fragTimestamp = timestamp
            timestamp = CMTimeAdd(timestamp, duration)
            return TimeFragment(timestamp: fragTimestamp, duration: duration)
        }
        self.abr = ABR(playlists: mediaPlaylists)
    }
    
    deinit {
        timer?.invalidate()
        HLSFilesManager.shared.sessionFinished(master: master)
    }
    
    func start() {
        queue.async { [weak self, logger] in
            logger.info(tag: "start", "will try start session")
            guard let self, !isStarted else { return }
            self.isStarted = true
            self.timer = SwiftSignalKit.Timer(timeout: 0.1, repeat: true, completion: { [weak self] in
                self?._scheduledRun()
            }, queue: queue)
            self.timer?.start()
            logger.info(tag: "start", "session started")
        }
    }
    
    func seek(timestamp: Double) {
        queue.async { [weak self] in
            self?._seek(timestamp: timestamp)
        }
    }
    
    func set(quality: UniversalVideoContentVideoQuality) {
        queue.async { [weak self] in
            guard let self else { return }
            abr.choose(quality: quality)
            _seek(timestamp: output.currentTime().timestamp.seconds)
        }
    }
    
    private func _seek(timestamp: Double) {
        var accDuration = 0.0
        var fragmentIndex = timeFragments.count - 1
        for i in 0..<timeFragments.count {
            if accDuration + timeFragments[i].duration.seconds > timestamp {
                fragmentIndex = i
                break
            } else {
                accDuration += timeFragments[i].duration.seconds
            }
        }
        currentFragmentIndex = fragmentIndex
        loadingProgress = timestamp
        
        bufferingId = UUID()
        loaders.values.forEach {
            $0.flush(newBuffId: bufferingId)
        }
        runDisposables = DisposableSet()
        
        logger.info(tag: "seek", "\(bufferingId), i: \(fragmentIndex) lp: \(loadingProgress)")
    }
    
    private func _scheduledRun() {
        guard isStarted else {
            logger.verbose(tag: "scheduledRun", "will not shedule run - not started")
            return
        }
        
        guard currentFragmentIndex < timeFragments.count else {
            logger.verbose(tag: "scheduledRun", "will not shedule run - end of playlist")
            return
        }
        
        guard !skipSheduled else {
            logger.verbose(tag: "scheduledRun", "will not shedule run - locked for schedule")
            return
        }
        
        _run()
    }
    
    private func _run() {
        let (playerTimestamp, rate) = output.currentTime()
        guard let playlist = abr.choose(fragmentIndex: currentFragmentIndex, currentTimestamp: playerTimestamp.seconds, bufferedTimestamp: loadingProgress, playbackRate: rate) else { return }
        skipSheduled = true
        let bufferingId = bufferingId
        let loadingStart = DispatchTime.now()
        let basetime = timeFragments[currentFragmentIndex].timestamp
        logger.info(tag: "run", "decided to load next fragment \(currentFragmentIndex), r: \(playlist.resolution)")
        
        guard let loader = loaders[playlist.id] else {
            assertionFailure()
            return
        }
        
        let fragment = loader.playlist.fragments[currentFragmentIndex]
        let signal = loader.load(bufferingId: bufferingId, fragment: fragment, basetime: basetime) 
        |> deliverOn(queue)
        |> filter { [weak self] _ in
            self?.bufferingId == bufferingId
        }
        |> afterDisposed { [weak self] in
            self?.skipSheduled = false
        }
        
        self.runDisposables.add(signal.start(
            next: { [weak self, currentFragmentIndex] fragment in
                guard let self else { return }
                logger.info(tag: "run", "loaded next fragment - br: \(fragment.fragment.byteRange), ts: \(fragment.basetime.seconds), d: \(fragment.duration.seconds)")
                let appendingTime = fragment.fragment.duration - (loadingProgress - basetime.seconds)
                loadingProgress += appendingTime
                self.currentFragmentIndex += 1
                let loadingFinish = DispatchTime.now()
                let loadingTimeSec = Double(loadingFinish.uptimeNanoseconds - loadingStart.uptimeNanoseconds) / 1_000_000_000
                abr.loaded(fragment: fragment, playlist: playlist, fragmentIndex: currentFragmentIndex, duration: loadingTimeSec)
                output.onNewFragment(self, fragment)
            },
            error: { [output] error in
                output.onErrorLoadingFragment(playlist, fragment, error)
            }
        ))
    }
}

final class ABR {
    let playlists: [M3U8Playlist]
    private let bufferingDuration: Double = 30
    private let urgentDuration: Double = 5
    private let notUrgentDuration: Double = 15
    private let bitrateRatio: Double = 1.3
    private let urgentRatio: Double = 2
    private let notUrgentRatio: Double = 1.1
    private let upgradeSpeed = 0.1
    private let downgradeSpeed = 0.3
    
    private(set) var selectedQuality = UniversalVideoContentVideoQuality.auto
    
    private var currentBitrateEstimate: Int
    private var loadedPlaylists: [Int: Int] = [:] // fragment index: playlist id
    
    private static var lastBitrateEstimate: Int = 1000000
    
    private let logger = HLSLogger(module: "ABR")
    
    init(playlists: [M3U8Playlist]) {
        self.playlists = playlists.sorted(by: { $0.bandwidth > $1.bandwidth })
        self.currentBitrateEstimate = Self.lastBitrateEstimate
    }
    
    deinit {
        Self.lastBitrateEstimate = currentBitrateEstimate
    }
    
    func choose(quality: UniversalVideoContentVideoQuality) {
        self.selectedQuality = quality
    }
    
    func loaded(fragment: HLSFragment, playlist: M3U8Playlist, fragmentIndex: Int,  duration: Double) {
        guard !fragment.isLocal else { return }
        
        if let previousId = loadedPlaylists[fragmentIndex], let previousPlaylist = playlists.first(where: { $0.id == previousId }), previousPlaylist.resolution.height > playlist.resolution.height {
        } else {
            loadedPlaylists[fragmentIndex] = playlist.id
        }
        
        let rate = Double(fragment.fragment.byteRange.length * 8) / duration
        let speed = rate > Double(currentBitrateEstimate) ? upgradeSpeed : downgradeSpeed
        currentBitrateEstimate = Int(speed * rate + (1.0 - speed) * Double(currentBitrateEstimate))
        
        logger.info(tag: "changeEstimation", "nr: \(currentBitrateEstimate), d: \(duration)")
    }
    
    func autoQuality(currentTimestamp: Double, bufferedTimestamp: Double, playbackRate: Double) -> Int {
        let index = playlistIndex(currentTimestamp: currentTimestamp, bufferedTimestamp: bufferedTimestamp, playbackRate: playbackRate)
        let playlist = playlists[index]
        return playlist.resolution.height
    }
    
    func choose(fragmentIndex: Int, currentTimestamp: Double, bufferedTimestamp: Double, playbackRate: Double) -> M3U8Playlist? {
        if case let .quality(qHeight) = selectedQuality , let matchingPlaylist = playlists.first(where: { $0.resolution.height == qHeight }) {
            return matchingPlaylist
        }
        
        let bufferLeft = bufferedTimestamp - currentTimestamp
        guard bufferLeft < bufferingDuration else { return nil }
        
        let index = playlistIndex(currentTimestamp: currentTimestamp, bufferedTimestamp: bufferedTimestamp, playbackRate: playbackRate)
        let playlist = playlists[index]
        let loadedPlaylist = playlists.first(where: { $0.id == loadedPlaylists[fragmentIndex] })
        
        let isUrgent = bufferLeft < urgentDuration
        let finalPlaylist: M3U8Playlist
        let choosePrivous: Bool
        if let loadedPlaylist, loadedPlaylist.resolution.height > playlist.resolution.height || isUrgent {
            finalPlaylist = loadedPlaylist
            choosePrivous = true
        } else {
            finalPlaylist = playlist
            choosePrivous = false
        }
        
        logger.info(tag: "choose", "cached: \(choosePrivous ? 1 : 0) res: \(finalPlaylist.resolution), cr: \(currentBitrateEstimate), pr: \(finalPlaylist.bandwidth)")
        return finalPlaylist
    }
    
    private func playlistIndex(currentTimestamp: Double, bufferedTimestamp: Double, playbackRate: Double) -> Int {
        let bufferLeft = bufferedTimestamp - currentTimestamp
        let isUrgent = bufferLeft < urgentDuration
        let isNotUrgent = bufferLeft > notUrgentDuration
        let ratio: Double
        if isUrgent {
            ratio = urgentRatio
        } else if isNotUrgent {
            ratio = notUrgentRatio
        } else {
            ratio = bitrateRatio
        }
        var playlistIndex = playlists.count - 1
        for i in 0..<playlists.count {
            let bitrateRatio = Double(currentBitrateEstimate) / (Double(playlists[i].bandwidth) * ratio * playbackRate)
            if bitrateRatio > 1 {
                playlistIndex = i
                break
            }
        }
        return playlistIndex
    }
}

private final class HLSPlaylistLoader {
    let master: M3U8MasterPlaylist
    let playlist: M3U8Playlist
    let queue: Queue
    
    private let decodingQueue = Queue(name: "hls.decode")
    private let logger = HLSLogger(module: "HLSPlaylistLoader")
    
    private var initSegmentData: Data?
    private var bufferingId: UUID
    
    init(master: M3U8MasterPlaylist, playlist: M3U8Playlist, bufferingId: UUID, queue: Queue) {
        self.master = master
        self.playlist = playlist
        self.queue = queue
        self.bufferingId = bufferingId
    }
    
    func load(bufferingId: UUID, fragment: M3U8Playlist.Fragment, basetime: CMTime) -> Signal<HLSFragment, Error> {
        withLocalFileURL(master: master, playlist: playlist, fragment: fragment)
        |> deliverOn(queue)
        |> filter { [weak self] _ in self?.bufferingId == bufferingId }
        |> deliverOn(decodingQueue)
        |> mapToSignal { [playlist, logger] (url, isLocal) in
            do {
                let hlsFragment = try read(logger: logger, playlist: playlist, fragment: fragment, assetUrl: url, basetime: basetime, isLocal: isLocal)
                return Signal.single(hlsFragment)
            } catch {
                return Signal.fail(error)
            }
        }
        |> deliverOn(queue)
    }
    
    func flush(newBuffId: UUID) {
        self.bufferingId = newBuffId
    }
    
    private func withLocalFileURL(master: M3U8MasterPlaylist, playlist: M3U8Playlist, fragment: M3U8Playlist.Fragment) -> Signal<(URL, Bool), Error> {
        if let localUrl = HLSFilesManager.shared.getFragment(master: master, playlist: playlist, fragment: fragment) {
            return Signal.single((localUrl, true))
        }
        
        return Signal<Data?, Error>.single(initSegmentData)
        |> mapToSignal { (dataOrNil: Data?) -> Signal<Data, Error> in
            if let dataOrNil {
                return Signal.single(dataOrNil)
            } else {
                return loadSegment(url: playlist.map.url, start: playlist.map.byteRange.start, length: playlist.map.byteRange.length)
            }
        }
        |> deliverOn(queue)
        |> beforeNext { [weak self] initSegmentData in
            self?.initSegmentData = initSegmentData
        }
        |> mapToSignal { (initSegmentData: Data) -> Signal<(Data, Data), Error> in
            loadSegment(url: fragment.url, start: fragment.byteRange.start, length: fragment.byteRange.length)
            |> map { (initSegmentData, $0) }
        }
        |> deliverOn(queue)
        |> mapToSignal { (initSegmentData, segmentData) -> Signal<(URL, Bool), Error> in
            do {
                let fullData = initSegmentData + segmentData
                let url = try HLSFilesManager.shared.saveFragment(master: master, playlist: playlist, fragment: fragment, data: fullData)
                return Signal.single((url, false))
            } catch {
                return Signal.fail(HLSError.localFilesAccess)
            }
        }
    }
}

private final class HLSFilesManager {
    static let shared = HLSFilesManager()
    
    private init() {
        if !directoryExists(url: HLSFilesManager.hlsDirectory) {
            try? FileManager.default.createDirectory(atPath: HLSFilesManager.hlsDirectory.path, withIntermediateDirectories: true)
        } else {
            let files = (try? FileManager.default.contentsOfDirectory(at: HLSFilesManager.hlsDirectory, includingPropertiesForKeys: nil)) ?? []
            for file in files {
                try? FileManager.default.removeItem(at: file)
            }
        }
    }
    
    private static let hlsDirectory = FileManager.default.temporaryDirectory.appendingPathComponent("hls")
    
    func sessionStarting(master: M3U8MasterPlaylist) {
        let directory = HLSFilesManager.hlsDirectory.appendingPathComponent("master\(master.id)")
        if directoryExists(url: directory) {
            try? FileManager.default.removeItem(atPath: directory.path)
        }
        
        try? master.variants.forEach {
            let url = directory.appendingPathComponent("quality\($0.bandwidth)")
            try FileManager.default.createDirectory(atPath: url.path, withIntermediateDirectories: true)
        }
    }
    
    func sessionFinished(master: M3U8MasterPlaylist) {
        let directory = HLSFilesManager.hlsDirectory.appendingPathComponent("master\(master.id)")
        if directoryExists(url: directory) {
            try? FileManager.default.removeItem(atPath: directory.path)
        }
    }
    
    func saveFragment(master: M3U8MasterPlaylist, playlist: M3U8Playlist, fragment: M3U8Playlist.Fragment, data: Data) throws -> URL {
        do {
            let fileUrl = fragmentUrl(master: master, playlist: playlist, fragment: fragment)
            try data.write(to: fileUrl)
            return fileUrl
        } catch {
            throw HLSError.localFilesAccess
        }
    }
    
    func getFragment(master: M3U8MasterPlaylist, playlist: M3U8Playlist, fragment: M3U8Playlist.Fragment) -> URL? {
        let fileUrl = fragmentUrl(master: master, playlist: playlist, fragment: fragment)
        if FileManager.default.fileExists(atPath: fileUrl.path) {
            return fileUrl
        } else {
            return nil
        }
    }
    
    private func fragmentUrl(master: M3U8MasterPlaylist, playlist: M3U8Playlist, fragment: M3U8Playlist.Fragment) -> URL {
        let fileUrl = HLSFilesManager.hlsDirectory
            .appendingPathComponent("master\(master.id)")
            .appendingPathComponent("quality\(playlist.bandwidth)")
            .appendingPathComponent("frag\(fragment.id).mp4")
        return fileUrl
    }
    
    private func directoryExists(url: URL) -> Bool {
        var isDirectory: ObjCBool = false
        return FileManager.default.fileExists(atPath: url.path, isDirectory: &isDirectory) && isDirectory.boolValue
    }
}

private func loadM3U8(url: URL) -> Signal<Data, Error> {
    Signal { sub in
        let request = URLRequest(url: url)
        
        let task = HLS.session.dataTask(with: request) { data, _, error in
            if let data {
                sub.putNext(data)
                sub.putCompletion()
            } else if let error {
                sub.putError(error)
            } else {
                assertionFailure()
            }
        }
        
        task.resume()
        
        return ActionDisposable {
            task.cancel()
        }
    }
}

private func loadSegment(url: URL, start: Int, length: Int) -> Signal<Data, Error> {
    Signal { sub in
        var request = URLRequest(url: url)
        request.setValue("bytes=\(start)-\(start+length-1)", forHTTPHeaderField: "Range")
        let task = HLS.session.dataTask(with: request) { data, _, error in
            if let data {
                sub.putNext(data)
                sub.putCompletion()
            } else if let error {
                sub.putError(error)
            } else {
                assertionFailure()
            }
        }
        
        task.resume()
        
        return ActionDisposable {
            task.cancel()
        }
    }
}

private func read(logger: HLSLogger, playlist: M3U8Playlist, fragment: M3U8Playlist.Fragment, assetUrl: URL, basetime: CMTime, isLocal: Bool) throws -> HLSFragment {
    let startTime = DispatchTime.now()
    logger.debug(tag: "read", "r: \(playlist.resolution), t: \(basetime.seconds)")
    
    let newAsset = AVURLAsset(url: assetUrl, options: [AVURLAssetPreferPreciseDurationAndTimingKey: true])
    
    guard !newAsset.tracks(withMediaType: .video).isEmpty else {
        throw HLSError.emptyVideo
    }
    
    guard !newAsset.tracks(withMediaType: .audio).isEmpty else {
        throw HLSError.emptyAudio
    }
    
    let assetVideoTrack = newAsset.tracks(withMediaType: .video)[0]
    let assetVideoTrackTime = assetVideoTrack.timeRange.end
    let assetAudioTrack = newAsset.tracks(withMediaType: .audio)[0]
    let assetAudioTrackTime = assetAudioTrack.timeRange.end
    
    var audioBuffers = [CMSampleBuffer]()
    var videoBuffers = [CMSampleBuffer]()
    var avAudioDuration: CMTime = .zero
    var avVideoDuration: CMTime = .zero
    
    do {
        let composition = AVMutableComposition()
        
        guard
            let videoTrack = composition.addMutableTrack(withMediaType: .video, preferredTrackID: kCMPersistentTrackID_Invalid),
            let audioTrack = composition.addMutableTrack(withMediaType: .audio, preferredTrackID: kCMPersistentTrackID_Invalid)
        else {
            throw HLSError.decodeMedia
        }
        
        try videoTrack.insertTimeRange(
            CMTimeRange(start: .zero, duration: assetVideoTrackTime),
            of: assetVideoTrack,
            at: basetime
        )
        let videoOutput = AVAssetReaderTrackOutput(track: videoTrack, outputSettings: nil)
        
        try audioTrack.insertTimeRange(
            CMTimeRange(start: .zero, duration: assetAudioTrackTime),
            of: assetAudioTrack,
            at: basetime
        )
        audioTrack.scaleTimeRange(CMTimeRange(start: basetime, duration: assetAudioTrackTime), toDuration: assetVideoTrackTime)
        let audioOutput = AVAssetReaderTrackOutput(track: audioTrack, outputSettings: [
            AVFormatIDKey: kAudioFormatLinearPCM
        ])
        
        let reader = try AVAssetReader(asset: composition)
        reader.timeRange = .init(start: basetime, end: .positiveInfinity)
        reader.add(audioOutput)
        reader.add(videoOutput)
        reader.startReading()
        
        while let buffer = videoOutput.copyNextSampleBuffer() {
            videoBuffers.append(buffer)
            avVideoDuration = CMTimeAdd(avVideoDuration, buffer.time.duration)
        }
        
        while let buffer = audioOutput.copyNextSampleBuffer() {
            audioBuffers.append(buffer)
            avAudioDuration = CMTimeAdd(avAudioDuration, buffer.time.duration)
        }
        
        let hlsFragment = HLSFragment(fragment: fragment, basetime: basetime, duration: assetVideoTrackTime, isLocal: isLocal, videoBuffers: videoBuffers, audioBuffers: audioBuffers)
        let endtime = DispatchTime.now()
        let decodingTime = Double(endtime.uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000_000
        logger.debug(tag: "read", "r: \(playlist.resolution), dd: \(decodingTime)")
        
        return hlsFragment
    } catch {
        throw HLSError.decodeMedia
    }
}
