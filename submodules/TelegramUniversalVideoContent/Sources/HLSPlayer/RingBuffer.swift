struct RingBuffer<T> {
    private var array: [T?]
    private var head = 0
    private var tail = 0
    private(set) var count = 0
    private(set) var capacity: Int

    init(capacity: Int) {
        self.capacity = capacity
        self.array = Array<T?>(repeating: nil, count: capacity)
    }

    var isEmpty: Bool {
        return count == 0
    }

    var isFull: Bool {
        return count == capacity
    }

    @discardableResult
    mutating func enqueue(_ element: T) -> Bool {
        if isFull {
            resizeBuffer()
        }

        array[tail] = element
        tail = (tail + 1) % capacity
        count += 1
        return true
    }

    mutating func dequeue() -> T? {
        guard !isEmpty else { return nil }

        let element = array[head]
        array[head] = nil
        head = (head + 1) % capacity
        count -= 1
        return element
    }

    func peek() -> T? {
        return array[head]
    }
    
    mutating func flush() {
        array = Array<T?>(repeating: nil, count: capacity)
        head = 0
        tail = 0
        count = 0
    }

    private mutating func resizeBuffer() {
        let newCapacity = capacity * 2
        var newArray = Array<T?>(repeating: nil, count: newCapacity)

        for i in 0..<count {
            newArray[i] = array[(head + i) % capacity]
        }

        array = newArray
        head = 0
        tail = count
        capacity = newCapacity
    }
}
