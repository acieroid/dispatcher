(ns dispatcher.core
  (:import [org.zeromq ZMQ]))

;; TODO:
;;   - add a callback that will be called when an event is recognized?

;; The dispatcher sends event to a destination (a string representing
;; the URI of a ZeroMQ socket) as map like {:type :event :event x}. It
;; can also send other messages such as {:type :quit}.
(defrecord Dispatcher
    [;; URI where the dispatcher will send the events
     dest-out
     ;; URI from where the dispatcher will read the recognized events
     dest-in
     ;; Maximum throughput (ev/s)
     throughput
     ;; Map of expected events (keys), valued on the timestamp at
     ;; which they were expected
     expected
     ;; Buffer of events to send
     buffer])

(defn now
  "Return the current time in milliseconds"
  []
  (System/currentTimeMillis))

(defn- spawn-loop
  "Spawn a background task that sends the events received by the
  dispatcher to its destination."
  [dispatcher]
  (let [t (long (/ 1000000000 (:throughput dispatcher)))
        ms (long (/ t 1000000))
        ;; The accuracy would probably not be precise enough to have
        ;; the need for nanoseconds
        ns (mod t 1000000)
        stop (atom false)
        context (ZMQ/context 1)
        socket-out (.socket context ZMQ/PUB)
        socket-in (.socket context ZMQ/SUB)
        _ (.bind socket-out (:dest-out dispatcher))
        _ (.connect socket-in (:dest-in dispatcher))
        _ (.subscribe socket-in (.getBytes "")) ; subscribe to all messages
        thread
        (future
          (try
            (loop []
              ;; Wait in order to respect the max. throughput
              (Thread/sleep ms ns)
              ;; Remember if we need to stop when all remaining
              ;; expected events are recognized
              (swap! (:buffer dispatcher)
                     #(if (empty? %)
                        []
                        (if (= (:type (first %)) :stop)
                          (do
                            (swap! stop (fn [_] true))
                            (into [] (rest %)))
                          %)))
              ;; Send the events waiting to be sent
              (swap! (:buffer dispatcher)
                     #(if (empty? %)
                        []
                        (if (= (:type (first %)) :event)
                          (do
                            (.send socket-out (str (first %)))
                            (into [] (rest %)))
                          %)))
              ;; Handle the expected events stored in the buffer
              (swap! (:buffer dispatcher)
                     (fn [b]
                       (let [[expected buffer] (split-with
                                                #(= (:type %) :expected)
                                                b)]
                         (when (not (empty? expected))
                           (mapv #(swap! (:expected dispatcher) conj {(:event %)
                                                                      (now)})
                                 expected))
                         (into [] buffer))))
              ;; Receive the events
              (when-let [reply (.recv socket-in ZMQ/NOBLOCK)]
                (let [msg (read-string (String. reply))]
                  (case (keyword (:type msg))
                    :recognition
                    (swap! (:expected dispatcher)
                           #(if-let [expected-t (get % (:event msg))]
                              (let [time-took (- (now) expected-t)]
                                (println (str "Recognition: " (:event msg)
                                              " (took " time-took "ms)"))
                                (dissoc % (:event msg)))
                              (do
                                (println "Unexpected recognition:" (:event msg))
                                %)))
                    (println "Unexpected reply:" msg))))
              (if (and @stop (empty? @(:expected dispatcher)))
                (do
                  (.send socket-out (str {:type :quit}))
                  (.close socket-out)
                  (.close socket-in)
                  (.term context)
                  (shutdown-agents))
                (recur)))
            (catch Exception e
              (println "Error in background task:" e)
              (.printStackTrace e))))]
    (update-in dispatcher [:thread] (fn [_] thread))))

(defn create
  "Create a dispatcher and launch a background task that will send the
  messages without exceeding the given throughput."
  [dest-in dest-out throughput]
  (let [dispatcher (Dispatcher. dest-in dest-out throughput (atom {}) (atom []))]
    ;; Spawn loop updates the dispatcher and returns it (and creates
    ;; the background loop that sends the events)
    (spawn-loop dispatcher)))

(defn enqueue
  "Enqueue an event to be sent later"
  [dispatcher event]
  (swap! (:buffer dispatcher) conj {:type :event :event event})
  dispatcher)

(defn expect
  "Remembers that an event is expected to happen now. Will compute the
  delay between now and the time it will really happen"
  [dispatcher event]
  (swap! (:buffer dispatcher) conj {:type :expected :event event})
  dispatcher)

(defn cancel
  "Immediately stops the dispatcher"
  [dispatcher]
  (future-cancel (:thread dispatcher))
  dispatcher)

(defn finish
  "Don't send more events than those already stored in the
  buffer. Waits for all expected events, then return."
  [dispatcher]
  (swap! (:buffer dispatcher) conj {:type :stop})
  dispatcher)

(defn abort
  "Directly send a stop message and drop all bufferised events"
  [dispatcher]
  (swap! (:buffer dispatcher) (fn [b] [{:type :stop}]))
  dispatcher)
