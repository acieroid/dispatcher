(ns dispatcher.core
  (:import [org.jeromq ZMQ]))

;; TODO:
;;   - add a callback that will be called when an event is recognized?
;;   - have some way to know when we can stop? (eg. when ``stop`` is
;;     called, we wait until all expected events are received and then
;;     we send a quit message, and close the connection

;; The dispatcher sends event to a destination (a string representing
;; the URI of a ZeroMQ socket) as map like {:type :event :event x}. It
;; can also send other messages such as {:type :quit}. It sends those
;; events with a maximum throughput (expressed in messages/seconds),
;; and contains a map indexed by events, and whose values are the time
;; at which they are expected to be recognized, and a buffer
;; containing events that will be sent.
(defrecord Dispatcher [destination throughput expected buffer])

(defn now
  "Return the current time in milliseconds"
  [])

(defn- spawn-loop
  "Spawn a background task that sends the events received by the
  dispatcher to its destination."
  [dispatcher]
  (let [t (long (/ 1000000000 (:throughput dispatcher)))
        ms (long (/ t 1000000))
        ;; The accuracy would probably not be precise enough to have
        ;; the need for nanoseconds
        ns (mod t 1000000)
        context (ZMQ/context 1)
        socket (.socket context ZMQ/PAIR)
        started (atom false)
        _ (.connect socket (:destination dispatcher))
        thread
        (future
          (try
            (loop []
              (Thread/sleep ms ns)
              (swap! (:buffer dispatcher)
                     #(if (empty? %)
                        []
                        (do
                          (.send socket
                                 (.getBytes
                                  (str {:type :event
                                        :event (first %)})))
                          (swap! started (fn [_] true))
                          (into [] (rest %)))))
              (when @started
                (when-let [reply (.recv socket ZMQ/NOBLOCK)]
                  (let [msg (read-string (String. reply))]
                    (case (keyword (:type msg))
                      :recognition
                      (swap! (:expected dispatcher)
                             #(if-let [expected-t (get % (:event msg))]
                                (let [time-took (- (now) expected-t)]
                                  (println (str "Recognition: " (:event msg)
                                                " (took" time-took "ms)"))
                                  (dissoc % (:event msg)))
                                (do
                                  (println "Unexpected recognition:" (:event msg))
                                  %)))
                      (println "Unexpected reply:" msg)))))
              (recur))
            (catch Exception e
              (println "Error in background task:" e)
              (.printStackTrace e))))]
    (update-in dispatcher [:thread] (fn [_] thread))))

(defn create
  "Create a dispatcher and launch a background task that will send the
  messages without exceeding the given throughput."
  [destination throughput]
  (let [dispatcher (Dispatcher. destination throughput (atom {}) (atom []))]
    ;; TODO: spawn background threads reading events from ``buffer``
    ;; at speed ``throughput``
    (spawn-loop dispatcher)))

(defn enqueue
  "Enqueue an event to be sent later"
  [dispatcher event]
  (swap! (:buffer dispatcher) conj event)
  dispatcher)

(defn expect
  "Remembers that an event is expected to happen now. Will compute the
  delay between now and the time it will really happen"
  [dispatcher event]
  (swap! (:expected dispatcher) conj {event (now)})
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
  ;; TODO
  dispatcher)
